// #![windows_subsystem = "windows"]
use arboard::Clipboard;
use futures_util::SinkExt;
use futures_util::StreamExt;
use imgui::Condition;
use imgui_rs_overlay::{
    window::{Windows, WindowsOptions},
    Result,
};
mod spoti;

use image::io::Reader as ImageReader;
use serde::{Deserialize, Serialize};
use spoti::SpotifyController;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use windows::core::Interface;
use windows::core::PWSTR;
use windows::Win32::Foundation::HWND;
use windows::Win32::Graphics::Direct3D11::{
    ID3D11Device, ID3D11DeviceContext, ID3D11ShaderResourceView, ID3D11Texture2D,
    D3D11_BIND_SHADER_RESOURCE, D3D11_SUBRESOURCE_DATA, D3D11_TEXTURE2D_DESC,
    D3D11_USAGE_IMMUTABLE,
};
use windows::Win32::Graphics::Dxgi::Common::DXGI_FORMAT_R8G8B8A8_UNORM;
use windows::Win32::System::Threading::{
    OpenProcess, QueryFullProcessImageNameW, PROCESS_NAME_FORMAT, PROCESS_QUERY_LIMITED_INFORMATION,
};
use windows::Win32::UI::WindowsAndMessaging::{GetForegroundWindow, GetWindowThreadProcessId};

const WS_SERVER_URL: &str = "ws://localhost:8080/";

unsafe fn create_d3d11_texture(
    device: &ID3D11Device,
    width: u32,
    height: u32,
    rgba_data: &[u8],
) -> Option<(usize, ID3D11Texture2D, ID3D11ShaderResourceView)> {
    let desc = D3D11_TEXTURE2D_DESC {
        Width: width,
        Height: height,
        MipLevels: 1,
        ArraySize: 1,
        Format: DXGI_FORMAT_R8G8B8A8_UNORM,
        SampleDesc: windows::Win32::Graphics::Dxgi::Common::DXGI_SAMPLE_DESC {
            Count: 1,
            Quality: 0,
        },
        Usage: D3D11_USAGE_IMMUTABLE,
        BindFlags: D3D11_BIND_SHADER_RESOURCE.0 as u32,
        CPUAccessFlags: Default::default(),
        MiscFlags: Default::default(),
    };

    let subresource = D3D11_SUBRESOURCE_DATA {
        pSysMem: rgba_data.as_ptr() as _,
        SysMemPitch: width * 4,
        SysMemSlicePitch: 0,
    };

    let mut texture: Option<ID3D11Texture2D> = None;
    device
        .CreateTexture2D(&desc, Some(&subresource), Some(&mut texture))
        .ok()?;
    let texture = texture?;

    let mut srv: Option<ID3D11ShaderResourceView> = None;
    device
        .CreateShaderResourceView(&texture, None, Some(&mut srv))
        .ok()?;
    let srv = srv?;

    let texture_id = srv.as_raw() as *const () as usize;
    Some((texture_id, texture, srv))
}

fn foreground_window_path() -> Option<String> {
    unsafe {
        let hwnd: HWND = GetForegroundWindow();
        if hwnd.0 == std::ptr::null_mut() {
            return None;
        }

        let mut pid = 0u32;
        GetWindowThreadProcessId(hwnd, Some(&mut pid));
        if pid == 0 {
            return None;
        }

        let process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid).ok()?;
        let mut buf = vec![0u16; 1024];
        let mut size = buf.len() as u32;
        QueryFullProcessImageNameW(
            process,
            PROCESS_NAME_FORMAT(0),
            PWSTR(buf.as_mut_ptr()),
            &mut size,
        )
        .ok()?;
        let path = String::from_utf16_lossy(&buf[..size as usize]);
        Some(path)
    }
}

#[derive(Clone)]
pub struct ImageData {
    pub width: u32,
    pub height: u32,
    pub data: Vec<u8>,
    pub texture_id: Option<usize>, // D3D11 SRV pointer as TextureId
    #[allow(dead_code)]
    pub texture: Option<ID3D11Texture2D>, // Keep texture alive
    #[allow(dead_code)]
    pub srv: Option<ID3D11ShaderResourceView>, // Keep SRV alive
}

#[derive(Clone)]
pub struct ImageCache {
    cache: Arc<Mutex<HashMap<String, ImageData>>>,
}

impl ImageCache {
    fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn load_image(
        &self,
        url: &str,
    ) -> std::result::Result<ImageData, Box<dyn std::error::Error + Send + Sync>> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(img) = cache.get(url) {
                return Ok(img.clone());
            }
        }

        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        let bytes = response
            .bytes()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let reader = ImageReader::new(Cursor::new(bytes));
        let reader = reader
            .with_guessed_format()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let img = reader
            .decode()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
            .to_rgba8();

        let (width, height) = img.dimensions();
        let data: Vec<u8> = img.into_raw();

        let image_data = ImageData {
            width,
            height,
            data,
            texture_id: None,
            texture: None,
            srv: None,
        };

        let mut cache = self.cache.lock().unwrap();
        cache.insert(url.to_string(), image_data.clone());

        Ok(image_data)
    }

    fn get(&self, url: &str) -> Option<ImageData> {
        self.cache.lock().unwrap().get(url).cloned()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LobbyEvent {
    UserJoin {
        username: String,
    },
    UserLeave {
        username: String,
    },
    LobbyClose,
    Sync {
        id: String,
        name: String,
        author: String,
        image_url: String,
        timestamp_ms: u64,
        duration_ms: u64,
        paused: bool,
    },
}

struct LobbyManager {
    lobby_id: String,
    spotify: Arc<SpotifyController>,
    event_tx: broadcast::Sender<LobbyEvent>,
    is_host: bool,
    state: Arc<std::sync::Mutex<LobbyManagerState>>,
    image_cache: ImageCache,
    device: Option<ID3D11Device>,
    device_context: Option<ID3D11DeviceContext>,
    ws_sender: Arc<tokio::sync::Mutex<Option<mpsc::Sender<Message>>>>,
}

struct LobbyManagerState {
    connected_users: Vec<String>,
    connected_count: usize,
    current_sync: Option<LobbyEvent>,
    display_text: String,
    current_image: Option<ImageData>,
    current_image_url: String,
    image_loading: bool,
}

impl LobbyManager {
    fn new(
        lobby_id: String,
        spotify: Arc<SpotifyController>,
        is_host: bool,
        device: Option<ID3D11Device>,
        device_context: Option<ID3D11DeviceContext>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(32);
        Self {
            lobby_id,
            spotify,
            event_tx,
            is_host,
            image_cache: ImageCache::new(),
            device,
            device_context,
            state: Arc::new(Mutex::new(LobbyManagerState {
                connected_users: Vec::new(),
                connected_count: 0,
                current_sync: None,
                display_text: String::from("Loading..."),
                current_image: None,
                current_image_url: String::new(),
                image_loading: false,
            })),
            ws_sender: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    fn queue_image_load(&self, image_url: &str) {
        if image_url.is_empty() {
            let mut state = self.state.lock().unwrap();
            state.current_image = None;
            state.current_image_url.clear();
            state.image_loading = false;
            return;
        }

        let mut state = self.state.lock().unwrap();
        let should_load = state.current_image_url != image_url
            || (!state.image_loading && state.current_image.is_none());
        if !should_load {
            return;
        }

        state.current_image_url = image_url.to_string();
        state.current_image = None;
        state.image_loading = true;
        drop(state);

        let cache = self.image_cache.clone();
        let url = image_url.to_string();
        let state = self.state.clone();
        let device = self.device.clone();
        tokio::spawn(async move {
            match cache.load_image(&url).await {
                Ok(mut img_data) => {
                    // Create D3D11 texture if device is available
                    if let Some(ref dev) = device {
                        if let Some((texture_id, texture, srv)) = unsafe {
                            create_d3d11_texture(
                                dev,
                                img_data.width,
                                img_data.height,
                                &img_data.data,
                            )
                        } {
                            img_data.texture_id = Some(texture_id);
                            img_data.texture = Some(texture);
                            img_data.srv = Some(srv);
                        }
                    }

                    let mut s = state.lock().unwrap();
                    if s.current_image_url == url {
                        s.current_image = Some(img_data);
                    }
                    s.image_loading = false;
                }
                Err(e) => {
                    eprintln!("Failed to load image: {}", e);
                    let mut s = state.lock().unwrap();
                    if s.current_image_url == url {
                        s.image_loading = false;
                    }
                }
            }
        });
    }

    async fn handle_event(&self, event: LobbyEvent) {
        match event.clone() {
            LobbyEvent::UserJoin { ref username } => {
                let mut state = self.state.lock().unwrap();
                if !state.connected_users.contains(username) {
                    state.connected_users.push(username.clone());
                    state.connected_count = state.connected_users.len();
                }
            }
            LobbyEvent::UserLeave { ref username } => {
                let mut state = self.state.lock().unwrap();
                state.connected_users.retain(|u| u != username);
                state.connected_count = state.connected_users.len();
            }
            LobbyEvent::LobbyClose => {
                let mut state = self.state.lock().unwrap();
                state.connected_users.clear();
                state.connected_count = 0;
                state.current_sync = None;
                state.display_text = String::from("Loading...");
                state.current_image = None;
                state.current_image_url.clear();
                state.image_loading = false;
            }
            LobbyEvent::Sync {
                ref paused,
                ref name,
                ref image_url,
                ..
            } => {
                {
                    let mut state = self.state.lock().unwrap();
                    state.current_sync = Some(event.clone());
                    state.display_text = if name.is_empty() {
                        "Loading...".to_string()
                    } else if *paused {
                        "Paused".to_string()
                    } else {
                        name.clone()
                    };
                } // state is dropped here

                self.queue_image_load(image_url);

                if !self.is_host {
                    if let Err(e) = self.apply_sync(&event).await {
                        eprintln!("Failed to apply sync: {}", e);
                    }
                }
            }
        }
        let _ = self.event_tx.send(event);
    }

    async fn apply_sync(
        &self,
        event: &LobbyEvent,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let LobbyEvent::Sync {
            id,
            paused,
            timestamp_ms,
            duration_ms: _,
            ..
        } = event
        {
            // Start timer immediately on receiving sync
            let receive_time = Instant::now();
            const LATENCY_BONUS_MS: u64 = 5;

            let current = self.spotify.get_id().await.unwrap_or_default();
            if &current != id && !id.is_empty() {
                println!("[guest] track changed, would need to switch to {}", id);
            }

            if *paused {
                self.spotify.set_pause().await?;
            } else {
                self.spotify.set_play().await?;

                // Get client's current playback position
                let local_position = self.spotify.get_progress_ms().await.unwrap_or(0);
                let diff = if *timestamp_ms > local_position {
                    *timestamp_ms - local_position
                } else {
                    local_position - *timestamp_ms
                };

                println!(
                    "[guest] sync diff: {}ms (host={}ms, local={}ms)",
                    diff, timestamp_ms, local_position
                );

                // Only seek if the difference is greater than 1.5 seconds
                if diff > 1500 {
                    // Adjust seek target by elapsed time since receive + latency bonus
                    let elapsed_ms = receive_time.elapsed().as_millis() as u64;
                    let adjusted_target = timestamp_ms + elapsed_ms + LATENCY_BONUS_MS;
                    println!(
                        "[guest] seeking to {}ms (elapsed={}ms, bonus={}ms)",
                        adjusted_target, elapsed_ms, LATENCY_BONUS_MS
                    );
                    self.spotify.set_seek(adjusted_target).await?;
                } else {
                    println!("[guest] diff {}ms <= 1500ms, skipping seek", diff);
                }
            }
        }
        Ok(())
    }

    fn get_connected_count(&self) -> usize {
        self.state.lock().unwrap().connected_count
    }

    fn get_display_text(&self) -> String {
        self.state.lock().unwrap().display_text.clone()
    }

    fn get_artist(&self) -> String {
        let state = self.state.lock().unwrap();
        if let Some(LobbyEvent::Sync { author, .. }) = &state.current_sync {
            author.clone()
        } else {
            String::new()
        }
    }

    fn get_image_url(&self) -> String {
        let state = self.state.lock().unwrap();
        if let Some(LobbyEvent::Sync { image_url, .. }) = &state.current_sync {
            image_url.clone()
        } else {
            String::new()
        }
    }

    fn get_image_data(&self) -> Option<ImageData> {
        self.state.lock().unwrap().current_image.clone()
    }

    async fn listen_events_with_handler(
        self: &Arc<Self>,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut reconnect_attempts = 0;
        let max_reconnect_attempts = 10;

        println!("[jam67] *** LOBBY ID: {} ***", self.lobby_id);

        loop {
            let ws_url = format!("{}?id={}", WS_SERVER_URL, self.lobby_id);
            println!("[jam67] Connecting to WebSocket: {}", ws_url);

            match connect_async(&ws_url).await {
                Ok((ws_stream, _)) => {
                    reconnect_attempts = 0;
                    println!("[jam67] WebSocket connected successfully");

                    let (mut write, mut read) = ws_stream.split();

                    // Create channel for outgoing messages
                    let (tx, mut rx) = mpsc::channel::<Message>(32);

                    // Store sender in ws_sender for publish_event to use
                    {
                        let mut ws_sender = self.ws_sender.lock().await;
                        *ws_sender = Some(tx);
                    }

                    // Spawn task to handle outgoing messages
                    let write_task = tokio::spawn(async move {
                        while let Some(msg) = rx.recv().await {
                            if let Err(e) = write.send(msg).await {
                                eprintln!("[jam67] Failed to send message: {}", e);
                                break;
                            }
                        }
                    });

                    // Handle incoming messages
                    let self_clone = self.clone();
                    let read_task = tokio::spawn(async move {
                        while let Some(msg) = read.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    println!("[jam67] << RECEIVED: {}", text);
                                    if let Ok(event) = serde_json::from_str::<LobbyEvent>(&text) {
                                        self_clone.handle_event(event).await;
                                    }
                                }
                                Ok(Message::Close(_)) => {
                                    eprintln!("[jam67] WebSocket closed by server");
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("[jam67] WebSocket error: {}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    });

                    // Wait for either task to finish
                    tokio::select! {
                        _ = write_task => {},
                        _ = read_task => {},
                    }

                    // Clear the sender
                    {
                        let mut ws_sender = self.ws_sender.lock().await;
                        *ws_sender = None;
                    }

                    println!("[jam67] Connection lost, attempting to reconnect...");
                    reconnect_attempts += 1;
                    if reconnect_attempts >= max_reconnect_attempts {
                        return Err("Max reconnect attempts reached".into());
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                Err(e) => {
                    eprintln!("[jam67] Failed to connect WebSocket: {}", e);
                    reconnect_attempts += 1;
                    if reconnect_attempts >= max_reconnect_attempts {
                        return Err(format!("Max reconnect attempts reached: {}", e).into());
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
    }

    async fn publish_event(
        &self,
        event: LobbyEvent,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body = serde_json::to_string(&event)?;
        println!("[jam67] >> SENDING: {}", body);

        let ws_sender = self.ws_sender.lock().await;
        if let Some(ref tx) = *ws_sender {
            tx.send(Message::Text(body.into())).await?;
        } else {
            eprintln!("[jam67] WebSocket not connected, cannot send event");
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn subscribe(&self) -> broadcast::Receiver<LobbyEvent> {
        self.event_tx.subscribe()
    }

    async fn host_polling_loop(
        self: &Arc<Self>,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let polling_interval = std::time::Duration::from_millis(300);

        loop {
            tokio::time::sleep(polling_interval).await;

            match self.spotify.get_track_info().await {
                Ok(info) => {
                    println!(
                        "[host] {} - {} | {}ms / {}ms | playing={}",
                        info.title,
                        info.artist,
                        info.progress_ms,
                        info.duration_ms,
                        info.is_playing
                    );

                    let title_display = if info.title.is_empty() {
                        String::from("Loading...")
                    } else if !info.is_playing {
                        String::from("Paused")
                    } else {
                        info.title.clone()
                    };

                    let sync_event = LobbyEvent::Sync {
                        id: String::new(),
                        name: info.title.clone(),
                        author: info.artist.clone(),
                        image_url: info.image_url.clone(),
                        timestamp_ms: info.progress_ms,
                        duration_ms: info.duration_ms,
                        paused: !info.is_playing,
                    };

                    let mut state = self.state.lock().unwrap();
                    state.current_sync = Some(sync_event.clone());
                    state.display_text = title_display;
                    drop(state);

                    self.queue_image_load(&info.image_url);

                    if let Err(e) = self.publish_event(sync_event).await {
                        eprintln!("[host] publish error: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("[host] polling error: {}", e);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Connecting to Spotify CDP...");
    let spotify = match SpotifyController::connect().await {
        Ok(s) => {
            println!("CDP connected OK");
            Arc::new(s)
        }
        Err(e) => {
            eprintln!("CDP connect failed: {}", e);
            eprintln!("Trying to restart Spotify with --remote-debugging-port=3132...");
            let kill_cmd = std::process::Command::new("powershell")
                .args([
                    "-Command",
                    "Get-Process -Name Spotify -ErrorAction SilentlyContinue | Stop-Process -Force",
                ])
                .output();
            if let Ok(o) = &kill_cmd {
                if !o.status.success() {
                    eprintln!("Failed to kill existing Spotify processes");
                }
            }
            let spotify_path = {
                let appdata = std::env::var("APPDATA").unwrap_or_default();
                format!("{}\\Spotify\\Spotify.exe", appdata)
            };
            match std::process::Command::new(&spotify_path)
                .arg("--remote-debugging-port=3132")
                .spawn()
            {
                Ok(_) => {
                    println!("Launched Spotify, retrying connection...");
                    let mut connected = false;
                    let mut spotify_arc: Option<Arc<SpotifyController>> = None;
                    for delay in &[2, 4, 8] {
                        println!("Waiting {}s...", delay);
                        tokio::time::sleep(std::time::Duration::from_secs(*delay)).await;
                        match SpotifyController::connect().await {
                            Ok(s) => {
                                println!("CDP connected OK after restart");
                                s.mark_as_restarted().await;
                                spotify_arc = Some(Arc::new(s));
                                connected = true;
                                break;
                            }
                            Err(e2) => {
                                eprintln!("Retry failed: {}", e2);
                            }
                        }
                    }
                    if connected {
                        spotify_arc.unwrap()
                    } else {
                        eprintln!("CDP connect failed after all retries");
                        eprintln!("Make sure Spotify is running with --remote-debugging-port=3132");
                        std::process::exit(1);
                    }
                }
                Err(e2) => {
                    eprintln!("Failed to launch Spotify: {}", e2);
                    std::process::exit(1);
                }
            }
        }
    };

    // Initialize player API with retries
    match spotify.init_player_api_with_retries().await {
        Ok(()) => {
            println!("[jam67] Player API initialized successfully");
        }
        Err(e) => {
            eprintln!("[jam67] Failed to initialize player API: {}", e);
            eprintln!("[jam67] Exiting...");
            std::process::exit(1);
        }
    }

    let mut app = Windows::new(&WindowsOptions::default())?;

    // Get D3D11 device for texture creation
    let (d3d_device, d3d_context) = match app.get_d3d11_devices() {
        Some((device, context)) => (Some(device), Some(context)),
        None => (None, None),
    };

    let mut app_state = AppState::LobbyChoice;
    let mut lobby_code = String::new();
    let mut is_host = false;
    let mut lobby_manager: Option<Arc<LobbyManager>> = None;

    app.run(move |ui, _style| {
        let should_show_ui = foreground_window_path()
            .map(|path| path.to_lowercase().contains("spotify"))
            .unwrap_or(false);
        if !should_show_ui {
            return true;
        }

        match &app_state {
            AppState::LobbyChoice => {
                ui.window("Jam67")
                    .resizable(false)
                    .size([320.0, 100.0], Condition::FirstUseEver)
                    .movable(true)
                    .collapsible(false)
                    .build(|| {
                        ui.text("Crea o entra in una lobby");
                        ui.spacing();

                        if ui.button("Esci") {
                            std::process::exit(0);
                        }

                        ui.same_line();

                        if ui.button("Crea lobby") {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis();
                            lobby_code = format!("{:x}", now);
                            is_host = true;
                            app_state = AppState::CreatingLobby;
                        }

                        ui.same_line();

                        if ui.button("Entra in lobby") {
                            app_state = AppState::JoiningLobby;
                        }
                    });
            }

            AppState::JoiningLobby { .. } => {
                ui.window("Join / Jam67")
                    .resizable(false)
                    .size([320.0, 100.0], Condition::FirstUseEver)
                    .movable(true)
                    .collapsible(false)
                    .build(|| {
                        ui.text("Codice lobby");
                        ui.input_text(" ", &mut lobby_code).build();

                        if ui.button("Esci") {
                            std::process::exit(0);
                        }

                        ui.same_line();

                        if ui.button("Incolla") {
                            if let Ok(mut clipboard) = Clipboard::new() {
                                if let Ok(text) = clipboard.get_text() {
                                    lobby_code = text;
                                }
                            }
                        }

                        ui.same_line();

                        if ui.button("Connetti") {
                            let code = lobby_code.clone();
                            let s = spotify.clone();

                            lobby_manager = Some(Arc::new(LobbyManager::new(
                                code,
                                s,
                                false,
                                d3d_device.clone(),
                                d3d_context.clone(),
                            )));

                            if let Some(ref manager) = lobby_manager {
                                let mgr = manager.clone();
                                std::thread::spawn(move || {
                                    let rt = tokio::runtime::Runtime::new().unwrap();
                                    rt.block_on(async move {
                                        println!("[guest] listening on WebSocket...");
                                        if let Err(e) = mgr.listen_events_with_handler().await {
                                            eprintln!("[guest] listen error: {}", e);
                                        }
                                    });
                                });
                            }

                            app_state = AppState::InLobby;
                        }
                    });
            }

            AppState::CreatingLobby { .. } => {
                ui.window("Create / Jam67")
                    .resizable(false)
                    .size([320.0, 100.0], Condition::FirstUseEver)
                    .movable(true)
                    .collapsible(false)
                    .build(|| {
                        ui.text("Generando lobby...");
                        ui.spacing();

                        if ui.button("Esci") {
                            std::process::exit(0);
                        }
                    });

                if lobby_manager.is_none() {
                    let code = lobby_code.clone();
                    let s = spotify.clone();
                    let mgr = Arc::new(LobbyManager::new(
                        code.clone(),
                        s,
                        true,
                        d3d_device.clone(),
                        d3d_context.clone(),
                    ));

                    {
                        {
                            let mgr = mgr.clone();
                            std::thread::spawn(move || {
                                let rt = tokio::runtime::Runtime::new().unwrap();
                                rt.block_on(async move {
                                    println!("[host] listening on WebSocket...");
                                    if let Err(e) = mgr.listen_events_with_handler().await {
                                        eprintln!("[host] listen error: {}", e);
                                    }
                                });
                            });
                        }

                        {
                            let mgr = mgr.clone();
                            std::thread::spawn(move || {
                                let rt = tokio::runtime::Runtime::new().unwrap();
                                rt.block_on(async move {
                                    println!("[host] polling loop started");
                                    if let Err(e) = mgr.host_polling_loop().await {
                                        eprintln!("[host] polling error: {}", e);
                                    }
                                });
                            });
                        }
                    }

                    lobby_manager = Some(mgr);
                    app_state = AppState::InLobby;
                }
            }

            AppState::InLobby { .. } => {
                if let Some(ref manager) = lobby_manager {
                    let _connected_count = manager.get_connected_count();
                    let display_text = manager.get_display_text();
                    let artist = manager.get_artist();
                    let image_data = manager.get_image_data();

                    let title = if is_host {
                        format!("Host / Jam67")
                    } else {
                        "Guest / Jam67".to_string()
                    };

                    ui.window(&title)
                        .resizable(false)
                        .size([560.0, 250.0], Condition::FirstUseEver)
                        .movable(true)
                        .collapsible(false)
                        .build(|| {
                            if let Some(img_data) = image_data {
                                // Render the actual album art image
                                if let Some(texture_id) = img_data.texture_id {
                                    let texture_id = imgui::TextureId::from(texture_id);
                                    // Display image at 150x150 pixels
                                    imgui::Image::new(texture_id, [120.0, 120.0]).build(ui);
                                } else {
                                    // Fallback if texture creation failed
                                    ui.text("Loading album art...");
                                    ui.dummy([150.0, 10.0]);
                                }
                            } else {
                                ui.text("Loading album art...");
                                ui.dummy([150.0, 10.0]);
                            }

                            ui.spacing();
                            ui.text(&display_text);
                            if !artist.is_empty() {
                                ui.text(format!("Artist: {}", artist));
                            }
                            ui.spacing();

                            if is_host {
                                ui.text(format!("Codice lobby: {}", lobby_code));
                            }

                            ui.spacing();

                            if ui.button("Esci") {
                                std::process::exit(0);
                            }

                            ui.same_line();

                            if ui.button("Copia Codice") {
                                if let Ok(mut clipboard) = Clipboard::new() {
                                    let _ = clipboard.set_text(lobby_code.clone());
                                }
                            }
                        });
                }
            }
        }

        true
    })?;

    Ok(())
}

#[derive(Clone, PartialEq)]
enum AppState {
    LobbyChoice,
    JoiningLobby,
    CreatingLobby,
    InLobby,
}
