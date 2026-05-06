// #![windows_subsystem = "windows"]
use futures_util::StreamExt;
use imgui::Condition;
use imgui_rs_overlay::{
    window::{Windows, WindowsOptions},
    Result,
};
mod spoti;

use serde::{Deserialize, Serialize};
use spoti::SpotifyController;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use std::collections::HashMap;
use image::io::Reader as ImageReader;
use std::io::Cursor;

const WORKER_URL: &str = "https://jam67.lu2000luk.workers.dev";

#[derive(Clone, Debug)]
pub struct ImageData {
    pub width: u32,
    pub height: u32,
    pub data: Vec<u8>,
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

    async fn load_image(&self, url: &str) -> std::result::Result<ImageData, Box<dyn std::error::Error + Send + Sync>> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(img) = cache.get(url) {
                return Ok(img.clone());
            }
        }

        let response = reqwest::Client::new().get(url).send().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        let bytes = response.bytes().await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        let reader = ImageReader::new(Cursor::new(bytes));
        let reader = reader.with_guessed_format()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        
        let img = reader.decode()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
            .to_rgba8();
        
        let (width, height) = img.dimensions();
        let data: Vec<u8> = img.into_raw();
        
        let image_data = ImageData { width, height, data };
        
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
}

struct LobbyManagerState {
    connected_users: Vec<String>,
    connected_count: usize,
    current_sync: Option<LobbyEvent>,
    display_text: String,
    current_image: Option<ImageData>,
}

impl LobbyManager {
    fn new(lobby_id: String, spotify: Arc<SpotifyController>, is_host: bool) -> Self {
        let (event_tx, _) = broadcast::channel(32);
        Self {
            lobby_id,
            spotify,
            event_tx,
            is_host,
            image_cache: ImageCache::new(),
            state: Arc::new(Mutex::new(LobbyManagerState {
                connected_users: Vec::new(),
                connected_count: 0,
                current_sync: None,
                display_text: String::from("Loading..."),
                current_image: None,
            })),
        }
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
            }
            LobbyEvent::Sync {
                ref paused,
                ref name,
                ref image_url,
                ..
            } => {
                let mut state = self.state.lock().unwrap();
                state.current_sync = Some(event.clone());
                state.display_text = if name.is_empty() {
                    "Loading...".to_string()
                } else if *paused {
                    "Paused".to_string()
                } else {
                    name.clone()
                };

                drop(state);

                // Load image asynchronously
                if !image_url.is_empty() {
                    let cache = self.image_cache.clone();
                    let url = image_url.clone();
                    let state = self.state.clone();
                    tokio::spawn(async move {
                        match cache.load_image(&url).await {
                            Ok(img_data) => {
                                let mut s = state.lock().unwrap();
                                s.current_image = Some(img_data);
                            }
                            Err(e) => {
                                eprintln!("Failed to load image: {}", e);
                            }
                        }
                    });
                }

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
            let current = self.spotify.get_id().await.unwrap_or_default();
            if &current != id && !id.is_empty() {
                println!("[guest] track changed, would need to switch to {}", id);
            }

            if *paused {
                self.spotify.set_pause().await?;
            } else {
                self.spotify.set_play().await?;
                self.spotify.set_seek(*timestamp_ms).await?;
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
        let url = format!("{}/socket?id={}", WORKER_URL, self.lobby_id);
        let ws_url = url
            .replace("https://", "wss://")
            .replace("http://", "ws://");

        let (ws_stream, _) = connect_async(&ws_url).await?;
        let (_write, mut read) = ws_stream.split();

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(event) = serde_json::from_str::<LobbyEvent>(&text) {
                        self.handle_event(event).await;
                    }
                }
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    eprintln!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn publish_event(
        &self,
        event: LobbyEvent,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();
        let url = format!("{}/push?id={}", WORKER_URL, self.lobby_id);
        let body = serde_json::to_string(&event)?;
        client.post(&url).body(body).send().await?;
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
                        name: info.title,
                        author: info.artist,
                        image_url: info.image_url,
                        timestamp_ms: info.progress_ms,
                        duration_ms: info.duration_ms,
                        paused: !info.is_playing,
                    };

                    let mut state = self.state.lock().unwrap();
                    state.current_sync = Some(sync_event.clone());
                    state.display_text = title_display;
                    drop(state);

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

    let mut app = Windows::new(&WindowsOptions::default())?;
    let mut app_state = AppState::LobbyChoice;
    let mut lobby_code = String::new();
    let mut is_host = false;
    let mut lobby_manager: Option<Arc<LobbyManager>> = None;

    app.run(move |ui, _style| {
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
                        ui.spacing();

                        if ui.button("Esci") {
                            std::process::exit(0);
                        }

                        ui.same_line();

                        if ui.button("Connetti") {
                            let code = lobby_code.clone();
                            let s = spotify.clone();

                            lobby_manager = Some(Arc::new(LobbyManager::new(code, s, false)));

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
                    let mgr = Arc::new(LobbyManager::new(code.clone(), s, true));

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
                        .size([560.0, 400.0], Condition::FirstUseEver)
                        .movable(true)
                        .collapsible(false)
                        .build(|| {
                            // Display album art as a colored box placeholder
                            let draw_list = ui.get_window_draw_list();
                            let [x, y] = ui.cursor_screen_pos();
                            
                            if let Some(img_data) = image_data {
                                // Draw a colored rectangle as placeholder
                                // Calculate average color from image for visualization
                                let sample_size = (img_data.data.len() / 4).min(100);
                                let mut r_sum = 0u32;
                                let mut g_sum = 0u32;
                                let mut b_sum = 0u32;
                                
                                for i in 0..sample_size {
                                    let idx = (i * 4) % img_data.data.len();
                                    if idx + 2 < img_data.data.len() {
                                        r_sum += img_data.data[idx] as u32;
                                        g_sum += img_data.data[idx + 1] as u32;
                                        b_sum += img_data.data[idx + 2] as u32;
                                    }
                                }
                                
                                let r = (r_sum / sample_size.max(1) as u32) as u8;
                                let g = (g_sum / sample_size.max(1) as u32) as u8;
                                let b = (b_sum / sample_size.max(1) as u32) as u8;
                                
                                draw_list
                                    .add_rect([x, y], [x + 200.0, y + 200.0], imgui::ImColor32::from_rgb(r, g, b))
                                    .filled(true)
                                    .build();
                                
                                // Add text overlay with image info
                                draw_list.add_text([x + 10.0, y + 90.0], imgui::ImColor32::WHITE, 
                                    &format!("{}x{}", img_data.width, img_data.height));
                                
                                ui.dummy([200.0, 200.0]);
                            } else {
                                ui.text("Loading album art...");
                                ui.dummy([200.0, 10.0]);
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
