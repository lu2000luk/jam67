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

const WORKER_URL: &str = "https://jam67.lu2000luk.workers.dev";

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
}

struct LobbyManagerState {
    connected_users: Vec<String>,
    connected_count: usize,
    current_sync: Option<LobbyEvent>,
    display_text: String,
}

impl LobbyManager {
    fn new(lobby_id: String, spotify: Arc<SpotifyController>, is_host: bool) -> Self {
        let (event_tx, _) = broadcast::channel(32);
        Self {
            lobby_id,
            spotify,
            event_tx,
            is_host,
            state: Arc::new(Mutex::new(LobbyManagerState {
                connected_users: Vec::new(),
                connected_count: 0,
                current_sync: None,
                display_text: String::from("Loading..."),
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
                    let connected_count = manager.get_connected_count();
                    let display_text = manager.get_display_text();

                    let title = if is_host {
                        format!("Host / Jam67")
                    } else {
                        "Guest / Jam67".to_string()
                    };

                    ui.window(&title)
                        .resizable(false)
                        .size([560.0, 240.0], Condition::FirstUseEver)
                        .movable(true)
                        .collapsible(false)
                        .build(|| {
                            ui.text(&display_text);
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
