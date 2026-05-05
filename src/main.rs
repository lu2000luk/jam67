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
use std::os::windows::ffi::OsStringExt;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use windows::Win32::Foundation::{HGLOBAL, HWND};
use windows::Win32::System::DataExchange::{CloseClipboard, GetClipboardData, OpenClipboard};
use windows::Win32::System::Memory::{GlobalLock, GlobalUnlock};

const PLAYER_API_URL: &str = "https://api.spotify.com/v1/me/player";
const WORKER_URL: &str = "https://jam67.lu2000luk.workers.dev";

fn read_clipboard_text() -> Option<String> {
    unsafe {
        if OpenClipboard(Some(HWND::default())).is_err() {
            return None;
        }
        let clipboard_data = match GetClipboardData(13_u32) {
            Ok(data) => data,
            Err(_) => {
                let _ = CloseClipboard();
                return None;
            }
        };
        let hglobal = HGLOBAL(clipboard_data.0);
        let global_lock = GlobalLock(hglobal);
        if global_lock.is_null() {
            let _ = CloseClipboard();
            return None;
        }
        let mut len = 0usize;
        while *((global_lock as *const u16).add(len)) != 0 {
            len += 1;
        }
        let text = std::ffi::OsString::from_wide(std::slice::from_raw_parts(
            global_lock as *const u16,
            len,
        ))
        .to_string_lossy()
        .into_owned();
        let _ = GlobalUnlock(hglobal);
        let _ = CloseClipboard();
        Some(text)
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlayerState {
    #[serde(rename = "is_playing")]
    pub is_playing: bool,
    #[serde(rename = "item")]
    pub item: Option<PlayerItem>,
    #[serde(rename = "timestamp")]
    pub timestamp: i64,
    #[serde(rename = "progress_ms")]
    pub progress_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlayerItem {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "artists")]
    pub artists: Vec<Artist>,
    #[serde(rename = "duration_ms")]
    pub duration_ms: i64,
    #[serde(rename = "album")]
    pub album: Option<Album>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artist {
    #[serde(rename = "name")]
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Album {
    #[serde(rename = "images")]
    pub images: Vec<AlbumImage>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AlbumImage {
    #[serde(rename = "url")]
    pub url: String,
}

struct LobbyManager {
    lobby_id: String,
    #[allow(dead_code)]
    auth_token: String,
    event_tx: broadcast::Sender<LobbyEvent>,
    #[allow(dead_code)]
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
    fn new(lobby_id: String, auth_token: String, is_host: bool) -> Self {
        let (event_tx, _) = broadcast::channel(32);
        Self {
            lobby_id,
            auth_token,
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
            }
        }
        let _ = self.event_tx.send(event);
    }

    fn get_connected_count(&self) -> usize {
        self.state.lock().unwrap().connected_count
    }

    fn get_display_text(&self) -> String {
        self.state.lock().unwrap().display_text.clone()
    }

    #[allow(dead_code)]
    async fn listen_events(
        &self,
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
                        let _ = self.event_tx.send(event);
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

    #[allow(dead_code)]
    async fn publish_event(
        &self,
        event: LobbyEvent,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();
        let url = format!("{}/push?id={}", WORKER_URL, self.lobby_id);
        let body = serde_json::to_string(&event)?;

        client
            .post(&url)
            .header("Authorization", &self.auth_token)
            .body(body)
            .send()
            .await?;

        Ok(())
    }

    #[allow(dead_code)]
    fn subscribe(&self) -> broadcast::Receiver<LobbyEvent> {
        self.event_tx.subscribe()
    }

    fn auth_header(&self) -> String {
        if self.auth_token.starts_with("Bearer ") {
            self.auth_token.clone()
        } else {
            format!("Bearer {}", self.auth_token)
        }
    }

    async fn fetch_player_state(
        &self,
    ) -> std::result::Result<PlayerState, Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();

        let response = client
            .get(PLAYER_API_URL)
            .header("Authorization", self.auth_header())
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(format!("Spotify API returned {}", response.status()).into());
        }

        let body = response.text().await?;
        let state = serde_json::from_str::<PlayerState>(&body)?;
        Ok(state)
    }

    async fn host_polling_loop(
        self: &Arc<Self>,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let polling_interval = std::time::Duration::from_millis(2500);

        loop {
            tokio::time::sleep(polling_interval).await;

            match self.fetch_player_state().await {
                Ok(state) => {
                    let is_playing = state.is_playing;
                    let paused = !is_playing;

                    if let Some(item) = state.item {
                        let id = item.id.clone();
                        let name = item.name.clone();
                        let author = item
                            .artists
                            .first()
                            .map(|a| a.name.clone())
                            .unwrap_or_else(|| "Unknown".to_string());
                        let image_url = item
                            .album
                            .as_ref()
                            .and_then(|album| album.images.first())
                            .map(|img| img.url.clone())
                            .unwrap_or_else(|| String::new());
                        let timestamp_ms = state.timestamp as u64;
                        let duration_ms = item.duration_ms as u64;

                        let sync_event = LobbyEvent::Sync {
                            id,
                            name,
                            author,
                            image_url,
                            timestamp_ms,
                            duration_ms,
                            paused,
                        };

                        let _ = self.publish_event_sync(sync_event);
                    } else {
                        let sync_event = LobbyEvent::Sync {
                            id: String::new(),
                            name: String::new(),
                            author: String::new(),
                            image_url: String::new(),
                            timestamp_ms: 0,
                            duration_ms: 0,
                            paused,
                        };

                        let _ = self.publish_event_sync(sync_event);
                    }
                }
                Err(e) => {
                    eprintln!("Error fetching player state: {}", e);
                }
            }
        }
    }

    #[allow(dead_code)]
    fn publish_event_sync(
        &self,
        event: LobbyEvent,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::blocking::Client::new();
        let url = format!("{}/push?id={}", WORKER_URL, self.lobby_id);
        let body = serde_json::to_string(&event)?;

        client
            .post(&url)
            .header("Authorization", &self.auth_token)
            .body(body)
            .send()?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut app = Windows::new(&WindowsOptions::default())?;
    let mut has_set_auth_token = false;
    let mut is_in_lobby = false;
    let mut is_joining_lobby = false;
    let mut is_creating_lobby = false;
    let mut auth_token = String::new();
    let mut lobby_code = String::new();
    let mut is_host = false;
    let mut lobby_manager: Option<Arc<LobbyManager>> = None;

    app.run(move |ui, _style| {
        if !has_set_auth_token {
            ui.window("Autenticazione / Jam67")
                .resizable(false)
                .size([400.0, 100.0], Condition::FirstUseEver)
                .movable(true)
                .collapsible(false)
                .build(|| {
                    ui.text("Token di autenticazione Spotify");
                    ui.input_text(" ", &mut auth_token).build();
                    ui.same_line();
                    if ui.button("<-") {
                        if let Some(text) = read_clipboard_text() {
                            auth_token = text;
                        }
                    }
                    ui.spacing();

                    if ui.button("Esci") {
                        std::process::exit(0);
                    }

                    ui.same_line();

                    if ui.button("Crea lobby") {
                        has_set_auth_token = true;
                        is_creating_lobby = true;
                        is_host = true;
                    }

                    ui.same_line();

                    if ui.button("Entra in una lobby") {
                        has_set_auth_token = true;
                        is_joining_lobby = true;
                        is_host = false;
                    }
                });
        } else {
            if is_joining_lobby {
                ui.window("Join / Jam67")
                    .resizable(false)
                    .size([320.0, 100.0], Condition::FirstUseEver)
                    .movable(true)
                    .collapsible(false)
                    .build(|| {
                        ui.text("Codice lobby");
                        ui.input_text(" ", &mut lobby_code).build();
                        ui.spacing();

                        ui.spacing();

                        if ui.button("Esci") {
                            std::process::exit(0);
                        }

                        ui.same_line();

                        if ui.button("Connetti") {
                            let code = lobby_code.clone();
                            let token = auth_token.clone();

                            lobby_manager = Some(Arc::new(LobbyManager::new(code, token, false)));

                            if let Some(ref manager) = lobby_manager {
                                let mgr = manager.clone();
                                std::thread::spawn(move || {
                                    let rt = tokio::runtime::Runtime::new().unwrap();
                                    rt.block_on(async move {
                                        if let Err(e) = mgr.listen_events_with_handler().await {
                                            eprintln!("Listen error: {}", e);
                                        }
                                    });
                                });
                            }

                            is_joining_lobby = false;
                            is_in_lobby = true;
                        }
                    });
            } else if is_creating_lobby {
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

                let code = lobby_code.clone();
                let token = auth_token.clone();

                if code.is_empty() {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    lobby_code = format!("{:x}", now);
                }

                if lobby_manager.is_none() {
                    lobby_manager =
                        Some(Arc::new(LobbyManager::new(lobby_code.clone(), token, true)));

                    if let Some(ref manager) = lobby_manager {
                        let mgr = manager.clone();
                        std::thread::spawn(move || {
                            let rt = tokio::runtime::Runtime::new().unwrap();
                            rt.block_on(async move {
                                if let Err(e) = mgr.listen_events_with_handler().await {
                                    eprintln!("Listen error: {}", e);
                                }
                            });
                        });

                        let mgr2 = manager.clone();
                        std::thread::spawn(move || {
                            let rt = tokio::runtime::Runtime::new().unwrap();
                            rt.block_on(async move {
                                if let Err(e) = mgr2.host_polling_loop().await {
                                    eprintln!("Polling error: {}", e);
                                }
                            });
                        });
                    }

                    is_creating_lobby = false;
                    is_in_lobby = true;
                }
            } else {
                if let Some(ref manager) = lobby_manager {
                    let connected_count = manager.get_connected_count();
                    let display_text = manager.get_display_text();

                    let title = if is_host {
                        format!("Host - {} / Jam67", connected_count)
                    } else {
                        format!("Guest / Jam67")
                    };

                    ui.window(&title)
                        .resizable(false)
                        .size([400.0, 120.0], Condition::FirstUseEver)
                        .movable(true)
                        .collapsible(false)
                        .build(|| {
                            ui.text(&display_text);
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
