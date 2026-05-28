use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex as TokioMutex;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const CDP_URL: &str = "http://127.0.0.1:3132";

#[derive(Debug, Clone)]
pub struct SpotifyController {
    client: reqwest::Client,
    ws_endpoint: Option<String>,
    request_id: Arc<AtomicU64>,
    tx: Arc<TokioMutex<Option<mpsc::UnboundedSender<(u64, String, serde_json::Value)>>>>,
    was_restarted: Arc<tokio::sync::Mutex<bool>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackInfo {
    pub title: String,
    pub artist: String,
    pub album: String,
    pub image_url: String,
    pub duration_ms: u64,
    pub progress_ms: u64,
    pub is_playing: bool,
}

#[derive(Deserialize)]
struct CdpResponse {
    id: Option<u64>,
    result: Option<serde_json::Value>,
    error: Option<serde_json::Value>,
}

impl SpotifyController {
    pub async fn connect() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();

        // Get the WebSocket endpoint
        let json_url = format!("{}/json", CDP_URL);
        let pages: Vec<HashMap<String, serde_json::Value>> =
            client.get(&json_url).send().await?.json().await?;

        let mut ws_endpoint = None;
        for page in pages {
            let page_type = page.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let ws_url = page.get("webSocketDebuggerUrl").and_then(|v| v.as_str());

            if page_type == "page" && ws_url.is_some() {
                ws_endpoint = ws_url.map(|s| s.to_string());
                break;
            }
        }

        let ws_endpoint = ws_endpoint.ok_or("No Spotify renderer page found")?;

        let instance = Self {
            client,
            ws_endpoint: Some(ws_endpoint),
            request_id: Arc::new(AtomicU64::new(1)),
            tx: Arc::new(TokioMutex::new(None)),
            was_restarted: Arc::new(tokio::sync::Mutex::new(false)),
        };

        // Test connection
        match instance.test_connection().await {
            Ok(_) => {
                println!("[jam67] Attached!");
                Ok(instance)
            }
            Err(e) => {
                eprintln!("Failed to execute test JS: {}", e);
                Err("Failed to connect to Spotify".into())
            }
        }
    }

    async fn test_connection(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.eval_js("console.log('[jam67] Attached!'); true")
            .await?;
        Ok(())
    }

    pub async fn mark_as_restarted(&self) {
        let mut was_restarted = self.was_restarted.lock().await;
        *was_restarted = true;
    }

    pub async fn init_player_api_with_retries(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let was_restarted = {
            let wr = self.was_restarted.lock().await;
            *wr
        };

        let initial_delay = if was_restarted {
            println!("[jam67] Spotify was restarted, waiting 2 seconds before searching for player API...");
            std::time::Duration::from_secs(2)
        } else {
            println!(
                "[jam67] Spotify was already running, searching for player API immediately..."
            );
            std::time::Duration::from_secs(0)
        };

        tokio::time::sleep(initial_delay).await;

        let max_retries = 5;
        for attempt in 1..=max_retries {
            println!(
                "[jam67] Searching for player API (attempt {}/{})",
                attempt, max_retries
            );

            match self.send_cdp_commands_sequence().await {
                Ok(result) => {
                    if result.as_bool().unwrap_or(false) {
                        println!("[jam67] Player API initialization completed successfully");
                        return Ok(());
                    } else {
                        println!("[jam67] Player API object not found yet");
                        if attempt < max_retries {
                            println!("[jam67] Retrying in 2 seconds...");
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[jam67] Error during player API search: {}", e);
                    if attempt < max_retries {
                        println!("[jam67] Retrying in 2 seconds...");
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                }
            }
        }

        eprintln!(
            "[jam67] Failed to find player API after {} attempts",
            max_retries
        );
        Err("Failed to find player API object after max retries".into())
    }

    async fn send_cdp_message(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let ws_url = self.ws_endpoint.as_ref().ok_or("No WebSocket endpoint")?;

        // Connect to WebSocket
        let (ws_stream, _) = connect_async(ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Send the CDP request
        let id = self.request_id.fetch_add(1, Ordering::SeqCst);
        let request = serde_json::json!({
            "id": id,
            "method": method,
            "params": params
        });

        println!("[jam67] Sending CDP request: {}: {}", method, request);

        write
            .send(Message::Text(request.to_string().into()))
            .await?;

        // Read response until we get our response (with timeout)
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(60); // 60s timeout for heavy operations like queryObjects

        while let Some(msg) = read.next().await {
            if start.elapsed() > timeout {
                return Err("CDP request timed out".into());
            }

            match msg? {
                Message::Text(text) => {
                    if method.contains("queryObjects") {
                        println!("[jam67] Raw CDP message for queryObjects: {}", text);
                    }
                    if let Ok(response) = serde_json::from_str::<CdpResponse>(&text) {
                        if response.id == Some(id) {
                            if method.contains("queryObjects") {
                                println!("[jam67] Parsed CdpResponse - id: {:?}, result: {:?}, error: {:?}",
                                    response.id, response.result.as_ref().map(|_| "..."), response.error);
                            }
                            if let Some(error) = response.error {
                                println!("[jam67] CDP error for {}: {}", method, error);
                                return Err(format!("CDP error: {}", error).into());
                            }
                            let result = response.result.unwrap_or(serde_json::Value::Null);
                            println!("[jam67] CDP response for {}: {}", method, result);
                            return Ok(result);
                        }
                    } else if method.contains("queryObjects") {
                        println!("[jam67] Failed to parse as CdpResponse");
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        Err("No response from CDP".into())
    }

    async fn send_cdp_commands_sequence(
        &self,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let ws_url = self.ws_endpoint.as_ref().ok_or("No WebSocket endpoint")?;

        // Keep a single WebSocket connection open for all commands
        let (ws_stream, _) = connect_async(ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Step 1: Get Object.prototype
        println!("[jam67] Step 1: Getting Object.prototype...");
        let id1 = self.request_id.fetch_add(1, Ordering::SeqCst);
        let request1 = serde_json::json!({
            "id": id1,
            "method": "Runtime.evaluate",
            "params": {
                "expression": "Object.prototype",
                "returnByValue": false
            }
        });
        println!("[jam67] Sending: {}", request1);
        write
            .send(Message::Text(request1.to_string().into()))
            .await?;

        let prototype_object_id = {
            let mut found = None;
            let start = std::time::Instant::now();
            let timeout = std::time::Duration::from_secs(10);

            while let Some(msg) = read.next().await {
                if start.elapsed() > timeout {
                    return Err("Timeout waiting for Runtime.evaluate response".into());
                }

                match msg? {
                    Message::Text(text) => {
                        println!("[jam67] Response: {}", text);
                        if let Ok(response) = serde_json::from_str::<CdpResponse>(&text) {
                            if response.id == Some(id1) {
                                if let Some(error) = response.error {
                                    return Err(format!("CDP error: {}", error).into());
                                }
                                let result = response
                                    .result
                                    .ok_or("No result in Runtime.evaluate response")?;
                                let oid = result
                                    .get("result")
                                    .and_then(|r| r.get("objectId"))
                                    .and_then(|id| id.as_str())
                                    .ok_or("Failed to get Object.prototype objectId")?
                                    .to_string();
                                println!("[jam67] Prototype objectId: {}", oid);
                                found = Some(oid);
                                break;
                            }
                        }
                    }
                    Message::Close(_) => return Err("WebSocket closed".into()),
                    _ => {}
                }
            }
            found.ok_or("Failed to get prototype object ID")?
        };

        // Step 2: Query all objects on the heap (using the SAME connection!)
        println!("[jam67] Step 2: Querying all objects on the heap (this may take a while)...");
        let id2 = self.request_id.fetch_add(1, Ordering::SeqCst);
        let request2 = serde_json::json!({
            "id": id2,
            "method": "Runtime.queryObjects",
            "params": {
                "prototypeObjectId": prototype_object_id
            }
        });
        println!("[jam67] Sending: {}", request2);
        write
            .send(Message::Text(request2.to_string().into()))
            .await?;

        let objects_array_id = {
            let mut found = None;
            let start = std::time::Instant::now();
            let timeout = std::time::Duration::from_secs(60); // queryObjects can take a while

            while let Some(msg) = read.next().await {
                if start.elapsed() > timeout {
                    return Err("Timeout waiting for Runtime.queryObjects response".into());
                }

                match msg? {
                    Message::Text(text) => {
                        println!("[jam67] QueryObjects response: {}", text);
                        if let Ok(response) = serde_json::from_str::<CdpResponse>(&text) {
                            if response.id == Some(id2) {
                                if let Some(error) = response.error {
                                    return Err(
                                        format!("CDP error in queryObjects: {}", error).into()
                                    );
                                }
                                let result = response
                                    .result
                                    .ok_or("No result in Runtime.queryObjects response")?;
                                println!("[jam67] QueryObjects result: {}", result);
                                let oid = result
                                    .get("objects")
                                    .and_then(|o| o.get("objectId"))
                                    .and_then(|id| id.as_str())
                                    .ok_or("Failed to extract objects objectId")?
                                    .to_string();
                                println!("[jam67] Objects array objectId: {}", oid);
                                found = Some(oid);
                                break;
                            }
                        }
                    }
                    Message::Close(_) => return Err("WebSocket closed".into()),
                    _ => {}
                }
            }
            found.ok_or("Failed to get objects array ID")?
        };

        // Step 3: Call function to find the player object
        println!("[jam67] Step 3: Searching for player API object...");
        let id3 = self.request_id.fetch_add(1, Ordering::SeqCst);
        let function_decl = r#"
            function() {
                const props = [
                    '_contextPlayer', '_contextualShuffle', '_defaultFeatureVersion',
                    '_events', '_isLikedSongsListPlatformEnabled', '_isSleepTimerEnabled',
                    '_playlistPlayServiceClient', '_playlistResyncerAPI', '_queue', '_sleepTimerCore'
                ];
                const found = this.find(obj =>
                    obj && props.every(p => Object.prototype.hasOwnProperty.call(obj, p))
                );
                if (found) {
                    window.JAM67_PLAYERAPI = found;
                    return true;
                }
                return false;
            }
        "#;

        let request3 = serde_json::json!({
            "id": id3,
            "method": "Runtime.callFunctionOn",
            "params": {
                "objectId": objects_array_id,
                "functionDeclaration": function_decl,
                "returnByValue": true
            }
        });
        println!("[jam67] Sending callFunctionOn...");
        write
            .send(Message::Text(request3.to_string().into()))
            .await?;

        {
            let start = std::time::Instant::now();
            let timeout = std::time::Duration::from_secs(30);

            while let Some(msg) = read.next().await {
                if start.elapsed() > timeout {
                    return Err("Timeout waiting for Runtime.callFunctionOn response".into());
                }

                match msg? {
                    Message::Text(text) => {
                        println!("[jam67] CallFunctionOn response: {}", text);
                        if let Ok(response) = serde_json::from_str::<CdpResponse>(&text) {
                            if response.id == Some(id3) {
                                if let Some(error) = response.error {
                                    return Err(
                                        format!("CDP error in callFunctionOn: {}", error).into()
                                    );
                                }
                                let result = response
                                    .result
                                    .ok_or("No result in Runtime.callFunctionOn response")?;
                                let success = result
                                    .get("result")
                                    .and_then(|r| r.get("value"))
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);

                                if success {
                                    println!("[jam67] Player API object found and stored in window.JAM67_PLAYERAPI");
                                } else {
                                    eprintln!("[jam67] Failed to find player API object with expected properties");
                                }

                                return Ok(serde_json::json!(success));
                            }
                        }
                    }
                    Message::Close(_) => return Err("WebSocket closed".into()),
                    _ => {}
                }
            }
        }

        Err("Failed to complete CDP sequence".into())
    }

    async fn init_player_api(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("[jam67] Starting player API initialization...");

        match self.send_cdp_commands_sequence().await {
            Ok(_) => {
                println!("[jam67] Player API initialization completed");
                Ok(())
            }
            Err(e) => {
                eprintln!("[jam67] Error during player API initialization: {}", e);
                eprintln!("[jam67] Continuing without player API...");
                Ok(()) // Don't fail the connection
            }
        }
    }

    async fn eval_js(
        &self,
        expression: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let ws_url = self.ws_endpoint.as_ref().ok_or("No WebSocket endpoint")?;

        // Connect to WebSocket
        let (ws_stream, _) = connect_async(ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Send the evaluate request
        let id = self.request_id.fetch_add(1, Ordering::SeqCst);
        let request = serde_json::json!({
            "id": id,
            "method": "Runtime.evaluate",
            "params": {
                "expression": expression,
                "returnByValue": true,
                "awaitPromise": true
            }
        });

        write
            .send(Message::Text(request.to_string().into()))
            .await?;

        // Read response until we get our response
        while let Some(msg) = read.next().await {
            match msg? {
                Message::Text(text) => {
                    if let Ok(response) = serde_json::from_str::<CdpResponse>(&text) {
                        if response.id == Some(id) {
                            let result = response
                                .result
                                .and_then(|r| {
                                    r.get("result").and_then(|rr| rr.get("value")).cloned()
                                })
                                .unwrap_or(serde_json::Value::Null);
                            return Ok(result);
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        Err("No response from CDP".into())
    }

    pub async fn get_title(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                const el = document.querySelector(".main-nowPlayingWidget-trackInfo")?.children[0];
                return el ? el.textContent : "";
            })()
        "###;
        let val = self.eval_js(js).await?;
        Ok(val.as_str().unwrap_or("").to_string())
    }

    pub async fn get_artist(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                const el = document.querySelector(".main-nowPlayingWidget-trackInfo")?.children[2];
                return el ? el.textContent : "";
            })()
        "###;
        let val = self.eval_js(js).await?;
        Ok(val.as_str().unwrap_or("").to_string())
    }

    pub async fn get_album(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(String::new())
    }

    pub async fn get_image_url(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                const imgs = document.querySelectorAll(".cover-art-image");
                return imgs.length > 0 ? imgs[0].src : "";
            })()
        "###;
        let val = self.eval_js(js).await?;
        Ok(val.as_str().unwrap_or("").to_string())
    }

    pub async fn get_duration_ms(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                const el = document.querySelectorAll('[data-testid="playback-duration"]')[0];
                if (!el) return 0;
                const [minutes, seconds] = el.textContent.split(':').map(Number);
                return (minutes * 60000) + (seconds * 1000);
            })()
        "###;
        let val = self.eval_js(js).await?;
        Ok(val.as_u64().unwrap_or(0))
    }

    pub async fn get_progress_ms(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                const el = document.querySelectorAll('[data-testid="playback-position"]')[0];
                if (!el) return 0;
                const [minutes, seconds] = el.textContent.split(':').map(Number);
                return (minutes * 60000) + (seconds * 1000);
            })()
        "###;
        let val = self.eval_js(js).await?;
        Ok(val.as_u64().unwrap_or(0))
    }

    pub async fn get_is_playing(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                const btn = document.querySelectorAll('[data-testid="control-button-playpause"]')[0];
                return btn ? !btn.ariaLabel.includes("y") : false;
            })()
        "###;
        let val = self.eval_js(js).await?;
        Ok(val.as_bool().unwrap_or(false))
    }

    pub async fn get_id(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                const el = document.querySelectorAll('[data-testid="track-visual-enhancement"]')[0];
                if (!el || !el.children[0] || !el.children[0].children[0]) return "";
                const href = el.children[0].children[0].href;
                const parts = href.split("spotify%3Atrack%3A");
                return parts.length > 1 ? parts[1] : "";
            })()
        "###;
        let val = self.eval_js(js).await?;
        Ok(val.as_str().unwrap_or("").to_string())
    }

    async fn toggle_playpause(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                document.querySelectorAll('[data-testid="control-button-playpause"]')[0].click();
            })()
        "###;
        self.eval_js(js).await?;
        Ok(())
    }

    pub async fn set_play(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                try { window.JAM67_PLAYERAPI.resume(); } catch { Spicetify.Player.play(); }
            })()
        "###;
        self.eval_js(js).await?;
        Ok(())
    }

    pub async fn set_pause(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                try { window.JAM67_PLAYERAPI.pause(); } catch { Spicetify.Player.pause(); }
            })()
        "###;
        self.eval_js(js).await?;
        Ok(())
    }

    pub async fn set_seek(
        &self,
        position_ms: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let js = format!(
            r###"
            (() => {{
                const pos = {};
                if (!window.JAM67_PLAYERAPI) return false;

                // Try the internal player API first (most stable)
                if (typeof window.JAM67_PLAYERAPI.seekTo === 'function') {{
                    window.JAM67_PLAYERAPI.seekTo(pos);
                    return true;
                }}

                // Fallback: generic seek if exposed with a different name
                if (typeof window.JAM67_PLAYERAPI.seek === 'function') {{
                    window.JAM67_PLAYERAPI.seek(pos);
                    return true;
                }}

                return false;
            }})()
            "###,
            position_ms
        );

        let ok = self.eval_js(&js).await?.as_bool().unwrap_or(false);
        if ok {
            return Ok(());
        }

        // Last fallback: UI-based seek (kept as backup only)
        let duration_ms = self.get_duration_ms().await.unwrap_or(1);
        let pct = if duration_ms > 0 {
            (position_ms as f64 / duration_ms as f64) * 100.0
        } else {
            0.0
        };
        let pct = pct.clamp(0.0, 100.0);
        let fallback_js = format!(
            r###"
            (() => {{
                const p = {};
                const e = document.querySelector('[data-testid="progress-bar"]');
                if (!e) return;
                const r = e.getBoundingClientRect();
                const x = r.left + r.width * Math.max(0, Math.min(100, p)) / 100;
                const y = r.top + r.height / 2;
                const o = {{bubbles: true, cancelable: true, clientX: x, clientY: y, button: 0}};
                const t = document.elementFromPoint(x, y) || e;
                ['pointerdown','mousedown','pointerup','mouseup','click']
                    .forEach(k => t.dispatchEvent(new MouseEvent(k, o)));
            }})()
            "###,
            pct
        );
        self.eval_js(&fallback_js).await?;
        Ok(())
    }

    pub async fn set_track(
        &self,
        track_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let js = format!(
            r###"
            (() => {{
               // window.JAM67_PLAYERAPI.play({{ uri: "spotify:track:{}" }}, {{}}, {{}});
               Spicetify.Player.playUri("spotify:track:{}");
            }})()
            "###,
            track_id, track_id
        );
        self.eval_js(&js).await?;
        Ok(())
    }

    pub async fn get_track_info(
        &self,
    ) -> Result<TrackInfo, Box<dyn std::error::Error + Send + Sync>> {
        let title = self.get_title().await.unwrap_or_default();
        let artist = self.get_artist().await.unwrap_or_default();
        let image_url = self.get_image_url().await.unwrap_or_default();
        let duration_ms = self.get_duration_ms().await.unwrap_or_default();
        let progress_ms = self.get_progress_ms().await.unwrap_or_default();
        let is_playing = self.get_is_playing().await.unwrap_or(false);

        Ok(TrackInfo {
            title,
            artist,
            album: String::new(),
            image_url,
            duration_ms,
            progress_ms,
            is_playing,
        })
    }
}
