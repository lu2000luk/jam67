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
        let playing = self.get_is_playing().await.unwrap_or(true);
        if !playing {
            self.toggle_playpause().await?;
        }
        Ok(())
    }

    pub async fn set_pause(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let playing = self.get_is_playing().await.unwrap_or(false);
        if playing {
            self.toggle_playpause().await?;
        }
        Ok(())
    }

    pub async fn set_seek(
        &self,
        position_ms: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let duration_ms = self.get_duration_ms().await.unwrap_or(1);
        let pct = if duration_ms > 0 {
            (position_ms as f64 / duration_ms as f64) * 100.0
        } else {
            0.0
        };
        let pct = pct.clamp(0.0, 100.0);
        let js = format!(
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
