use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const CDP_URL: &str = "http://127.0.0.1:3132";

#[derive(Debug, Clone)]
pub struct SpotifyController {
    client: reqwest::Client,
    ws_endpoint: String,
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

#[derive(Serialize)]
struct CdpRequest {
    id: u64,
    method: String,
    params: serde_json::Value,
}

#[derive(Deserialize, Debug)]
struct CdpResponse {
    #[allow(dead_code)]
    id: Option<u64>,
    result: Option<serde_json::Value>,
    #[allow(dead_code)]
    error: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct JsonVersionResponse {
    #[serde(rename = "webSocketDebuggerUrl")]
    web_socket_debugger_url: String,
}

impl SpotifyController {
    pub async fn connect() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();
        let json_url = format!("{}/json/version", CDP_URL);
        let resp: JsonVersionResponse = client.get(&json_url).send().await?.json().await?;
        Ok(Self {
            client,
            ws_endpoint: resp.web_socket_debugger_url,
        })
    }

    async fn eval_js(
        &self,
        expression: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let session_id = self.get_renderer_session().await?;
        let result = self.runtime_evaluate(&session_id, expression).await?;
        Ok(result)
    }

    async fn get_renderer_session(
        &self,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let list_url = format!("{}/json", CDP_URL);
        let pages: Vec<HashMap<String, serde_json::Value>> =
            self.client.get(&list_url).send().await?.json().await?;

        for page in &pages {
            let page_type = page.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let url = page.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let ws_url = page
                .get("webSocketDebuggerUrl")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            if page_type == "page" && !url.is_empty() && !ws_url.is_empty() {
                return Ok(ws_url.to_string());
            }
        }

        Err("No Spotify renderer page found".into())
    }

    async fn cdp_call(
        &self,
        ws_url: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let request = CdpRequest {
            id: rand::random(),
            method: method.to_string(),
            params,
        };

        let response = self
            .client
            .post(ws_url)
            .json(&request)
            .send()
            .await?
            .json::<CdpResponse>()
            .await?;

        response
            .result
            .ok_or_else(|| "CDP call returned no result".into())
    }

    async fn runtime_evaluate(
        &self,
        ws_url: &str,
        expression: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let params = serde_json::json!({
            "expression": expression,
            "returnByValue": true,
            "awaitPromise": true,
        });

        let result = self.cdp_call(ws_url, "Runtime.evaluate", params).await?;
        Ok(result
            .get("result")
            .and_then(|v| v.get("value"))
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }

    // -------------------------------------------------------------------------
    // Script slots - populate these with the JS your custom code needs
    // -------------------------------------------------------------------------

    pub async fn get_title(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                const el = document.querySelectorAll('[data-testid="context-item-info-title"]')[0];
                return el ? el.textContent : "";
            })()
        "###;
        let val = self.eval_js(js).await?;
        Ok(val.as_str().unwrap_or("").to_string())
    }

    pub async fn get_artist(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                const el = document.querySelectorAll('[data-testid="context-item-info-artist"]')[0];
                return el ? el.textContent : "";
            })()
        "###;
        let val = self.eval_js(js).await?;
        Ok(val.as_str().unwrap_or("").to_string())
    }

    pub async fn get_album(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                // TODO: add album selector when needed
                return "";
            })()
        "###;
        let val = self.eval_js(js).await?;
        Ok(val.as_str().unwrap_or("").to_string())
    }

    pub async fn get_image_url(
        &self,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let js = r###"
            (() => {
                const imgs = document.querySelectorAll('[data-testid="cover-art-image"]');
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

    // -------------------------------------------------------------------------
    // Setters / actions
    // -------------------------------------------------------------------------

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