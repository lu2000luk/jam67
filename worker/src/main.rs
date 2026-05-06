use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await?;

    let rooms: Arc<Mutex<HashMap<String, Vec<broadcast::Sender<String>>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    println!("[server] WebSocket server running on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        let rooms = rooms.clone();

        println!("[server] New connection from: {}", addr);

        tokio::spawn(async move {
            let ws_stream = match accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    eprintln!("[server] Handshake error: {}", e);
                    return;
                }
            };

            let (mut write, mut read) = ws_stream.split();

            let room_id = "default".to_string();

            let (tx, mut rx) = broadcast::channel::<String>(100);
            let tx_clone = tx.clone();

            {
                let mut rooms = rooms.lock().await;
                rooms.entry(room_id.clone()).or_insert_with(Vec::new).push(tx);
            }

            let room_id_read = room_id.clone();
            let tx_for_read = tx_clone.clone();
            let read_task = tokio::spawn(async move {
                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            println!("[server] Room {} << {}", room_id_read, text);
                            let _ = tx_for_read.send(text);
                        }
                        Ok(Message::Close(_)) => break,
                        Err(_) => break,
                        _ => {}
                    }
                }
            });

            let write_task = tokio::spawn(async move {
                while let Ok(msg) = rx.recv().await {
                    if write.send(Message::Text(msg.into())).await.is_err() {
                        break;
                    }
                }
            });

            tokio::select! {
                _ = read_task => {},
                _ = write_task => {},
            }

            println!("[server] Client disconnected from room: {}", room_id);
        });
    }
}