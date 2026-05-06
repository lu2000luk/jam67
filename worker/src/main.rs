use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{accept_async, tungstenite::Message};

type ClientMap = Arc<Mutex<HashMap<String, Vec<tokio::sync::mpsc::Sender<String>>>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await?;

    let rooms: ClientMap = Arc::new(Mutex::new(HashMap::new()));

    println!("[server] WebSocket server running on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        let rooms = rooms.clone();

        println!("[server] New connection from: {}", addr);

        tokio::spawn(async move {
            let mut buffer = [0u8; 1024];
            let n = match stream.peek(&mut buffer).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };

            let request_str = std::str::from_utf8(&buffer[..n]).unwrap_or("");
            let room_id = if let Some(query_start) = request_str.find('?') {
                if let Some(id_start) = request_str[query_start..].find("id=") {
                    let id_value_start = query_start + id_start + 3;
                    let id_value_end = request_str[id_value_start..].find('&')
                        .map(|p| id_value_start + p)
                        .unwrap_or(request_str.len());
                    request_str[id_value_start..id_value_end].to_string()
                } else {
                    "default".to_string()
                }
            } else {
                "default".to_string()
            };

            println!("[server] Client joining room: {}", room_id);

            let ws_stream = match accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    eprintln!("[server] Handshake error: {}", e);
                    return;
                }
            };

            let (mut write, mut read) = ws_stream.split();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);

            // Add this client to the room
            {
                let mut rooms = rooms.lock().await;
                rooms.entry(room_id.clone()).or_insert_with(Vec::new).push(tx);
            }

            // Read task - receive messages from this client and broadcast to others
            let rooms_for_read = rooms.clone();
            let room_id_for_log = room_id.clone();
            let read_task = tokio::spawn(async move {
                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            println!("[server] Room {} << {}", room_id_for_log, text);
                            
                            // Send to all OTHER clients in the room
                            let rooms = rooms_for_read.lock().await;
                            if let Some(clients) = rooms.get(&room_id_for_log) {
                                for client in clients {
                                    if client.try_send(text.clone()).is_err() {
                                        // Client disconnected
                                    }
                                }
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Err(_) => break,
                        _ => {}
                    }
                }
            });

            // Write task - send messages to this client
            let write_task = tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
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
            
            // Remove this client from the room
            let mut rooms = rooms.lock().await;
            if let Some(clients) = rooms.get_mut(&room_id) {
                clients.retain(|c| !c.is_closed());
                if clients.is_empty() {
                    rooms.remove(&room_id);
                }
            }
        });
    }
}