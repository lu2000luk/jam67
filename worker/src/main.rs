use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, broadcast};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

/// Each room has a broadcast channel.
/// Messages are (sender_id, message) so receivers can skip their own messages.
type Rooms = Arc<Mutex<HashMap<String, broadcast::Sender<(String, Message)>>>>;

const CHANNEL_CAPACITY: usize = 256;

#[tokio::main]
async fn main() {
    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await.expect("Failed to bind");
    println!("[ws-server] Listening on {}", addr);

    let rooms: Rooms = Arc::new(Mutex::new(HashMap::new()));

    while let Ok((stream, peer)) = listener.accept().await {
        println!("[ws-server] New TCP connection from {}", peer);
        let rooms = rooms.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, rooms).await {
                eprintln!("[ws-server] Connection error: {}", e);
            }
        });
    }
}

/// Extract the `id` query parameter from the WebSocket upgrade request URI.
fn extract_room_id(uri: &str) -> Option<String> {
    // URI looks like "/?id=abc123" or "/?id=abc123&foo=bar"
    let query = uri.split('?').nth(1)?;
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        if let (Some(key), Some(value)) = (kv.next(), kv.next()) {
            if key == "id" {
                return Some(value.to_string());
            }
        }
    }
    None
}

async fn handle_connection(
    stream: TcpStream,
    rooms: Rooms,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Perform the WebSocket handshake, capturing the request URI
    let mut room_id = String::new();

    let ws_stream = tokio_tungstenite::accept_hdr_async(
        stream,
        |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
         res: tokio_tungstenite::tungstenite::handshake::server::Response| {
            let uri = req.uri().to_string();
            room_id = extract_room_id(&uri).unwrap_or_default();
            Ok(res)
        },
    )
    .await?;

    if room_id.is_empty() {
        eprintln!("[ws-server] No room id provided, dropping connection");
        return Ok(());
    }

    let client_id = Uuid::new_v4().to_string();
    println!(
        "[ws-server] Client {} joined room \"{}\"",
        client_id, room_id
    );

    // Get or create the broadcast channel for this room
    let tx = {
        let mut rooms_lock = rooms.lock().await;
        rooms_lock
            .entry(room_id.clone())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
                tx
            })
            .clone()
    };

    let mut rx = tx.subscribe();
    let (mut ws_write, mut ws_read) = ws_stream.split();

    // Task: forward broadcast messages to this client (skip own messages)
    let client_id_clone = client_id.clone();
    let send_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok((sender_id, msg)) => {
                    if sender_id == client_id_clone {
                        continue; // don't echo back to sender
                    }
                    if ws_write.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("[ws-server] Client {} lagged by {} messages", client_id_clone, n);
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Task: read from this client and broadcast to the room
    let client_id_clone = client_id.clone();
    let tx_clone = tx.clone();
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_read.next().await {
            match msg {
                Message::Text(text) => {
                    let _ = tx_clone.send((client_id_clone.clone(), Message::Text(text)));
                }
                Message::Binary(data) => {
                    let _ = tx_clone.send((client_id_clone.clone(), Message::Binary(data)));
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    // Wait for either task to finish (client disconnect)
    tokio::select! {
        _ = send_task => {},
        _ = recv_task => {},
    }

    println!(
        "[ws-server] Client {} left room \"{}\"",
        client_id, room_id
    );

    // Clean up empty rooms
    {
        let mut rooms_lock = rooms.lock().await;
        if let Some(tx) = rooms_lock.get(&room_id) {
            if tx.receiver_count() == 0 {
                rooms_lock.remove(&room_id);
                println!("[ws-server] Room \"{}\" removed (empty)", room_id);
            }
        }
    }

    Ok(())
}
