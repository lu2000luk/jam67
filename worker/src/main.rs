use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};
use tokio_tungstenite::{accept_async, tungstenite::Message};

type Room = Arc<Mutex<HashMap<String, Vec::tokio::sync::broadcast::Sender<String>>>>>;

async fn handle_connection(
    stream: TcpStream,
    rooms: Room,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_stream = accept_async(stream).await?;
    let (mut write, mut read) = ws_stream.split();

    let addr = read.remote_addr()?;
    println!("[server] New connection from: {}", addr);

    let room_id = {
        let request = ws_stream.get_ref().request();
        let uri = request.uri();
        let query = uri.query().unwrap_or("");
        query.strip_prefix("id=").unwrap_or("default").to_string()
    };

    println!("[server] Client joining room: {}", room_id);

    let (tx, mut rx) = broadcast::channel::<String>(100);

    {
        let mut rooms = rooms.lock().await;
        rooms.entry(room_id.clone()).or_insert_with(Vec::new).push(tx.clone());
    }

    let read_task = tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    println!("[server] Room {} << {}", room_id, text);
                    let _ = tx.send(text);
                }
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    eprintln!("[server] Error: {}", e);
                    break;
                }
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
    {
        let mut rooms = rooms.lock().await;
        if let Some(senders) = rooms.get_mut(&room_id) {
            senders.retain(|s| !s.is_closed());
            if senders.is_empty() {
                rooms.remove(&room_id);
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await?;

    let rooms: Room = Arc::new(Mutex::new(HashMap::new()));

    println!("[server] WebSocket server running on {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let rooms = rooms.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, rooms).await {
                eprintln!("[server] Connection error: {}", e);
            }
        });
    }
}