mod protocol;

use serde_json::json;
use std::{collections::HashMap, env, error::Error, net::{Ipv4Addr, SocketAddr}, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, UdpSocket}, sync::RwLock, time};
use tracing::{info, warn};

#[derive(Clone, serde::Serialize)]
struct Peer { id: String, name: String, address: String, last_seen_ms: u64, paired: bool }
type Peers = Arc<RwLock<HashMap<String, Peer>>>;
fn now_ms() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let id = env::var("NEXUS_DEVICE_ID").unwrap_or_else(|_| "nexus-development-identity".into());
    let name = env::var("NEXUS_DEVICE_NAME").unwrap_or_else(|_| "Nexus Desktop (discovery only)".into());
    let peers: Peers = Arc::new(RwLock::new(HashMap::new()));
    let udp = Arc::new(UdpSocket::bind((Ipv4Addr::UNSPECIFIED, protocol::KDE_PORT)).await?);
    udp.set_broadcast(true)?;
    let http = TcpListener::bind((Ipv4Addr::LOCALHOST, 8787)).await?;
    info!("Discovery listening on UDP 1716. Local dashboard API: http://127.0.0.1:8787/status");
    let recv_socket = udp.clone(); let recv_peers = peers.clone(); let own_id = id.clone();
    let listener = tokio::spawn(async move {
        let mut buf = [0u8; protocol::MAX_DATAGRAM];
        loop {
            match recv_socket.recv_from(&mut buf).await {
                Ok((len, addr)) => if let Some(identity) = protocol::parse_identity(&buf[..len]) {
                    if identity.device_id != own_id {
                        recv_peers.write().await.insert(identity.device_id.clone(), Peer {
                            id: identity.device_id, name: identity.device_name,
                            address: addr.ip().to_string(), last_seen_ms: now_ms(), paired: false,
                        });
                    }
                },
                Err(e) => warn!("UDP receive error: {e}"),
            }
        }
    });
    let broadcast_socket = udp.clone();
    let broadcaster = tokio::spawn(async move {
        let mut tick = time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            let body = json!({"id": now_ms(), "type": "kdeconnect.identity", "body": {
                "deviceId": id, "deviceName": name, "deviceType": "laptop",
                "protocolVersion": protocol::PROTOCOL_VERSION, "tcpPort": protocol::KDE_PORT,
                "incomingCapabilities": [], "outgoingCapabilities": [] }});
            let payload = format!("{body}\n");
            if let Err(e) = broadcast_socket.send_to(payload.as_bytes(), (Ipv4Addr::BROADCAST, protocol::KDE_PORT)).await {
                warn!("Broadcast error: {e}");
            }
        }
    });
    let api_peers = peers.clone();
    let api = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = http.accept().await else { break };
            let state = api_peers.clone();
            tokio::spawn(async move {
                let mut buf = [0_u8; 2048];
                let Ok(Ok(n)) = time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await else { return };
                if n == 0 { return; }
                let request = String::from_utf8_lossy(&buf[..n]);
                let valid = request.starts_with("GET /status HTTP/1.");
                let (status, body) = if valid {
                    let peers: Vec<Peer> = state.read().await.values().cloned().collect();
                    ("200 OK", json!({"mode":"discovery-only", "peers": peers}).to_string())
                } else { ("404 Not Found", "{}".to_owned()) };
                let response = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nCache-Control: no-store\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",body.len());
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    tokio::signal::ctrl_c().await?;
    listener.abort(); broadcaster.abort(); api.abort();
    info!("Shutdown requested");
    Ok(())
}
