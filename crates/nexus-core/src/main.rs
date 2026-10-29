mod peers;
mod protocol;

use serde_json::json;
use std::{env, error::Error, net::Ipv4Addr, sync::Arc, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, UdpSocket}, sync::{RwLock, Semaphore}, time};
use tracing::{info, warn};

fn now_ms() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64 }

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let id = env::var("NEXUS_DEVICE_ID").unwrap_or_else(|_| format!("nexus-session-{}-{}", std::process::id(), now_ms()));
    let name = env::var("NEXUS_DEVICE_NAME").unwrap_or_else(|_| "Nexus Desktop (discovery only)".into());
    let peers = Arc::new(RwLock::new(peers::PeerStore::default()));
    let udp = Arc::new(UdpSocket::bind((Ipv4Addr::UNSPECIFIED, protocol::KDE_PORT)).await?);
    udp.set_broadcast(true)?;
    let http = TcpListener::bind((Ipv4Addr::LOCALHOST, 8787)).await?;
    info!("Passive discovery on UDP 1716; read-only local status on 127.0.0.1:8787");

    let listener = {
        let socket = udp.clone(); let state = peers.clone(); let own_id = id.clone();
        tokio::spawn(async move {
            let mut buf = [0_u8; protocol::MAX_DATAGRAM];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, source)) => {
                        if let Some(identity) = protocol::parse_identity(&buf[..n]) {
                            if identity.device_id != own_id {
                                state.write().await.observe(identity, source.ip(), Instant::now());
                            }
                        }
                    },
                    Err(e) => warn!("Discovery receive error: {e}"),
                }
            }
        })
    };
    let broadcaster = {
        let socket = udp.clone();
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_secs(10));
            loop {
                ticker.tick().await;
                let payload = format!("{}\n", json!({"id": now_ms(), "type":"kdeconnect.identity", "body": {
                    "deviceId": id, "deviceName": name, "deviceType": "laptop", "protocolVersion": protocol::PROTOCOL_VERSION,
                    "tcpPort": protocol::KDE_PORT, "incomingCapabilities": [], "outgoingCapabilities": []
                }}));
                if let Err(e) = socket.send_to(payload.as_bytes(), (Ipv4Addr::BROADCAST, protocol::KDE_PORT)).await {
                    warn!("Broadcast failed: {e}");
                }
            }
        })
    };
    let api = {
        let state = peers.clone();
        tokio::spawn(async move {
            let slots = Arc::new(Semaphore::new(32));
            loop {
                let Ok((mut stream, _)) = http.accept().await else { break };
                let Ok(slot) = slots.clone().try_acquire_owned() else { continue };
                let state = state.clone();
                tokio::spawn(async move {
                    let _slot = slot;
                    let mut buf = [0_u8; 2048];
                    let Ok(Ok(n)) = time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await else { return };
                    if n == 0 { return; }
                    let valid = buf[..n].starts_with(b"GET /status HTTP/1.");
                    let (status, body) = if valid {
                        let snapshot = state.write().await.snapshot(Instant::now());
                        ("200 OK", json!({"mode":"discovery-only", "trusted":false, "peers":snapshot}).to_string())
                    } else { ("404 Not Found", "{}".to_string()) };
                    let reply = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len());
                    let _ = time::timeout(Duration::from_secs(2), stream.write_all(reply.as_bytes())).await;
                });
            }
        })
    };
    tokio::signal::ctrl_c().await?;
    listener.abort(); broadcaster.abort(); api.abort();
    info!("Shutdown requested");
    Ok(())
}
