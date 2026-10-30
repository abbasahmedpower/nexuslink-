mod peers;
mod protocol;

use async_trait::async_trait;
use nexus_adapter_kde::CliKdeAdapter;
use nexus_adapter_scrcpy::ProcessScrcpySupervisor;
use nexus_application::{DiscoveryProvider, HubApplication};
use nexus_domain::DiscoveredPeerInfo;
use nexus_ipc::{AuthConfig, IpcServer, IPC_TCP_FALLBACK_PORT};
use serde_json::json;
use std::{
    env,
    error::Error,
    net::Ipv4Addr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    sync::{RwLock, Semaphore},
    time,
};
use tracing::{error, info, warn};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

struct PeerStoreDiscovery(Arc<RwLock<peers::PeerStore>>);

#[async_trait]
impl DiscoveryProvider for PeerStoreDiscovery {
    async fn get_discovered_peers(&self) -> Vec<DiscoveredPeerInfo> {
        let snapshot = self.0.write().await.snapshot(Instant::now());
        snapshot
            .into_iter()
            .map(|p| DiscoveredPeerInfo {
                id: p.id,
                name: p.name,
                address: p.address,
                paired: p.paired,
            })
            .collect()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    info!("NexusLink Hub v1.0.0 starting...");

    let id = env::var("NEXUS_DEVICE_ID")
        .unwrap_or_else(|_| format!("nexus-session-{}-{}", std::process::id(), now_ms()));
    let _name =
        env::var("NEXUS_DEVICE_NAME").unwrap_or_else(|_| "Nexus Desktop (discovery only)".into());
    let peers = Arc::new(RwLock::new(peers::PeerStore::default()));

    // 1. Initialize Adapters and Application
    let kde_path = env::var("NEXUS_KDECONNECT_PATH").unwrap_or_else(|_| "kdeconnect-cli".into());
    let scrcpy_path = env::var("NEXUS_SCRCPY_PATH").unwrap_or_else(|_| "scrcpy".into());

    let kde_adapter = Arc::new(CliKdeAdapter::new(kde_path));
    let scrcpy_supervisor = Arc::new(ProcessScrcpySupervisor::new(scrcpy_path));
    let app = HubApplication::new(kde_adapter, scrcpy_supervisor)
        .with_discovery(Arc::new(PeerStoreDiscovery(peers.clone())));

    // 2. Initialize Secure Per-User IPC Server
    let auth = AuthConfig::init_per_user()?;
    info!("Initialized auth token at {}", auth.token_path.display());
    let ipc_server = Arc::new(IpcServer::new(app.clone(), auth));

    let ipc_task = {
        let server = ipc_server.clone();
        tokio::spawn(async move {
            if let Err(e) = server.run_tcp_loopback(IPC_TCP_FALLBACK_PORT).await {
                error!("IPC server error: {}", e);
            }
        })
    };

    // 3. Passive LAN discovery (receive-only; does not collide or broadcast fake identity)
    let listener = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, protocol::KDE_PORT)).await {
        Ok(udp) => {
            let _ = udp.set_broadcast(true);
            let socket = Arc::new(udp);
            let state = peers.clone();
            let own_id = id.clone();
            info!(
                "Passive LAN discovery listener active on UDP port {}",
                protocol::KDE_PORT
            );
            Some(tokio::spawn(async move {
                let mut buf = [0_u8; protocol::MAX_DATAGRAM];
                loop {
                    match socket.recv_from(&mut buf).await {
                        Ok((n, source)) => {
                            if let Some(identity) = protocol::parse_identity(&buf[..n]) {
                                if identity.device_id != own_id {
                                    state.write().await.observe(
                                        identity,
                                        source.ip(),
                                        Instant::now(),
                                    );
                                }
                            }
                        }
                        Err(e) => warn!("Discovery receive error: {e}"),
                    }
                }
            }))
        }
        Err(e) => {
            warn!(
                "Passive UDP listener could not bind to port {}: {}. Real KDE Connect daemon or another process is running; LAN discovery will defer to kdeconnect-cli.",
                protocol::KDE_PORT, e
            );
            None
        }
    };

    // 4. Insecure legacy HTTP server (disabled by default; strictly opt-in)
    let enable_http = env::var("NEXUS_ENABLE_INSECURE_HTTP")
        .map(|v| v == "1")
        .unwrap_or(false);
    let api_task = if enable_http {
        let http = TcpListener::bind((Ipv4Addr::LOCALHOST, 8787)).await?;
        warn!("INSECURE: Local unauthenticated HTTP status server enabled on 127.0.0.1:8787");
        let state = peers.clone();
        Some(tokio::spawn(async move {
            let slots = Arc::new(Semaphore::new(32));
            loop {
                let Ok((mut stream, _)) = http.accept().await else {
                    break;
                };
                let Ok(slot) = slots.clone().try_acquire_owned() else {
                    continue;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    let _slot = slot;
                    let mut buf = [0_u8; 2048];
                    let Ok(Ok(n)) =
                        time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await
                    else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    let valid = buf[..n].starts_with(b"GET /status HTTP/1.");
                    let (status, body) = if valid {
                        let snapshot = state.write().await.snapshot(Instant::now());
                        (
                            "200 OK",
                            json!({"mode":"discovery-only", "trusted":false, "peers":snapshot})
                                .to_string(),
                        )
                    } else {
                        ("404 Not Found", "{}".to_string())
                    };
                    let reply = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len());
                    let _ =
                        time::timeout(Duration::from_secs(2), stream.write_all(reply.as_bytes()))
                            .await;
                });
            }
        }))
    } else {
        info!("Unauthenticated localhost HTTP disabled. All communication flows via secure IPC.");
        None
    };

    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received. Terminating NexusLink Hub v1.0.0 gracefully...");

    // Stop active scrcpy session if running
    if let Some(sess) = app.get_active_session().await {
        if sess.state == nexus_domain::SessionState::Running {
            info!("Stopping active streaming session {}...", sess.session_id);
            let _ = tokio::time::timeout(Duration::from_secs(3), app.stop_mirror(None)).await;
        }
    }

    ipc_task.abort();
    if let Some(task) = listener {
        task.abort();
    }
    if let Some(task) = api_task {
        task.abort();
    }
    info!("NexusLink Hub v1.0.0 shutdown complete.");
    Ok(())
}
