//! Nexus Hub core daemon (v0.4 — thin composition root).
//!
//! Startup order (shutdown is strictly reverse):
//! 1. IPC session token from the OS CSPRNG — refuse to run without real entropy.
//! 2. Passive KDE-Connect-compatible discovery engine (UDP 1716) + identity broadcast.
//! 3. Process Supervisor (nexus-adapter-scrcpy) — sole owner of every child we launch.
//! 4. Authenticated, loopback-only IPC server (nexus-ipc) for the Flutter UI and CLI.
//!    The endpoint + token are published to $XDG_RUNTIME_DIR/nexus-hub/ (unix) or
//!    %LOCALAPPDATA%\nexus-hub\ (windows) with owner-only permissions, so the UI
//!    discovers the *actual* ephemeral port instead of guessing a fixed one.
//!
//! All business logic lives in nexus-application; this binary only wires things.

use nexus_adapter_kde::{parse_identity, KDE_PORT, MAX_DATAGRAM, PROTOCOL_VERSION};
use nexus_application::AppState;
use serde_json::json;
use std::{
    env,
    error::Error,
    net::Ipv4Addr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{net::UdpSocket, sync::broadcast, time};
use tracing::{info, warn};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    // 1. Token: fail closed if the OS CSPRNG is unavailable.
    let token = nexus_ipc::generate_token();
    let id = env::var("NEXUS_DEVICE_ID")
        .unwrap_or_else(|_| format!("nexus-session-{}-{}", std::process::id(), now_ms()));
    let name = env::var("NEXUS_DEVICE_NAME").unwrap_or_else(|_| "Nexus Desktop".into());

    let state = AppState::new();

    // Event bus bridging supervisor exits to any interested IPC subscriber.
    let (event_tx, mut event_rx0): (
        broadcast::Sender<serde_json::Value>,
        broadcast::Receiver<serde_json::Value>,
    ) = broadcast::channel(64);
    {
        let mut rx = state.supervisor.subscribe().expect("first subscriber");
        let tx = event_tx.clone();
        tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                let payload = json!({
                    "event": "process-exit",
                    "handle": ev.handle,
                    "kind": ev.kind.to_string(),
                    "status": ev.status,
                });
                let _ = tx.send(payload); // drop when no subscribers: bounded by design
            }
        });
    }
    // Keep the initial receiver alive so the bus has at least one listener and
    // `send` never early-fails before real subscribers attach (v0.5 will expose
    // events over IPC via subscribe semantics).
    let _ = &mut event_rx0;

    // 2. Discovery (passive, untrusted peers).
    let udp = Arc::new(UdpSocket::bind((Ipv4Addr::UNSPECIFIED, KDE_PORT)).await?);
    udp.set_broadcast(true)?;
    let discovery = {
        let socket = udp.clone();
        let peers = state.peers.clone();
        let own_id = id.clone();
        tokio::spawn(async move {
            let mut buf = [0_u8; MAX_DATAGRAM];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, source)) => {
                        if let Some(identity) = parse_identity(&buf[..n]) {
                            if identity.device_id != own_id {
                                peers
                                    .write()
                                    .await
                                    .observe(identity, source.ip(), Instant::now());
                            }
                        }
                    }
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
                let payload = format!(
                    "{}\n",
                    json!({"id": now_ms(), "type":"kdeconnect.identity", "body": {
                        "deviceId": id, "deviceName": name, "deviceType": "laptop",
                        "protocolVersion": PROTOCOL_VERSION,
                        "tcpPort": KDE_PORT,
                        "incomingCapabilities": [], "outgoingCapabilities": []
                    }})
                );
                if let Err(e) = socket
                    .send_to(payload.as_bytes(), (Ipv4Addr::BROADCAST, KDE_PORT))
                    .await
                {
                    warn!("Broadcast failed: {e}");
                }
            }
        })
    };

    // 3+4. Authenticated IPC on an ephemeral loopback port, credentials published.
    let handler = nexus_application::build_handler(state.clone());
    let server = nexus_ipc::IpcServer::bind(token.clone(), handler).await?;
    let addr = server.local_addr()?;
    nexus_ipc::write_credentials(&token, addr.port())?;
    info!("Authenticated IPC listening on {addr} (loopback only); credentials published");
    let ipc_task = tokio::spawn(async move { server.run().await });

    // Orderly shutdown: fatal IPC outage OR ctrl-c both take the same path.
    tokio::select! {
        res = ipc_task => {
            if let Err(e) = res {
                warn!("IPC accept loop failed: {e}; shutting down");
            }
        }
        _ = tokio::signal::ctrl_c() => {}
    }
    info!("Shutdown requested: stopping in reverse startup order");
    state.supervisor.stop_all();
    broadcaster.abort();
    discovery.abort();
    nexus_ipc::remove_credentials();
    Ok(())
}
