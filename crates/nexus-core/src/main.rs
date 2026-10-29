//! Nexus Hub core daemon (v0.4).
//!
//! Startup order (shutdown is strictly reverse):
//! 1. IPC session token from the OS CSPRNG — refuse to run without real entropy.
//! 2. Passive KDE-Connect-compatible discovery engine (UDP 1716) + identity broadcast.
//! 3. Process Supervisor — sole owner of every child we launch (scrcpy, kde-cli).
//! 4. Authenticated, loopback-only IPC server for the Flutter UI and CLI.
//!
//! The handler is fully async: no `block_on` inside worker threads, no lock held
//! across an await that could depend on the same lock. Every external input is
//! validated before it reaches argv or the peer store.

mod commands;
mod ipc;
mod peers;
mod protocol;
mod supervisor;

use std::{
    env,
    error::Error,
    net::Ipv4Addr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use serde_json::json;
use supervisor::ManagedKind;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    process::Command as TokioCommand,
    sync::{Mutex, Semaphore},
    time,
};
use tracing::{info, warn};

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// Validate a device id / ADB serial before it ever reaches an argv array.
/// Injection firewall: no shell is used anywhere, but option-smuggling via
/// crafted ids must also be impossible at the upstream-tool boundary.
pub fn valid_device_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

struct DaemonState {
    supervisor: Arc<supervisor::Supervisor>,
    peers: Arc<tokio::sync::RwLock<peers::PeerStore>>,
    /// Serialize stream starts so two racing clients cannot spawn two mirrors.
    start_lock: Mutex<()>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    let token = ipc::generate_token();
    let id = env::var("NEXUS_DEVICE_ID")
        .unwrap_or_else(|_| format!("nexus-session-{}-{}", std::process::id(), now_ms()));
    let name = env::var("NEXUS_DEVICE_NAME").unwrap_or_else(|_| "Nexus Desktop".into());

    let state = Arc::new(DaemonState {
        supervisor: supervisor::Supervisor::new(),
        peers: Arc::new(tokio::sync::RwLock::new(peers::PeerStore::default())),
        start_lock: Mutex::new(()),
    });

    // --- Discovery ---
    let udp = Arc::new(UdpSocket::bind((Ipv4Addr::UNSPECIFIED, protocol::KDE_PORT)).await?);
    udp.set_broadcast(true)?;
    let discovery = {
        let socket = udp.clone();
        let peers = state.peers.clone();
        let own_id = id.clone();
        tokio::spawn(async move {
            let mut buf = [0_u8; protocol::MAX_DATAGRAM];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, source)) => {
                        if let Some(identity) = protocol::parse_identity(&buf[..n]) {
                            if identity.device_id != own_id {
                                peers.write().await.observe(identity, source.ip(), Instant::now());
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
                        "protocolVersion": protocol::PROTOCOL_VERSION,
                        "tcpPort": protocol::KDE_PORT,
                        "incomingCapabilities": [], "outgoingCapabilities": []
                    }})
                );
                if let Err(e) =
                    socket.send_to(payload.as_bytes(), (Ipv4Addr::BROADCAST, protocol::KDE_PORT)).await
                {
                    warn!("Broadcast failed: {e}");
                }
            }
        })
    };

    // --- Legacy read-only status endpoint (kept for v0.3 compatibility) ---
    let http = TcpListener::bind((Ipv4Addr::LOCALHOST, 8787)).await?;
    let status_api = {
        let peers = state.peers.clone();
        tokio::spawn(async move {
            let slots = Arc::new(Semaphore::new(32));
            loop {
                let Ok((mut stream, _)) = http.accept().await else { break };
                let Ok(slot) = slots.clone().try_acquire_owned() else { continue };
                let peers = peers.clone();
                tokio::spawn(async move {
                    let _slot = slot;
                    let mut buf = [0_u8; 2048];
                    let Ok(Ok(n)) = time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await
                    else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    let valid = buf[..n].starts_with(b"GET /status HTTP/1.");
                    let (status, body) = if valid {
                        let snapshot = peers.write().await.snapshot(Instant::now());
                        (
                            "200 OK",
                            json!({"mode": "discovery-only", "trusted": false, "peers": snapshot})
                                .to_string(),
                        )
                    } else {
                        ("404 Not Found", "{}".to_string())
                    };
                    let reply = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let write = async {
                        let _ = stream.write_all(reply.as_bytes()).await;
                        let _ = stream.flush().await;
                    };
                    let _ = time::timeout(Duration::from_secs(2), write).await;
                });
            }
        })
    };

    // --- Authenticated IPC (v0.4) ---
    let handler: ipc::Handler = {
        let state = state.clone();
        Arc::new(move |req: ipc::Request| {
            let state = state.clone();
            Box::pin(async move { handle_command(state, req).await })
        })
    };
    let server = ipc::IpcServer::bind(token, handler).await?;
    let addr = server.local_addr()?;
    info!("Authenticated IPC listening on {addr} (loopback only)");
    let ipc_task = tokio::spawn(async move { server.run().await });

    info!("Passive discovery on UDP {}; status API on 127.0.0.1:8787", protocol::KDE_PORT);
    // Fatal IPC outage OR ctrl-c both lead to the same orderly shutdown.
    tokio::select! {
        res = &mut ipc_task => {
            if let Err(e) = res {
                warn!("IPC accept loop failed: {e}; shutting down");
            }
        }
        _ = tokio::signal::ctrl_c() => {}
    }
    info!("Shutdown requested: stopping in reverse startup order");
    ipc_task.abort();
    state.supervisor.stop_all();
    broadcaster.abort();
    discovery.abort();
    status_api.abort();
    Ok(())
}

async fn handle_command(state: Arc<DaemonState>, req: ipc::Request) -> ipc::Response {
    let seq = req.seq;
    let envelope = json!({ "method": req.method, "params": req.params });
    let cmd = match commands::parse_command(&envelope) {
        Ok(c) => c,
        Err(e) => return ipc::Response::failure(seq, e),
    };
    match cmd {
        commands::Command::Ping => {
            ipc::Response::success(seq, json!({"pong": true, "version": commands::IPC_VERSION}))
        }
        commands::Command::DevicesList => {
            let snapshot = state.peers.write().await.snapshot(Instant::now());
            ipc::Response::success(seq, json!({"devices": snapshot}))
        }
        commands::Command::ProcessesList => {
            let list: Vec<_> = state
                .supervisor
                .list()
                .into_iter()
                .map(|(handle, kind)| commands::ProcessInfo {
                    handle,
                    kind: kind.to_string(),
                    running: true,
                })
                .collect();
            ipc::Response::success(seq, json!({"processes": list}))
        }
        commands::Command::MirrorStart { serial } => {
            match spawn_stream(&state, ManagedKind::ScrcpyMirror, serial, false).await {
                Ok(handle) => ipc::Response::success(seq, json!({"handle": handle})),
                Err(e) => ipc::Response::failure(seq, e),
            }
        }
        commands::Command::CameraStart { serial } => {
            match spawn_stream(&state, ManagedKind::ScrcpyCamera, serial, true).await {
                Ok(handle) => ipc::Response::success(seq, json!({"handle": handle})),
                Err(e) => ipc::Response::failure(seq, e),
            }
        }
        commands::Command::ProcessStop { handle } => {
            let confirmed =
                state.supervisor.stop(handle, supervisor::DEFAULT_KILL_GRACE).await;
            ipc::Response::success(seq, json!({"stopped": confirmed}))
        }
    }
}

/// Launch scrcpy as a supervised child. Never through a shell; fixed validated
/// argv; at most one stream per kind concurrently (checked under the start lock).
async fn spawn_stream(
    state: &DaemonState,
    kind: ManagedKind,
    serial: Option<String>,
    camera: bool,
) -> Result<u64, String> {
    if let Some(ref s) = serial {
        if !valid_device_id(s) {
            return Err("invalid-device-id".into());
        }
    }
    // Hold the start lock across the duplicate check AND the spawn so racing
    // clients serialize instead of double-launching (micro-level invariant).
    let _guard = state.start_lock.lock().await;
    if state.supervisor.list().iter().any(|(_, k)| *k == kind) {
        return Err(format!("already-running:{kind}"));
    }
    let mut cmd = TokioCommand::new("scrcpy");
    cmd.arg("--max-fps=60");
    if camera {
        cmd.arg("--video-source=camera");
    }
    if let Some(s) = serial.filter(|x| !x.is_empty()) {
        cmd.args(["--serial", &s]);
    }
    // No shell. Fixed argv. kill_on_drop is belt-and-suspenders next to the supervisor.
    cmd.kill_on_drop(true);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());
    let child = cmd.spawn().map_err(|e| format!("spawn-failed:{e}"))?;
    Ok(state.supervisor.adopt(kind, child))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_id_firewall() {
        assert!(valid_device_id("emulator-5554"));
        assert!(valid_device_id("RFC1Y7WZ9HMLTVL9"));
        assert!(valid_device_id("a.b:c-d_1"));
        assert!(!valid_device_id(""));
        assert!(!valid_device_id("--wrap=start-server-under-cover"));
        assert!(!valid_device_id("id with spaces"));
        assert!(!valid_device_id("semi;colon"));
        assert!(!valid_device_id(&"x".repeat(129)));
    }
}
