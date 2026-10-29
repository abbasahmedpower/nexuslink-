//! nexus-application — use cases and orchestration.
//!
//! This crate owns the command handler that the IPC transport invokes. It is the
//! only place that wires adapters together, which keeps adapters independently
//! testable and the UI transport-agnostic (hexagonal: IPC is a driving adapter,
//! scrcpy/kde are driven adapters).

use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use nexus_adapter_kde::PeerStore;
use nexus_adapter_scrcpy::{ManagedKind, Supervisor, DEFAULT_KILL_GRACE};
use nexus_domain::{parse_command, Command, NexusError, ProcessInfo, IPC_VERSION};
use nexus_ipc::{Request, Response};
use serde_json::json;
use tokio::{sync::Mutex, time};

pub struct AppState {
    pub supervisor: Arc<Supervisor>,
    pub peers: Arc<tokio::sync::RwLock<PeerStore>>,
    /// Serialize stream starts so two racing clients cannot spawn two mirrors.
    pub start_lock: Mutex<()>,
    /// Our advertised KDE Connect device id (used in pair/packet envelopes).
    pub device_id: String,
}

impl AppState {
    pub fn new() -> Arc<Self> {
        Self::with_device_id(format!("nexus-{}", std::process::id()))
    }
    pub fn with_device_id(id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            supervisor: Supervisor::new(),
            peers: Arc::new(tokio::sync::RwLock::new(PeerStore::default())),
            start_lock: Mutex::new(()),
            device_id: id.into(),
        })
    }
}

/// Build the boxed handler the IPC server dispatches into.
pub fn build_handler(state: Arc<AppState>) -> nexus_ipc::Handler {
    Arc::new(move |req: Request| {
        let state = state.clone();
        Box::pin(async move { handle(state, req).await })
    })
}

fn fail(seq: u64, e: &NexusError) -> Response {
    Response::failure(seq, e.code())
}

/// Connect to a discovered peer's TCP session port with a strict deadline,
/// send one fixed-shape packet, and await the echoed response type. The whole
/// exchange is bounded in TIME (connect/read/write deadlines), SIZE (packet cap),
/// and never trusts content beyond the packet `type` field.
async fn session_exchange(
    addr: std::net::SocketAddr,
    packet: String,
    expect_type: &str,
) -> Result<(), NexusError> {
    use nexus_adapter_kde::{packet_type, read_packet_bounded, MAX_DATAGRAM};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let stream = time::timeout(Duration::from_secs(3), tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| NexusError::NotFound("connect-timeout".into()))?
        .map_err(|_| NexusError::NotFound("unreachable".into()))?;
    let (mut rd, mut wr) = stream.into_split();
    let mut buf_reader = tokio::io::BufReader::new(&mut rd);
    let write = async {
        wr.write_all(packet.as_bytes()).await?;
        wr.write_all(b"\n").await
    };
    time::timeout(Duration::from_secs(3), write)
        .await
        .map_err(|_| NexusError::Internal)?
        .map_err(|_| NexusError::Internal)?;
    // One bounded read with deadline; anything else is a protocol failure.
    let raw = time::timeout(Duration::from_secs(5), read_packet_bounded(&mut buf_reader, MAX_DATAGRAM))
        .await
        .map_err(|_| NexusError::NotFound("response-timeout".into()))?
        .map_err(|_| NexusError::InvalidInput)?;
    match raw {
        Some(bytes) if packet_type(&bytes).as_deref() == Some(expect_type) => Ok(()),
        Some(_) => Err(NexusError::InvalidInput),
        None => Err(NexusError::NotFound("closed".into())),
    }
}

/// Look up a validated device id in the peer store; returns (addr, paired).
async fn peer_addr(state: &AppState, device_id: &str) -> Result<std::net::SocketAddr, NexusError> {
    if !nexus_domain::valid_device_id(device_id) {
        return Err(NexusError::InvalidDeviceId);
    }
    let now = Instant::now();
    let peers = state.peers.read().await.snapshot(now);
    let peer = peers.iter().find(|p| p.id == device_id).ok_or(NexusError::NotFound(device_id.into()))?;
    let ip: std::net::IpAddr = peer.address.parse().map_err(|_| NexusError::Internal)?;
    Ok(std::net::SocketAddr::new(ip, nexus_adapter_kde::KDE_PORT))
}

async fn handle(state: Arc<AppState>, req: Request) -> Response {
    let seq = req.seq;
    let envelope = json!({ "method": req.method, "params": req.params });
    let cmd = match parse_command(&envelope) {
        Ok(c) => c,
        Err(e) => return fail(seq, &e),
    };
    match cmd {
        Command::Ping => Response::success(seq, json!({ "pong": true, "version": IPC_VERSION })),
        Command::DevicesList => {
            let snapshot = state.peers.write().await.snapshot(Instant::now());
            Response::success(seq, json!({ "devices": snapshot }))
        }
        Command::ProcessesList => {
            let list: Vec<_> = state
                .supervisor
                .list()
                .into_iter()
                .map(|(handle, kind)| ProcessInfo { handle, kind: kind.to_string(), running: true })
                .collect();
            Response::success(seq, json!({ "processes": list }))
        }
        Command::DevicePair { device_id } => {
            match peer_addr(&state, &device_id).await {
                Ok(addr) => {
                    let pkt = nexus_adapter_kde::pair_packet(1, &state.device_id);
                    match session_exchange(addr, pkt, "kdeconnect.pair").await {
                        Ok(()) => {
                            state.peers.write().await.mark_paired(&device_id);
                            Response::success(seq, json!({ "paired": true }))
                        }
                        Err(e) => fail(seq, &e),
                    }
                }
                Err(e) => fail(seq, &e),
            }
        }
        Command::DevicePing { device_id } => {
            match peer_addr(&state, &device_id).await {
                Ok(addr) => match session_exchange(addr, nexus_adapter_kde::ping_packet(1), "kdeconnect.ping").await {
                    Ok(()) => Response::success(seq, json!({ "reachable": true })),
                    Err(e) => fail(seq, &e),
                },
                Err(e) => fail(seq, &e),
            }
        }
        Command::DeviceSendFile { device_id, uri } => {
            // v0.6 scope: validate identity + shape only; actual transfer needs TLS sessions.
            if device_id.is_empty() || uri.trim().is_empty() || uri.len() > 4096 {
                return fail(seq, &NexusError::InvalidInput);
            }
            match peer_addr(&state, &device_id).await {
                Ok(_) => Response::success(seq, json!({ "queued": true, "note": "transfer requires v0.6 TLS session" })),
                Err(e) => fail(seq, &e),
            }
        }
        Command::MirrorStart { serial } => match spawn_stream(&state, ManagedKind::ScrcpyMirror, serial, false).await {
            Ok(handle) => Response::success(seq, json!({ "handle": handle })),
            Err(e) => fail(seq, &e),
        },
        Command::CameraStart { serial } => match spawn_stream(&state, ManagedKind::ScrcpyCamera, serial, true).await {
            Ok(handle) => Response::success(seq, json!({ "handle": handle })),
            Err(e) => fail(seq, &e),
        },
        Command::ProcessStop { handle } => {
            // stop() is race-safe by construction (channel-based termination);
            // additionally bound it so a wedged reaper can never wedge the handler.
            let confirmed = time::timeout(
                DEFAULT_KILL_GRACE + Duration::from_secs(2),
                state.supervisor.stop(handle, DEFAULT_KILL_GRACE),
            )
            .await
            .unwrap_or(false);
            Response::success(seq, json!({ "stopped": confirmed }))
        }
    }
}

/// Launch scrcpy as a supervised child. Never through a shell; fixed validated
/// argv; at most one stream per kind concurrently (checked under the start lock).
async fn spawn_stream(
    state: &AppState,
    kind: ManagedKind,
    serial: Option<String>,
    camera: bool,
) -> Result<u64, NexusError> {
    // Validation firewall FIRST: malformed input must yield its typed error
    // regardless of whether the upstream tool is installed. Ordering here is a
    // security-relevant invariant covered by `mirror_start_rejects_bad_serial`.
    if let Some(ref s) = serial {
        if !nexus_domain::valid_device_id(s) {
            return Err(NexusError::InvalidDeviceId);
        }
    }
    // Hold the start lock across the duplicate check AND the spawn so racing
    // clients serialize instead of double-launching (micro-level invariant).
    let _guard = state.start_lock.lock().await;
    if state.supervisor.is_running(kind) {
        return Err(NexusError::AlreadyRunning(kind.to_string()));
    }
    // Pre-flight AFTER validation: refuse with a typed error when the upstream
    // binary is absent instead of surfacing an opaque spawn OS error later.
    // PATH lookup without spawning a shell (no injection surface).
    if !binary_on_path("scrcpy") {
        return Err(NexusError::ToolUnavailable("scrcpy".into()));
    }
    let mut cmd = tokio::process::Command::new("scrcpy");
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
    let child = cmd.spawn().map_err(|e| NexusError::SpawnFailed(e.kind().to_string()))?;
    Ok(state.supervisor.adopt(kind, child))
}

/// Locate an executable on PATH without spawning a shell (injection-free preflight).
fn binary_on_path(name: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else { return false };
    let exe_names: &[&str] = if cfg!(windows) {
        &[name, &format!("{name}.exe")]
    } else {
        &[name]
    };
    for dir in std::env::split_paths(&path) {
        for n in exe_names {
            let candidate = dir.join(n);
            if candidate.is_file() {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_ipc::IpcClient;

    async fn serve(state: Arc<AppState>) -> std::net::SocketAddr {
        let token = "test-token";
        let server = nexus_ipc::IpcServer::bind(token.to_string(), build_handler(state)).await.unwrap();
        let addr = server.local_addr().unwrap();
        tokio::spawn(async move { server.run().await });
        addr
    }

    #[tokio::test]
    async fn ping_over_real_ipc() {
        let state = AppState::new();
        let addr = serve(state).await;
        let client = IpcClient::connect(addr, "test-token").await.unwrap();
        let resp = client.call("ping", serde_json::Value::Null, 1).await.unwrap();
        assert!(resp.ok);
        assert_eq!(resp.data.unwrap()["pong"], true);
    }

    #[tokio::test]
    async fn devices_list_is_empty_initially() {
        let state = AppState::new();
        let addr = serve(state).await;
        let client = IpcClient::connect(addr, "test-token").await.unwrap();
        let resp = client.call("devices_list", serde_json::Value::Null, 2).await.unwrap();
        assert!(resp.ok);
        assert_eq!(resp.data.unwrap()["devices"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn mirror_start_rejects_bad_serial() {
        let state = AppState::new();
        let addr = serve(state).await;
        let client = IpcClient::connect(addr, "test-token").await.unwrap();
        let resp = client
            .call("mirror_start", json!({ "serial": "--wrap=evil" }), 3)
            .await
            .unwrap();
        assert!(!resp.ok);
        assert_eq!(resp.error.as_deref(), Some("invalid-device-id"));
    }

    #[tokio::test]
    async fn unknown_command_is_invalid_input() {
        let state = AppState::new();
        let addr = serve(state).await;
        let client = IpcClient::connect(addr, "test-token").await.unwrap();
        let resp = client.call("rm_rf", serde_json::Value::Null, 4).await.unwrap();
        assert!(!resp.ok);
        assert_eq!(resp.error.as_deref(), Some("invalid-input"));
    }

    #[tokio::test]
    async fn stop_unknown_handle_reports_stopped_true() {
        let state = AppState::new();
        let addr = serve(state).await;
        let client = IpcClient::connect(addr, "test-token").await.unwrap();
        let resp = client.call("process_stop", json!({ "handle": 9999 }), 5).await.unwrap();
        assert!(resp.ok);
        assert_eq!(resp.data.unwrap()["stopped"], true);
    }
}
