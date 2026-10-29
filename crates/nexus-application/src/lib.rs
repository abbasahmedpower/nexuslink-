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
}

impl AppState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            supervisor: Supervisor::new(),
            peers: Arc::new(tokio::sync::RwLock::new(PeerStore::default())),
            start_lock: Mutex::new(()),
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
    let mut cmd = tokio::process::Command::new("scrcpy");
    // Pre-flight: refuse with a typed error when the upstream binary is absent,
    // instead of surfacing an opaque spawn OS error later. `Cmd`-style lookup via
    // PATH without spawning a shell (no injection surface).
    if !binary_on_path("scrcpy") {
        return Err(NexusError::ToolUnavailable("scrcpy".into()));
    }
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
