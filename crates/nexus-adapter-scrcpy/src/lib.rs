use async_trait::async_trait;
pub use nexus_domain::SessionEvent;
use nexus_domain::{
    is_valid_device_id, DomainError, ErrorCode, SessionState, StreamKind, StreamSession,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{broadcast, Mutex, Notify};
use tracing::{info, warn};

/// Maximum bytes we'll buffer from child stdout/stderr to prevent memory exhaustion.
const MAX_CHILD_OUTPUT: usize = 65_536; // 64 KiB

#[async_trait]
pub trait ScrcpySupervisor: Send + Sync {
    async fn start_mirror(
        &self,
        serial: Option<String>,
        camera: bool,
        record: Option<String>,
    ) -> Result<StreamSession, DomainError>;
    async fn stop_mirror(&self, session_id: Option<String>) -> Result<StreamSession, DomainError>;
    async fn get_active_session(&self) -> Option<StreamSession>;
    async fn check_available(&self) -> bool;
    fn subscribe(&self) -> broadcast::Receiver<SessionEvent>;
}

/// Session metadata stored under the session mutex — never contains the Child handle.
struct SessionInfo {
    session: StreamSession,
    /// Notify used to signal the monitor task to kill the child and shut down.
    kill_signal: Arc<Notify>,
    /// Notify used by the monitor task to broadcast that termination and state recording is complete.
    completed: Arc<Notify>,
}

/// Maximum number of completed sessions kept in history to prevent unbounded memory growth.
const MAX_HISTORY: usize = 1_000;

pub struct ProcessScrcpySupervisor {
    executable: PathBuf,
    /// Holds only session metadata + signals. Child handle is owned by the monitor task.
    active: Arc<Mutex<Option<SessionInfo>>>,
    history: Arc<Mutex<HashMap<String, StreamSession>>>,
    events: broadcast::Sender<SessionEvent>,
    counter: AtomicU64,
    /// Daemon start timestamp in milliseconds (used to make session IDs unique across restarts).
    daemon_start_ms: u64,
}

impl ProcessScrcpySupervisor {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        let (tx, _) = broadcast::channel(128);
        let daemon_start_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            executable: executable.into(),
            active: Arc::new(Mutex::new(None)),
            history: Arc::new(Mutex::new(HashMap::new())),
            events: tx,
            counter: AtomicU64::new(1),
            daemon_start_ms,
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[async_trait]
impl ScrcpySupervisor for ProcessScrcpySupervisor {
    async fn check_available(&self) -> bool {
        let mut cmd = Command::new(&self.executable);
        cmd.arg("--version")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match tokio::time::timeout(Duration::from_secs(3), cmd.status()).await {
            Ok(Ok(status)) => status.success(),
            _ => false,
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<SessionEvent> {
        self.events.subscribe()
    }

    async fn get_active_session(&self) -> Option<StreamSession> {
        let lock = self.active.lock().await;
        lock.as_ref().map(|m| m.session.clone())
    }

    async fn start_mirror(
        &self,
        serial: Option<String>,
        camera: bool,
        record: Option<String>,
    ) -> Result<StreamSession, DomainError> {
        let mut lock = self.active.lock().await;

        if let Some(ref m) = *lock {
            return Err(DomainError::with_detail(
                ErrorCode::SessionAlreadyRunning,
                "A streaming session is already active",
                format!("Active session ID: {}", m.session.session_id),
            ));
        }

        if let Some(ref s) = serial {
            if !s.is_empty() && !is_valid_device_id(s) {
                return Err(DomainError::new(
                    ErrorCode::InvalidDeviceId,
                    "Invalid ADB serial",
                ));
            }
        }

        // Prefix with daemon start timestamp to ensure uniqueness across daemon restarts.
        let session_id = format!(
            "scrcpy-{}-{}",
            self.daemon_start_ms,
            self.counter.fetch_add(1, Ordering::SeqCst)
        );
        let kind = if camera {
            StreamKind::Camera
        } else {
            StreamKind::Screen
        };

        let mut cmd = Command::new(&self.executable);
        // Build strictly validated args without shell interpolation
        if let Some(ref s) = serial {
            if !s.is_empty() {
                cmd.args(["--serial", s]);
            }
        }
        if camera {
            cmd.arg("--video-source=camera");
        }
        if let Some(ref rec) = record {
            let rec = rec.trim();
            if !rec.is_empty() {
                cmd.args(["--record", rec]);
            }
        }
        cmd.arg("--max-fps=60");

        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                return Err(DomainError::with_detail(
                    ErrorCode::ToolNotFound,
                    format!("Failed to start scrcpy: {}", e),
                    e.to_string(),
                ));
            }
        };

        let pid = child.id();
        info!("Started scrcpy child pid: {:?}", pid);

        // Take ownership of pipes BEFORE child is moved into monitor task
        let stdout_pipe = child.stdout.take().unwrap();
        let stderr_pipe = child.stderr.take().unwrap();

        let session = StreamSession {
            session_id: session_id.clone(),
            kind,
            device_serial: serial,
            state: SessionState::Running,
            started_at_ms: Self::now_ms(),
            pid,
            exit_code: None,
            last_error: None,
        };

        let kill_signal = Arc::new(Notify::new());
        let completed = Arc::new(Notify::new());

        *lock = Some(SessionInfo {
            session: session.clone(),
            kill_signal: kill_signal.clone(),
            completed: completed.clone(),
        });
        drop(lock); // Release lock BEFORE spawning monitor task

        let _ = self.events.send(SessionEvent::Started(session.clone()));

        // Use a oneshot channel to report early exit back to the caller
        let (early_tx, early_rx) = tokio::sync::oneshot::channel::<Option<DomainError>>();

        // Background monitor for child exit and log draining.
        // CRITICAL: The child handle lives ONLY in this task. No mutex is held during child.wait().
        let active_clone = self.active.clone();
        let history_clone = self.history.clone();
        let events_clone = self.events.clone();
        let sess_id_clone = session_id.clone();
        let completed_clone = completed.clone();
        let session_snapshot = session.clone();

        tokio::spawn(async move {
            // Sole owner of child process
            let mut child = child;

            // Bounded output capture buffers
            let mut out_buf = Vec::new();
            let mut err_buf = Vec::new();

            // Drain stdout/stderr with bounded memory while waiting for child or kill signal
            let monitor_result = tokio::select! {
                result = async {
                    let drain = async {
                        let mut stdout_limited = stdout_pipe.take(MAX_CHILD_OUTPUT as u64);
                        let mut stderr_limited = stderr_pipe.take(MAX_CHILD_OUTPUT as u64);
                        let _ = tokio::join!(
                            stdout_limited.read_to_end(&mut out_buf),
                            stderr_limited.read_to_end(&mut err_buf)
                        );
                    };
                    let wait = child.wait();
                    let (_, wait_result) = tokio::join!(drain, wait);
                    wait_result
                } => {
                    result
                }
                _ = kill_signal.notified() => {
                    info!("Kill signal received for session {}", sess_id_clone);
                    if let Err(e) = child.kill().await {
                        warn!("Failed to kill scrcpy child: {}", e);
                    }
                    match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
                        Ok(result) => result,
                        Err(_) => {
                            warn!("scrcpy child did not exit within 3s after kill for session {}", sess_id_clone);
                            Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "Child did not exit after kill signal",
                            ))
                        }
                    }
                }
            };

            let (exit_code, err_msg) = match monitor_result {
                Ok(status) => {
                    info!("scrcpy session {} finished with: {}", sess_id_clone, status);
                    let code = status.code();
                    let err = if !status.success() {
                        let e_str = String::from_utf8_lossy(&err_buf).trim().to_string();
                        if e_str.is_empty() {
                            Some(format!("Exited with code: {:?}", code))
                        } else {
                            Some(e_str)
                        }
                    } else {
                        None
                    };
                    (code, err)
                }
                Err(e) => {
                    warn!("scrcpy wait error: {}", e);
                    (None, Some(e.to_string()))
                }
            };

            // Calculate final state
            let final_state = if exit_code == Some(0) {
                SessionState::Stopped
            } else {
                SessionState::Failed
            };

            // Atomically retrieve and clear active session handle
            let mut guard = active_clone.lock().await;
            let mut final_session = if let Some(ref current) = *guard {
                if current.session.session_id == sess_id_clone {
                    let s = current.session.clone();
                    *guard = None;
                    s
                } else {
                    session_snapshot
                }
            } else {
                session_snapshot
            };
            drop(guard); // Release active lock immediately

            final_session.state = final_state;
            final_session.exit_code = exit_code;
            final_session.last_error = err_msg.clone();

            // Save to history with bounded size (MAX_HISTORY) — monitor task is the single source of truth for session completion
            let mut hist = history_clone.lock().await;
            if hist.len() >= MAX_HISTORY {
                // Prune oldest entry by earliest started_at_ms (O(n) but history cap keeps it fast)
                if let Some(oldest_key) = hist
                    .iter()
                    .min_by_key(|(_, s)| s.started_at_ms)
                    .map(|(k, _)| k.clone())
                {
                    hist.remove(&oldest_key);
                }
            }
            hist.insert(sess_id_clone.clone(), final_session.clone());
            drop(hist);

            // Notify all waiters in stop_mirror that state recording is complete
            completed_clone.notify_waiters();

            // Send broadcast events
            if let Some(err) = err_msg {
                let _ = events_clone.send(SessionEvent::Failed {
                    session_id: sess_id_clone.clone(),
                    error: err.clone(),
                });
                let _ = early_tx.send(Some(DomainError::with_detail(
                    ErrorCode::ToolExecutionFailed,
                    "scrcpy failed immediately on startup",
                    err,
                )));
            } else {
                let _ = events_clone.send(SessionEvent::Exited {
                    session_id: sess_id_clone,
                    exit_code,
                });
                let _ = early_tx.send(None);
            }
        });

        // Wait briefly for early exit detection
        if let Ok(Ok(Some(err))) = tokio::time::timeout(Duration::from_millis(200), early_rx).await
        {
            return Err(err);
        }

        Ok(session)
    }

    async fn stop_mirror(&self, session_id: Option<String>) -> Result<StreamSession, DomainError> {
        let (kill_signal, completed, sid) = {
            let lock = self.active.lock().await;
            match *lock {
                Some(ref info) => {
                    if let Some(ref req_sid) = session_id {
                        if info.session.session_id != *req_sid {
                            // Check if already in history (idempotency for previously stopped session)
                            let hist = self.history.lock().await;
                            if let Some(s) = hist.get(req_sid) {
                                return Ok(s.clone());
                            }
                            return Err(DomainError::with_detail(
                                ErrorCode::SessionNotFound,
                                "Session ID does not match active stream",
                                format!(
                                    "Requested: {}, Active: {}",
                                    req_sid, info.session.session_id
                                ),
                            ));
                        }
                    }
                    (
                        info.kill_signal.clone(),
                        info.completed.clone(),
                        info.session.session_id.clone(),
                    )
                }
                None => {
                    // Check if already stopped and recorded in history (idempotent stop)
                    if let Some(ref req_sid) = session_id {
                        let hist = self.history.lock().await;
                        if let Some(s) = hist.get(req_sid) {
                            return Ok(s.clone());
                        }
                    }
                    return Err(DomainError::new(
                        ErrorCode::SessionNotFound,
                        "No active stream owned by Nexus.",
                    ));
                }
            }
        }; // Active lock is released immediately here!

        // Signal monitor task to terminate child
        kill_signal.notify_one();

        // Await confirmation from the monitor task (up to 4s safeguard deadline)
        match tokio::time::timeout(Duration::from_secs(4), completed.notified()).await {
            Ok(()) => {
                let hist = self.history.lock().await;
                if let Some(s) = hist.get(&sid) {
                    return Ok(s.clone());
                }
            }
            Err(_) => {
                warn!(
                    "Stop requested but termination confirmation timed out for {}",
                    sid
                );
            }
        }

        // Safeguard fallback: check history once more
        let mut hist = self.history.lock().await;
        if let Some(s) = hist.get(&sid) {
            return Ok(s.clone());
        }

        let fallback = StreamSession {
            session_id: sid.clone(),
            kind: StreamKind::Screen,
            device_serial: None,
            state: SessionState::Stopped,
            started_at_ms: Self::now_ms(),
            pid: None,
            exit_code: None,
            last_error: Some("Termination confirmation timed out".to_string()),
        };
        hist.insert(sid, fallback.clone());
        Ok(fallback)
    }
}
