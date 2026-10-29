use async_trait::async_trait;
use nexus_domain::{
    is_valid_device_id, DomainError, ErrorCode, SessionState, StreamKind, StreamSession,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, Mutex};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub enum SessionEvent {
    Started(StreamSession),
    Exited {
        session_id: String,
        exit_code: Option<i32>,
    },
    Failed {
        session_id: String,
        error: String,
    },
}

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

struct ManagedChild {
    session: StreamSession,
    child: Child,
    #[allow(dead_code)]
    birth_time: Instant,
}

pub struct ProcessScrcpySupervisor {
    executable: PathBuf,
    active: Arc<Mutex<Option<ManagedChild>>>,
    history: Arc<Mutex<HashMap<String, StreamSession>>>,
    events: broadcast::Sender<SessionEvent>,
    counter: AtomicU64,
}

impl ProcessScrcpySupervisor {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        let (tx, _) = broadcast::channel(128);
        Self {
            executable: executable.into(),
            active: Arc::new(Mutex::new(None)),
            history: Arc::new(Mutex::new(HashMap::new())),
            events: tx,
            counter: AtomicU64::new(1),
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

        let session_id = format!("scrcpy-{}", self.counter.fetch_add(1, Ordering::SeqCst));
        let kind = if camera {
            StreamKind::Camera
        } else {
            StreamKind::Screen
        };

        let mut cmd = Command::new(&self.executable);

        // Build strictly validated arguments (no shell)
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

        cmd.arg("--max-fps=60")
            .stdin(Stdio::null())
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

        let mut stdout_pipe = child.stdout.take().unwrap();
        let mut stderr_pipe = child.stderr.take().unwrap();

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

        *lock = Some(ManagedChild {
            session: session.clone(),
            child,
            birth_time: Instant::now(),
        });
        drop(lock);

        let _ = self.events.send(SessionEvent::Started(session.clone()));

        // Background monitor for exit + log draining
        let active_clone = self.active.clone();
        let history_clone = self.history.clone();
        let events_clone = self.events.clone();
        let sess_id_clone = session_id.clone();

        tokio::spawn(async move {
            let mut out_tail = Vec::new();
            let mut err_tail = Vec::new();

            let drain_fut = async {
                let _ = tokio::join!(
                    stdout_pipe.read_to_end(&mut out_tail),
                    stderr_pipe.read_to_end(&mut err_tail)
                );
            };

            let wait_fut = async {
                let mut guard = active_clone.lock().await;
                if let Some(ref mut m) = *guard {
                    if m.session.session_id == sess_id_clone {
                        return m.child.wait().await;
                    }
                }
                Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Session replaced",
                ))
            };

            let (_drain_res, wait_res) = tokio::join!(drain_fut, wait_fut);

            let (exit_code, err_msg) = match wait_res {
                Ok(status) => {
                    info!("scrcpy session {} finished with: {}", sess_id_clone, status);
                    let code = status.code();
                    let err = if !status.success() {
                        let e_str = String::from_utf8_lossy(&err_tail).trim().to_string();
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

            let mut guard = active_clone.lock().await;
            if let Some(ref m) = *guard {
                if m.session.session_id == sess_id_clone {
                    let mut final_session = m.session.clone();
                    final_session.state = if exit_code == Some(0) {
                        SessionState::Stopped
                    } else {
                        SessionState::Failed
                    };
                    final_session.exit_code = exit_code;
                    final_session.last_error = err_msg.clone();

                    history_clone
                        .lock()
                        .await
                        .insert(sess_id_clone.clone(), final_session.clone());

                    *guard = None;

                    if let Some(err) = err_msg {
                        let _ = events_clone.send(SessionEvent::Failed {
                            session_id: sess_id_clone,
                            error: err,
                        });
                    } else {
                        let _ = events_clone.send(SessionEvent::Exited {
                            session_id: sess_id_clone,
                            exit_code,
                        });
                    }
                }
            }
        });

        // Quick failure detection
        tokio::time::sleep(Duration::from_millis(150)).await;
        {
            let lock = self.active.lock().await;
            if lock.is_none() {
                let hist = self.history.lock().await;
                if let Some(s) = hist.get(&session_id) {
                    if s.state == SessionState::Failed {
                        return Err(DomainError::with_detail(
                            ErrorCode::ToolExecutionFailed,
                            "scrcpy failed immediately on startup",
                            s.last_error
                                .clone()
                                .unwrap_or_else(|| "Unknown failure".into()),
                        ));
                    }
                }
            }
        }

        Ok(session)
    }

    async fn stop_mirror(&self, session_id: Option<String>) -> Result<StreamSession, DomainError> {
        let mut lock = self.active.lock().await;

        let mut managed = match lock.take() {
            Some(m) => m,
            None => {
                return Err(DomainError::new(
                    ErrorCode::SessionNotFound,
                    "No active stream owned by Nexus.",
                ));
            }
        };

        if let Some(ref sid) = session_id {
            if managed.session.session_id != *sid {
                let active_sid = managed.session.session_id.clone();
                *lock = Some(managed);
                return Err(DomainError::with_detail(
                    ErrorCode::SessionNotFound,
                    "Session ID does not match active stream",
                    format!("Requested: {}, Active: {}", sid, active_sid),
                ));
            }
        }

        managed.session.state = SessionState::Stopping;

        if let Err(e) = managed.child.kill().await {
            warn!("Failed to send kill to scrcpy: {}", e);
        }

        let wait_res = tokio::time::timeout(Duration::from_secs(3), managed.child.wait()).await;

        let mut final_session = managed.session.clone();
        final_session.state = SessionState::Stopped;

        match wait_res {
            Ok(Ok(status)) => {
                final_session.exit_code = status.code();
            }
            Ok(Err(e)) => {
                final_session.last_error = Some(e.to_string());
            }
            Err(_) => {
                final_session.last_error =
                    Some("Stop requested but termination timed out".to_string());
            }
        }

        self.history
            .lock()
            .await
            .insert(final_session.session_id.clone(), final_session.clone());

        let _ = self.events.send(SessionEvent::Exited {
            session_id: final_session.session_id.clone(),
            exit_code: final_session.exit_code,
        });

        Ok(final_session)
    }
}