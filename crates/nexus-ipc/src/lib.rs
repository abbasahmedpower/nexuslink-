pub mod contract;

use contract::{IpcNotification, IpcRequest, IpcResponse, WireMessage, WireMessageBody};
use nexus_application::HubApplication;
use nexus_domain::{DomainError, ErrorCode, SessionEvent, MAX_IPC_FRAME_SIZE, PROTOCOL_VERSION};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Semaphore};
use tracing::{info, warn};

pub const IPC_TCP_FALLBACK_PORT: u16 = 8788;
pub const MAX_CLIENTS: usize = 8;

/// Manages per-user session token stored in user's protected runtime/appdata directory
pub struct AuthConfig {
    pub token: String,
    pub token_path: PathBuf,
}

impl AuthConfig {
    pub fn init_per_user() -> std::io::Result<Self> {
        let run_dir = Self::get_user_runtime_dir()?;
        fs::create_dir_all(&run_dir)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("Failed to set runtime dir permissions: {}", e),
                )
            })?;
        }

        let token_path = run_dir.join("nexus_ipc.token");

        // Generate 32-byte CSPRNG token via OS random source
        let mut token_bytes = [0u8; 32];
        getrandom::getrandom(&mut token_bytes).map_err(|e| {
            std::io::Error::other(format!("Failed to generate secure auth token: {}", e))
        })?;
        let token: String = token_bytes.iter().map(|b| format!("{:02x}", b)).collect();

        fs::write(&token_path, token.as_bytes())?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600)).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("Failed to set token file permissions: {}", e),
                )
            })?;
        }

        // On Windows, use icacls to restrict token file to current user only.
        // This removes inherited permissions and grants only the current user Full Control,
        // mirroring the Unix 0600 protection model.
        #[cfg(windows)]
        {
            let path_str = token_path.to_string_lossy();
            // Step 1: Remove all inherited ACEs (icacls /inheritance:r)
            let _ = std::process::Command::new("icacls")
                .args([path_str.as_ref(), "/inheritance:r"])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
            // Step 2: Grant only current user Full Control
            if let Ok(username) = env::var("USERNAME") {
                let grant_arg = format!("{}:F", username);
                let _ = std::process::Command::new("icacls")
                    .args([path_str.as_ref(), "/grant:r", grant_arg.as_str()])
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status();
            }
            // Note: icacls errors are non-fatal (best-effort); the token is still required for auth.
        }

        Ok(Self { token, token_path })
    }

    /// Constant-time comparison to prevent timing side-channel leaks on token verification.
    pub fn verify_token(&self, candidate: &str) -> bool {
        let a = candidate.as_bytes();
        let b = self.token.as_bytes();
        if a.len() != b.len() {
            return false;
        }
        // XOR accumulator: if any byte differs, result will be non-zero
        let diff = a
            .iter()
            .zip(b.iter())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y));
        diff == 0
    }

    pub fn get_user_runtime_dir() -> std::io::Result<PathBuf> {
        if let Ok(dir) = env::var("XDG_RUNTIME_DIR") {
            return Ok(PathBuf::from(dir).join("nexus_hub"));
        }
        if let Ok(appdata) = env::var("LOCALAPPDATA") {
            return Ok(PathBuf::from(appdata).join("NexusHub").join("run"));
        }
        if let Ok(home) = env::var("HOME") {
            return Ok(PathBuf::from(home).join(".nexus_hub").join("run"));
        }
        if let Ok(profile) = env::var("USERPROFILE") {
            return Ok(PathBuf::from(profile).join(".nexus_hub").join("run"));
        }
        Ok(env::temp_dir().join("nexus_hub_run"))
    }
}

/// Reads a single line from the buffered reader with strict memory allocation bounds.
/// Guarantees that buffer size never exceeds `max_len`, preventing memory exhaustion from unclosed lines.
async fn read_bounded_frame<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    max_len: usize,
) -> Result<Option<String>, DomainError> {
    let mut buf = Vec::new();
    loop {
        let available = match reader.fill_buf().await {
            Ok([]) => {
                if buf.is_empty() {
                    return Ok(None); // EOF
                } else {
                    break;
                }
            }
            Ok(b) => b,
            Err(e) => return Err(DomainError::new(ErrorCode::InternalError, e.to_string())),
        };

        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            let take = pos + 1;
            if buf.len() + pos > max_len {
                reader.consume(take);
                return Err(DomainError::new(
                    ErrorCode::PayloadTooLarge,
                    format!("Frame exceeds maximum allowed size of {} bytes", max_len),
                ));
            }
            buf.extend_from_slice(&available[..pos]);
            reader.consume(take);
            break;
        } else {
            let take = available.len();
            if buf.len() + take > max_len {
                reader.consume(take);
                return Err(DomainError::new(
                    ErrorCode::PayloadTooLarge,
                    format!("Frame exceeds maximum allowed size of {} bytes", max_len),
                ));
            }
            buf.extend_from_slice(available);
            reader.consume(take);
        }
    }

    if buf.ends_with(b"\r") {
        buf.pop();
    }
    String::from_utf8(buf)
        .map(Some)
        .map_err(|e| DomainError::new(ErrorCode::InvalidRequest, format!("Invalid UTF-8: {}", e)))
}

pub struct IpcServer {
    app: HubApplication,
    auth: Arc<AuthConfig>,
    connection_limit: Arc<Semaphore>,
}

impl IpcServer {
    pub fn new(app: HubApplication, auth: AuthConfig) -> Self {
        Self {
            app,
            auth: Arc::new(auth),
            connection_limit: Arc::new(Semaphore::new(MAX_CLIENTS)),
        }
    }

    pub async fn run_tcp_loopback(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(("127.0.0.1", port)).await?;
        info!(
            "Secure Per-User IPC listening on 127.0.0.1:{} (token required at {})",
            port,
            self.auth.token_path.display()
        );

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let permit = match self.connection_limit.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    warn!(
                        "Max IPC clients reached ({}), rejecting {}",
                        MAX_CLIENTS, peer_addr
                    );
                    continue;
                }
            };

            let app = self.app.clone();
            let auth = self.auth.clone();

            tokio::spawn(async move {
                let _permit = permit;
                let (reader, mut writer) = stream.into_split();
                let mut reader = BufReader::new(reader);
                let mut authenticated = false;
                let mut event_sub: Option<broadcast::Receiver<SessionEvent>> = None;

                loop {
                    tokio::select! {
                        frame_res = read_bounded_frame(&mut reader, MAX_IPC_FRAME_SIZE) => {
                            let line = match frame_res {
                                Ok(Some(l)) => l,
                                Ok(None) => break, // EOF
                                Err(e) => {
                                    let err_resp = WireMessage {
                                        id: 0,
                                        body: WireMessageBody::Response(IpcResponse::err(e)),
                                    };
                                    if let Ok(s) = serde_json::to_string(&err_resp) {
                                        let _ = writer.write_all(format!("{}\n", s).as_bytes()).await;
                                    }
                                    break;
                                }
                            };

                            let wire_req: WireMessage = match serde_json::from_str(&line) {
                                Ok(m) => m,
                                Err(e) => {
                                    let err_resp = WireMessage {
                                        id: 0,
                                        body: WireMessageBody::Response(IpcResponse::err(DomainError::new(
                                            ErrorCode::InvalidRequest,
                                            format!("JSON parse error: {}", e),
                                        ))),
                                    };
                                    if let Ok(s) = serde_json::to_string(&err_resp) {
                                        let _ = writer.write_all(format!("{}\n", s).as_bytes()).await;
                                    }
                                    continue;
                                }
                            };

                            let req = match wire_req.body {
                                WireMessageBody::Request(r) => r,
                                _ => continue,
                            };

                            let (resp, subscribe) = Self::handle_request(&app, &auth, &mut authenticated, req).await;
                            if subscribe {
                                event_sub = Some(app.subscribe_events());
                            }

                            let wire_resp = WireMessage {
                                id: wire_req.id,
                                body: WireMessageBody::Response(resp),
                            };

                            if let Ok(serialized) = serde_json::to_string(&wire_resp) {
                                if let Err(e) = writer.write_all(format!("{}\n", serialized).as_bytes()).await {
                                    warn!("Failed to write IPC response: {}", e);
                                    break;
                                }
                            }
                        }
                        evt_res = async {
                            if let Some(ref mut sub) = event_sub {
                                sub.recv().await
                            } else {
                                std::future::pending().await
                            }
                        } => {
                            match evt_res {
                                Ok(evt) => {
                                    let notif = match evt {
                                        SessionEvent::Started(s) => IpcNotification::SessionStarted(s),
                                        SessionEvent::Exited { session_id, exit_code } => IpcNotification::SessionExited { session_id, exit_code },
                                        SessionEvent::Failed { session_id, error } => IpcNotification::SessionFailed { session_id, error },
                                    };
                                    let wire_notif = WireMessage {
                                        id: 0,
                                        body: WireMessageBody::Notification(notif),
                                    };
                                    if let Ok(s) = serde_json::to_string(&wire_notif) {
                                        let _ = writer.write_all(format!("{}\n", s).as_bytes()).await;
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => {}
                                Err(broadcast::error::RecvError::Closed) => {
                                    event_sub = None;
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    async fn handle_request(
        app: &HubApplication,
        auth: &AuthConfig,
        authenticated: &mut bool,
        req: IpcRequest,
    ) -> (IpcResponse, bool) {
        match req {
            IpcRequest::Handshake {
                protocol_version,
                client_name,
                auth_token,
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    return (
                        IpcResponse::err(DomainError::new(
                            ErrorCode::InvalidRequest,
                            format!(
                                "Incompatible protocol version {}. Expected {}",
                                protocol_version, PROTOCOL_VERSION
                            ),
                        )),
                        false,
                    );
                }
                if let Some(t) = auth_token {
                    if auth.verify_token(&t) {
                        *authenticated = true;
                        info!("Client '{}' successfully authenticated", client_name);
                        return (IpcResponse::ok_empty(), false);
                    }
                }
                (
                    IpcResponse::err(DomainError::new(
                        ErrorCode::IpcAuthenticationRequired,
                        "Invalid authentication token",
                    )),
                    false,
                )
            }
            // All operations below require prior handshake authentication
            _ if !*authenticated => (
                IpcResponse::err(DomainError::new(
                    ErrorCode::IpcAuthenticationRequired,
                    "Client is not authenticated. Perform handshake with valid auth_token first.",
                )),
                false,
            ),
            IpcRequest::GetCapabilities => {
                let caps = app.get_capabilities().await;
                match IpcResponse::ok_data(caps) {
                    Ok(r) => (r, false),
                    Err(e) => (IpcResponse::err(e), false),
                }
            }
            IpcRequest::GetDiscoveryPeers => {
                let peers = app.get_discovered_peers().await;
                match IpcResponse::ok_data(peers) {
                    Ok(r) => (r, false),
                    Err(e) => (IpcResponse::err(e), false),
                }
            }
            IpcRequest::ListDevices => match app.list_devices().await {
                Ok(devs) => match IpcResponse::ok_data(devs) {
                    Ok(r) => (r, false),
                    Err(e) => (IpcResponse::err(e), false),
                },
                Err(e) => (IpcResponse::err(e), false),
            },
            IpcRequest::PairDevice { device_id } => match app.pair_device(&device_id).await {
                Ok(out) => match IpcResponse::ok_data(out) {
                    Ok(r) => (r, false),
                    Err(e) => (IpcResponse::err(e), false),
                },
                Err(e) => (IpcResponse::err(e), false),
            },
            IpcRequest::PingDevice { device_id } => match app.ping_device(&device_id).await {
                Ok(out) => match IpcResponse::ok_data(out) {
                    Ok(r) => (r, false),
                    Err(e) => (IpcResponse::err(e), false),
                },
                Err(e) => (IpcResponse::err(e), false),
            },
            IpcRequest::ShareFile {
                device_id,
                file_or_url,
            } => match app.share_file(&device_id, &file_or_url).await {
                Ok(out) => match IpcResponse::ok_data(out) {
                    Ok(r) => (r, false),
                    Err(e) => (IpcResponse::err(e), false),
                },
                Err(e) => (IpcResponse::err(e), false),
            },
            IpcRequest::StartMirror {
                serial,
                camera,
                record,
            } => match app.start_mirror(serial, camera, record).await {
                Ok(sess) => match IpcResponse::ok_data(sess) {
                    Ok(r) => (r, false),
                    Err(e) => (IpcResponse::err(e), false),
                },
                Err(e) => (IpcResponse::err(e), false),
            },
            IpcRequest::StopMirror { session_id } => match app.stop_mirror(session_id).await {
                Ok(sess) => match IpcResponse::ok_data(sess) {
                    Ok(r) => (r, false),
                    Err(e) => (IpcResponse::err(e), false),
                },
                Err(e) => (IpcResponse::err(e), false),
            },
            IpcRequest::GetSession => {
                let sess = app.get_active_session().await;
                match IpcResponse::ok_data(sess) {
                    Ok(r) => (r, false),
                    Err(e) => (IpcResponse::err(e), false),
                }
            }
            IpcRequest::SubscribeEvents => (IpcResponse::ok_empty(), true),
        }
    }
}
