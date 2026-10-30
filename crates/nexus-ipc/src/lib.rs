pub mod contract;
pub mod wire;

pub use wire::{decode as decode_frame, encode as encode_frame, sniff_codec, WireCodec};

use contract::{IpcNotification, IpcRequest, IpcResponse, WireMessage, WireMessageBody};
use nexus_application::HubApplication;
use nexus_domain::{DomainError, ErrorCode, SessionEvent, MAX_IPC_FRAME_SIZE, PROTOCOL_VERSION};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex, Semaphore};
use tracing::{info, warn};
use wire::WireCodec;

/// Result of handling a request: `(response, wants_event_subscription, negotiated_codec)`.
type HandleResult = (IpcResponse, bool, Option<WireCodec>);

pub const IPC_TCP_FALLBACK_PORT: u16 = 8788;
pub const MAX_CLIENTS: usize = 8;
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

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
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Could not determine a secure user runtime directory (USERPROFILE/HOME/LOCALAPPDATA/XDG_RUNTIME_DIR not set). Refusing insecure shared fallback.",
        ))
    }
}

/// Reads one complete inbound frame from the stream with strict memory
/// allocation bounds and codec-aware framing.
///
/// Framing rules (both formats are bounded by `max_len` *before* parsing):
/// - JSON frames: NDJSON line, terminated by `\n` (a trailing `\r` is stripped).
/// - Binary frames (`bincode1`): start with `BINCODE_TAG` (0xB1), which can
///   never appear at the start of a valid JSON document, so the format is
///   self-identifying; the frame ends at the next `\n` (appended by the writer
///   for transport consistency) or at EOF.
///
/// Guarantees: the returned buffer never exceeds `max_len`, preventing memory
/// exhaustion from unclosed or oversized lines.
async fn read_bounded_frame<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    max_len: usize,
) -> Result<Option<Vec<u8>>, DomainError> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let available = match reader.fill_buf().await {
            Ok([]) => {
                if buf.is_empty() {
                    return Ok(None); // EOF
                } else {
                    break; // EOF mid-frame (binary framing tolerates missing newline)
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
    Ok(Some(buf))
}

/// Writes an encoded frame to the transport. Encoders already guarantee the
/// frame fits within `MAX_IPC_FRAME_SIZE`, so this is a single bounded write
/// with no extra concatenation copy.
async fn write_frame<W: AsyncWriteExt + Unpin>(writer: &mut W, bytes: &[u8]) -> std::io::Result<()> {
    writer.write_all(bytes).await?;
    writer.flush().await
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
                // Outbound codec state, shared with the writer half of the
                // command loop below. Negotiated during handshake; JSON until
                // a successful bincode1 negotiation.
                let codec = Arc::new(Mutex::new(WireCodec::Json));

                // 1. Enforce strict handshake deadline (5 seconds).
                //    The handshake itself is always JSON for interoperability.
                let auth_result = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
                    let frame = match read_bounded_frame(&mut reader, MAX_IPC_FRAME_SIZE).await {
                        Ok(Some(f)) => f,
                        Ok(None) => {
                            return Err(DomainError::new(
                                ErrorCode::IpcAuthenticationRequired,
                                "Connection closed before handshake",
                            ));
                        }
                        Err(e) => return Err(e),
                    };

                    let wire_req: WireMessage = match wire::decode(&frame) {
                        Ok(m) => m,
                        Err(e) => {
                            return Err(DomainError::new(
                                ErrorCode::InvalidRequest,
                                format!("Handshake frame decode error: {}", e.message),
                            ));
                        }
                    };

                    let req = match wire_req.body {
                        WireMessageBody::Request(r) => r,
                        _ => {
                            return Err(DomainError::new(
                                ErrorCode::InvalidRequest,
                                "Expected handshake request",
                            ));
                        }
                    };

                    let (resp, _subscribe, requested_codec) =
                        Self::handle_request(&app, &auth, &mut authenticated, req).await;

                    // The handshake response is ALWAYS written in JSON, even
                    // when the client requested bincode1. Rationale (micro-level
                    // correctness): if auth fails or an old daemon replies with
                    // plain JSON to a binary-capable client, both peers must
                    // still agree on the framing of this first response. The
                    // actual upgrade happens later via `negotiate_codec`, whose
                    // reply carries the negotiated codec and is therefore
                    // self-identifying for the client.
                    let _ = requested_codec; // consumed by negotiate_codec below
                    let wire_resp = WireMessage {
                        id: wire_req.id,
                        body: WireMessageBody::Response(resp),
                    };

                    if let Ok(bytes) = wire::encode(&wire_resp, WireCodec::Json) {
                        let _ = write_frame(&mut writer, &bytes).await;
                    }

                    if !authenticated {
                        return Err(DomainError::new(
                            ErrorCode::IpcAuthenticationRequired,
                            "Authentication failed",
                        ));
                    }
                    Ok(())
                })
                .await;

                match auth_result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        warn!("IPC handshake failed for {}: {}", peer_addr, e.message);
                        let err_resp = WireMessage {
                            id: 0,
                            body: WireMessageBody::Response(IpcResponse::err(e)),
                        };
                        if let Ok(bytes) = wire::encode(&err_resp, WireCodec::Json) {
                            let _ = write_frame(&mut writer, &bytes).await;
                        }
                        return;
                    }
                    Err(_) => {
                        warn!("IPC handshake timed out after 5s for {}", peer_addr);
                        let err_resp = WireMessage {
                            id: 0,
                            body: WireMessageBody::Response(IpcResponse::err(DomainError::new(
                                ErrorCode::IpcAuthenticationRequired,
                                "Handshake deadline timed out after 5 seconds",
                            ))),
                        };
                        if let Ok(bytes) = wire::encode(&err_resp, WireCodec::Json) {
                            let _ = write_frame(&mut writer, &bytes).await;
                        }
                        return;
                    }
                }

                // 2. Authenticated command loop with idle timeout and outbound limits
                let codec_reader = codec.clone();
                let codec_writer = codec.clone();
                loop {
                    tokio::select! {
                        frame_res = tokio::time::timeout(IDLE_TIMEOUT, read_bounded_frame(&mut reader, MAX_IPC_FRAME_SIZE)) => {
                            let frame = match frame_res {
                                Ok(Ok(Some(f))) => f,
                                Ok(Ok(None)) => break, // EOF
                                Ok(Err(e)) => {
                                    let err_resp = WireMessage {
                                        id: 0,
                                        body: WireMessageBody::Response(IpcResponse::err(e)),
                                    };
                                    let cur = *codec_reader.lock().await;
                                    if let Ok(bytes) = wire::encode(&err_resp, cur) {
                                        let _ = write_frame(&mut writer, &bytes).await;
                                    }
                                    break;
                                }
                                Err(_) => {
                                    info!("IPC client {} idle timeout reached (60s), closing", peer_addr);
                                    break;
                                }
                            };

                            let wire_req: WireMessage = match wire::decode(&frame) {
                                Ok(m) => m,
                                Err(e) => {
                                    let err_resp = WireMessage {
                                        id: 0,
                                        body: WireMessageBody::Response(IpcResponse::err(e)),
                                    };
                                    let cur = *codec_reader.lock().await;
                                    if let Ok(bytes) = wire::encode(&err_resp, cur) {
                                        let _ = write_frame(&mut writer, &bytes).await;
                                    }
                                    continue;
                                }
                            };

                            let req = match wire_req.body {
                                WireMessageBody::Request(r) => r,
                                _ => continue,
                            };

                            // Inbound frames are always decoded (both codecs are
                            // accepted at any time), but the OUTBOUND codec only
                            // changes through an explicit, authenticated
                            // `negotiate_codec` exchange — never implicitly.
                            let requested_switch: Option<WireCodec> = match &req {
                                IpcRequest::NegotiateCodec { codec } => match codec.as_str() {
                                    "bincode1" => Some(WireCodec::Bincode1),
                                    "json" => Some(WireCodec::Json),
                                    _ => None,
                                },
                                _ => None,
                            };
                            let (resp, subscribe, _req_codec) = Self::handle_request(&app, &auth, &mut authenticated, req).await;
                            if subscribe {
                                event_sub = Some(app.subscribe_events());
                            }
                            // Switch ONLY when this exact request was a successful
                            // negotiation (server answered Ok). Errors keep the
                            // current framing — fail-closed.
                            let negotiated_switch = requested_switch.filter(|_| matches!(&resp, IpcResponse::Ok { .. }));

                            let wire_resp = WireMessage {
                                id: wire_req.id,
                                body: WireMessageBody::Response(resp),
                            };

                            let mut cur = *codec_writer.lock().await;
                            // Codec upgrade point (micro-level correctness):
                            // the `negotiate_codec` ACK is written in the NEW
                            // codec because every frame is self-identifying
                            // (first-byte sniffing), so there is no ambiguous
                            // transition window — the client switches on the
                            // first byte it reads, never on assumptions.
                            // A rejected/failed negotiation keeps JSON framing.
                            if let Some(new_codec) = negotiated_switch {
                                if new_codec != cur {
                                    cur = new_codec;
                                    *codec_writer.lock().await = cur;
                                    info!("IPC client {} switched outbound codec to {}", peer_addr, cur.as_str());
                                }
                            }
                            match wire::encode(&wire_resp, cur) {
                                Ok(bytes) => {
                                    if let Err(e) = write_frame(&mut writer, &bytes).await {
                                        warn!("Failed to write IPC response: {}", e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    // Encoder refused an oversized payload before writing anything.
                                    warn!("IPC response rejected by encoder: {}", e.message);
                                    let err_resp = WireMessage {
                                        id: wire_req.id,
                                        body: WireMessageBody::Response(IpcResponse::err(e)),
                                    };
                                    if let Ok(bytes) = wire::encode(&err_resp, cur) {
                                        let _ = write_frame(&mut writer, &bytes).await;
                                    }
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
                                    let cur = *codec_writer.lock().await;
                                    if let Ok(bytes) = wire::encode(&wire_notif, cur) {
                                        let _ = write_frame(&mut writer, &bytes).await;
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(missed)) => {
                                    warn!("Client lagged, missed {} events; sending resync notification", missed);
                                    let wire_notif = WireMessage {
                                        id: 0,
                                        body: WireMessageBody::Notification(IpcNotification::ResyncRequired {
                                            reason: format!("Event queue lagged, missed {} events", missed),
                                        }),
                                    };
                                    let cur = *codec_writer.lock().await;
                                    if let Ok(bytes) = wire::encode(&wire_notif, cur) {
                                        let _ = write_frame(&mut writer, &bytes).await;
                                    }
                                }
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
    ) -> HandleResult {
        match req {
            IpcRequest::Handshake {
                protocol_version,
                client_name,
                auth_token,
                codec,
            } => {
                // Parse the requested codec defensively: unknown/absent => JSON.
                let requested = match codec.as_deref() {
                    Some("bincode1") => Some(WireCodec::Bincode1),
                    Some("json") | None => Some(WireCodec::Json),
                    Some(other) => {
                        warn!("Client requested unknown IPC codec '{}'; falling back to json", other);
                        None
                    }
                };
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
                        None,
                    );
                }
                if let Some(t) = auth_token {
                    if auth.verify_token(&t) {
                        *authenticated = true;
                        info!("Client '{}' successfully authenticated", client_name);
                        return (IpcResponse::ok_empty(), false, requested);
                    }
                }
                (
                    IpcResponse::err(DomainError::new(
                        ErrorCode::IpcAuthenticationRequired,
                        "Invalid authentication token",
                    )),
                    false,
                    None,
                )
            }
            // All operations below require prior handshake authentication
            _ if !*authenticated => (
                IpcResponse::err(DomainError::new(
                    ErrorCode::IpcAuthenticationRequired,
                    "Client is not authenticated. Perform handshake with valid auth_token first.",
                )),
                false,
                None,
            ),
            IpcRequest::GetCapabilities => {
                let caps = app.get_capabilities().await;
                match IpcResponse::ok_data(caps) {
                    Ok(r) => (r, false, None),
                    Err(e) => (IpcResponse::err(e), false, None),
                }
            }
            IpcRequest::GetDiscoveryPeers => {
                let peers = app.get_discovered_peers().await;
                match IpcResponse::ok_data(peers) {
                    Ok(r) => (r, false, None),
                    Err(e) => (IpcResponse::err(e), false, None),
                }
            }
            IpcRequest::ListDevices => match app.list_devices().await {
                Ok(devs) => match IpcResponse::ok_data(devs) {
                    Ok(r) => (r, false, None),
                    Err(e) => (IpcResponse::err(e), false, None),
                },
                Err(e) => (IpcResponse::err(e), false, None),
            },
            IpcRequest::PairDevice { device_id } => match app.pair_device(&device_id).await {
                Ok(out) => match IpcResponse::ok_data(out) {
                    Ok(r) => (r, false, None),
                    Err(e) => (IpcResponse::err(e), false, None),
                },
                Err(e) => (IpcResponse::err(e), false, None),
            },
            IpcRequest::PingDevice { device_id } => match app.ping_device(&device_id).await {
                Ok(out) => match IpcResponse::ok_data(out) {
                    Ok(r) => (r, false, None),
                    Err(e) => (IpcResponse::err(e), false, None),
                },
                Err(e) => (IpcResponse::err(e), false, None),
            },
            IpcRequest::ShareFile {
                device_id,
                file_or_url,
            } => match app.share_file(&device_id, &file_or_url).await {
                Ok(out) => match IpcResponse::ok_data(out) {
                    Ok(r) => (r, false, None),
                    Err(e) => (IpcResponse::err(e), false, None),
                },
                Err(e) => (IpcResponse::err(e), false, None),
            },
            IpcRequest::StartMirror {
                serial,
                camera,
                record,
            } => match app.start_mirror(serial, camera, record).await {
                Ok(sess) => match IpcResponse::ok_data(sess) {
                    Ok(r) => (r, false, None),
                    Err(e) => (IpcResponse::err(e), false, None),
                },
                Err(e) => (IpcResponse::err(e), false, None),
            },
            IpcRequest::StopMirror { session_id } => match app.stop_mirror(session_id).await {
                Ok(sess) => match IpcResponse::ok_data(sess) {
                    Ok(r) => (r, false, None),
                    Err(e) => (IpcResponse::err(e), false, None),
                },
                Err(e) => (IpcResponse::err(e), false, None),
            },
            IpcRequest::GetSession => {
                let sess = app.get_active_session().await;
                match IpcResponse::ok_data(sess) {
                    Ok(r) => (r, false, None),
                    Err(e) => (IpcResponse::err(e), false, None),
                }
            }
            // In-session codec upgrade. The guard arm above already rejects this
            // request when the client is unauthenticated, so reaching here means
            // the session holds a valid token. Negotiation is idempotent:
            // re-negotiating `json` downgrades, `bincode1` upgrades.
            IpcRequest::NegotiateCodec { codec } => match codec.as_str() {
                "json" => (IpcResponse::ok_data("json").unwrap_or_else(|_| IpcResponse::ok_empty()), false, Some(WireCodec::Json)),
                "bincode1" => (IpcResponse::ok_data("bincode1").unwrap_or_else(|_| IpcResponse::ok_empty()), false, Some(WireCodec::Bincode1)),
                other => (
                    IpcResponse::err(DomainError::with_detail(
                        ErrorCode::InvalidRequest,
                        format!("Unsupported IPC codec '{}'", other),
                        "supported codecs: json, bincode1",
                    )),
                    false,
                    None,
                ),
            },
            IpcRequest::SubscribeEvents => (IpcResponse::ok_empty(), true, Some(WireCodec::Json)),
        }
    }
}
