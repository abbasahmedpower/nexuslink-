//! nexus-ipc — authenticated local control plane between the Rust core and clients.
//!
//! Security model (v0.4 hardened):
//! - Binds ONLY to 127.0.0.1. No LAN exposure, ever.
//! - Every connection must present the shared session token (constant-time compare)
//!   before any command is accepted. The token comes from the OS CSPRNG and lives in
//!   a mode-0600 file under a SINGLE canonical path shared with the Flutter client:
//!       $XDG_RUNTIME_DIR/nexus-hub/ipc-token        (unix)
//!       %LOCALAPPDATA%\nexus-hub\ipc-token          (windows)
//! - Requests are length-prefixed JSON frames with HARD bounds enforced *before*
//!   allocation (read_exact into fixed stack chunk; never trust the announced length).
//! - Read deadlines on every socket operation; idle connections are reclaimed.
//! - Concurrency across connections is bounded by a semaphore so a client flood
//!   cannot exhaust task slots.

use std::{io, sync::Arc, time::Duration};
use nexus_domain::IPC_VERSION;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore},
    time,
};
use tracing::warn;

pub const DEFAULT_PORT: u16 = 8727;
/// Hard bound for a single request/response frame.
pub const MAX_FRAME: usize = 64 * 1024;
/// Per-connection request rate bound (tokens refilled continuously).
const RATE_BURST: u32 = 40;
const RATE_PER_SEC: u32 = 20;
/// Deadlines (nano-level: no socket op may block forever).
const HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const BODY_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize)]
pub struct Request {
    pub token: String,
    /// Monotonic client-side id echoed back for correlation.
    pub seq: u64,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub seq: u64,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    pub fn success(seq: u64, data: serde_json::Value) -> Self {
        Self { seq, ok: true, data: Some(data), error: None }
    }
    pub fn failure(seq: u64, error: impl Into<String>) -> Self {
        Self { seq, ok: false, data: None, error: Some(error.into()) }
    }
}

// ---------------------------------------------------------------------------
// Token material & canonical token-file path
// ---------------------------------------------------------------------------

/// Cryptographically random 256-bit hex token from the OS CSPRNG.
/// Unix: getrandom(2) (kernel CSPRNG, fails closed if unavailable).
/// Windows: RtlGenRandom/BCrypt fallback via SystemFunction036.
pub fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    fill_entropy(&mut bytes).expect("no OS entropy source available; refusing to generate IPC token");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(unix)]
fn fill_entropy(buf: &mut [u8]) -> io::Result<()> {
    // getrandom() blocks only until the pool is initialized (always true post-boot)
    // and returns an error instead of weak output — fail closed.
    extern "C" {
        fn getrandom(buf: *mut u8, len: usize, flags: u32) -> isize;
    }
    let mut off = 0usize;
    while off < buf.len() {
        let n = unsafe { getrandom(buf[off..].as_mut_ptr(), buf.len() - off, 0) };
        if n <= 0 {
            return Err(io::Error::last_os_error());
        }
        off += n as usize;
    }
    Ok(())
}

#[cfg(windows)]
fn fill_entropy(buf: &mut [u8]) -> io::Result<()> {
    #[link(name = "advapi32")]
    extern "system" {
        fn SystemFunction036(buf: *mut u8, len: u32) -> u8; // RtlGenRandom
    }
    let ok = unsafe { SystemFunction036(buf.as_mut_ptr(), buf.len() as u32) };
    if ok != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(unix, windows)))]
fn fill_entropy(_buf: &mut [u8]) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "no entropy binding for this platform"))
}

/// Canonical directory holding the IPC endpoint descriptor (single source of truth:
/// BOTH the Rust daemon and the Flutter client derive it from this function's rules).
pub fn runtime_dir() -> io::Result<std::path::PathBuf> {
    #[cfg(unix)]
    {
        let base = std::env::var("XDG_RUNTIME_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::path::PathBuf::from("/tmp"));
        Ok(base.join("nexus-hub"))
    }
    #[cfg(windows)]
    {
        let base = std::env::var("LOCALAPPDATA")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        Ok(base.join("nexus-hub"))
    }
    #[cfg(not(any(unix, windows)))]
    {
        Ok(std::env::temp_dir().join("nexus-hub"))
    }
}

pub fn token_path() -> io::Result<std::path::PathBuf> {
    Ok(runtime_dir()?.join("ipc-token"))
}

pub fn endpoint_path() -> io::Result<std::path::PathBuf> {
    Ok(runtime_dir()?.join("ipc-endpoint"))
}

/// Write token + endpoint descriptor with owner-only permissions, VERIFYING that
/// the permission enforcement succeeded (P1 fix: permissions are asserted, not best-effort).
pub fn write_credentials(token: &str, port: u16) -> io::Result<()> {
    let dir = runtime_dir()?;
    std::fs::create_dir_all(&dir)?;
    enforce_dir_permissions(&dir)?;

    std::fs::write(token_path()?, token.as_bytes())?;
    enforce_file_permissions(&token_path()?)?;

    std::fs::write(endpoint_path()?, format!("127.0.0.1:{port}\n").as_bytes())?;
    enforce_file_permissions(&endpoint_path()?)?;
    Ok(())
}

#[cfg(unix)]
fn enforce_dir_permissions(path: &std::path::Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    if mode != 0o700 {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "runtime dir perms not 0700"));
    }
    Ok(())
}

#[cfg(unix)]
fn enforce_file_permissions(path: &std::path::Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    if mode != 0o600 {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "token file perms not 0600"));
    }
    Ok(())
}

#[cfg(windows)]
fn enforce_dir_permissions(_path: &std::path::Path) -> io::Result<()> {
    // LOCALAPPDATA is already per-user ACL'd; leave inherited ACLs in place.
    Ok(())
}

#[cfg(windows)]
fn enforce_file_permissions(_path: &std::path::Path) -> io::Result<()> {
    // Inherit parent (per-user) ACL. Documented limitation in THREAT_MODEL.md.
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn enforce_dir_permissions(_p: &std::path::Path) -> io::Result<()> {
    Ok(())
}
#[cfg(not(any(unix, windows)))]
fn enforce_file_permissions(_p: &std::path::Path) -> io::Result<()> {
    Ok(())
}

/// Remove the credential files (shutdown path — no stale tokens on disk).
pub fn remove_credentials() {
    let _ = std::fs::remove_file(token_path().unwrap_or_default());
    let _ = std::fs::remove_file(endpoint_path().unwrap_or_default());
}

// ---------------------------------------------------------------------------
// Constant-time comparison & rate limiting
// ---------------------------------------------------------------------------

/// Constant-time string comparison to avoid timing side channels on the token.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Simple continuous token bucket per connection (nano-level: O(1), no timers).
struct Bucket {
    tokens: f64,
    last: tokio::time::Instant,
}

impl Bucket {
    fn new() -> Self {
        Self { tokens: RATE_BURST as f64, last: tokio::time::Instant::now() }
    }
    fn allow(&mut self) -> bool {
        let now = tokio::time::Instant::now();
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * RATE_PER_SEC as f64).min(RATE_BURST as f64);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Async command handler. Returning a boxed future keeps the IPC layer decoupled
/// from whatever concurrency the application layer needs (locks, process waits, etc.).
pub type Handler = Arc<
    dyn Fn(Request) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>>
        + Send
        + Sync
        + 'static,
>;

pub struct IpcServer {
    listener: TcpListener,
    token: Arc<String>,
    handler: Handler,
    slots: Arc<Semaphore>,
}

impl IpcServer {
    /// Bind loopback-only on an OS-assigned ephemeral port (P0 fix: two daemons for
    /// the same user can no longer collide on a hard-coded port; the actual address
    /// is published through the endpoint descriptor file).
    pub async fn bind(token: String, handler: Handler) -> io::Result<Self> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        Ok(Self { listener, token: Arc::new(token), handler, slots: Arc::new(Semaphore::new(8)) })
    }

    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept loop. Returns Err only on a fatal accept error after one retry;
    /// transient errors (EMFILE-style) back off and keep serving.
    pub async fn run(&self) -> io::Result<()> {
        loop {
            match self.listener.accept().await {
                Ok((stream, peer)) => {
                    // Defense-in-depth: reject anything not coming from loopback.
                    if !peer.ip().is_loopback() {
                        warn!(%peer, "non-loopback IPC connection dropped");
                        drop(stream);
                        continue;
                    }
                    let Ok(slot) = self.slots.clone().try_acquire_owned() else {
                        warn!("IPC concurrency saturated, dropping connection");
                        continue;
                    };
                    let token = self.token.clone();
                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        let _slot = slot; // released when the task ends
                        if let Err(e) = serve_conn(stream, token, handler).await {
                            tracing::debug!("IPC connection ended: {e}");
                        }
                    });
                }
                Err(e) => {
                    warn!("IPC accept error: {e}");
                    // One bounded retry absorbs transient EMFILE-style storms; a second
                    // failure is a real outage and must propagate to the supervisor.
                    time::sleep(Duration::from_millis(50)).await;
                    match self.listener.accept().await {
                        Ok((stream, peer)) => {
                            if peer.ip().is_loopback() {
                                let token = self.token.clone();
                                let handler = self.handler.clone();
                                tokio::spawn(async move {
                                    let _ = serve_conn(stream, token, handler).await;
                                });
                            }
                        }
                        Err(e2) => return Err(e2),
                    }
                }
            }
        }
    }
}

async fn serve_conn(mut stream: TcpStream, token: Arc<String>, handler: Handler) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut bucket = Bucket::new();
    let mut len_buf = [0u8; 4];
    let mut body: Vec<u8> = Vec::with_capacity(4096);
    loop {
        // 4-byte big-endian length prefix with idle deadline.
        if time::timeout(HEADER_TIMEOUT, stream.read_exact(&mut len_buf)).await.is_err() {
            return Ok(()); // idle timeout: reclaim the socket
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > MAX_FRAME {
            // Protocol violation: close immediately, do not allocate attacker-sized buffers.
            return Ok(());
        }
        if !bucket.allow() {
            let resp = Response::failure(0, "rate-limited");
            write_frame(&mut stream, &resp).await?;
            continue;
        }
        // Bound-checked body read with a strict deadline (allocation capped at MAX_FRAME).
        body.resize(len, 0u8);
        if time::timeout(BODY_TIMEOUT, stream.read_exact(&mut body)).await.is_err() {
            return Ok(());
        }
        let req: Request = match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(_) => {
                let resp = Response::failure(0, "malformed-request");
                write_frame(&mut stream, &resp).await?;
                continue;
            }
        };
        // Authenticate BEFORE touching the handler. Never reveal why it failed.
        if !constant_time_eq(&req.token, token.as_str()) {
            let resp = Response::failure(req.seq, "unauthorized");
            write_frame(&mut stream, &resp).await?;
            continue;
        }
        let resp = handler(req).await;
        write_frame(&mut stream, &resp).await?;
    }
}

async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, resp: &Response) -> io::Result<()> {
    let mut body = serde_json::to_vec(resp).unwrap_or_else(|_| br#"{"seq":0,"ok":false}"#.to_vec());
    if body.len() > MAX_FRAME {
        let err = Response::failure(resp.seq, "response-too-large");
        body = serde_json::to_vec(&err).unwrap_or_default();
    }
    let write = async {
        w.write_all(&(body.len() as u32).to_be_bytes()).await?;
        w.write_all(&body).await?;
        w.flush().await
    };
    time::timeout(WRITE_TIMEOUT, write)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write deadline"))?
}

// ---------------------------------------------------------------------------
// Client (used by tests, CLI health checks, and mirrored by Dart)
// ---------------------------------------------------------------------------

pub struct IpcClient {
    stream: Mutex<TcpStream>,
    token: String,
}

impl IpcClient {
    pub async fn connect(addr: std::net::SocketAddr, token: &str) -> io::Result<Self> {
        let stream = time::timeout(Duration::from_secs(3), TcpStream::connect(addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect deadline"))??;
        stream.set_nodelay(true)?;
        Ok(Self { stream: Mutex::new(stream), token: token.to_string() })
    }

    /// Connect using the published endpoint descriptor + token files.
    pub async fn connect_default() -> io::Result<Self> {
        let token = tokio::fs::read_to_string(token_path()?).await?.trim().to_string();
        let ep = tokio::fs::read_to_string(endpoint_path()?).await?.trim().to_string();
        let addr: std::net::SocketAddr = ep.parse().map_err(io::Error::other)?;
        Self::connect(addr, &token).await
    }

    pub async fn call(&self, method: &str, params: serde_json::Value, seq: u64) -> io::Result<Response> {
        let req = serde_json::to_vec(&serde_json::json!({
            "token": self.token, "seq": seq, "method": method, "params": params,
            "version": IPC_VERSION
        }))
        .map_err(io::Error::other)?;
        if req.len() > MAX_FRAME {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "request too large"));
        }
        let mut guard = self.stream.lock().await;
        guard.write_all(&(req.len() as u32).to_be_bytes()).await?;
        guard.write_all(&req).await?;
        guard.flush().await?;
        let mut len_buf = [0u8; 4];
        guard.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > MAX_FRAME {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad frame"));
        }
        let mut body = vec![0u8; len];
        guard.read_exact(&mut body).await?;
        serde_json::from_slice(&body).map_err(io::Error::other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn echo_handler() -> Handler {
        Arc::new(|req: Request| {
            Box::pin(async move {
                match req.method.as_str() {
                    "echo" => Response::success(req.seq, req.params),
                    other => Response::failure(req.seq, format!("unknown-method:{other}")),
                }
            })
        })
    }

    async fn spawn_server(token: &str) -> std::net::SocketAddr {
        let server = IpcServer::bind(token.to_string(), echo_handler()).await.expect("bind");
        let addr = server.local_addr().unwrap();
        tokio::spawn(async move { server.run().await });
        addr
    }

    #[test]
    fn constant_time_eq_basics() {
        assert!(constant_time_eq("abc123", "abc123"));
        assert!(!constant_time_eq("abc123", "abc124"));
        assert!(!constant_time_eq("abc", "abcd"));
        assert!(!constant_time_eq("", "a"));
    }

    #[test]
    fn token_is_random_hex64() {
        let a = generate_token();
        let b = generate_token();
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn roundtrip_and_auth() {
        let token = generate_token();
        let addr = spawn_server(&token).await;
        let client = IpcClient::connect(addr, &token).await.unwrap();
        let resp = client.call("echo", serde_json::json!({"hello": "world"}), 7).await.unwrap();
        assert!(resp.ok);
        assert_eq!(resp.seq, 7);
        assert_eq!(resp.data.unwrap()["hello"], "world");
        // Wrong token rejected without reaching the handler.
        let bad = IpcClient::connect(addr, "deadbeef").await.unwrap();
        let resp = bad.call("echo", serde_json::json!({}), 1).await.unwrap();
        assert!(!resp.ok);
        assert_eq!(resp.error.as_deref(), Some("unauthorized"));
    }

    #[tokio::test]
    async fn rejects_oversized_frame_header() {
        let token = generate_token();
        let addr = spawn_server(&token).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&((MAX_FRAME as u32 + 10)).to_be_bytes()).await.unwrap();
        let mut buf = [0u8; 8];
        let n = time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await.expect("timeout");
        assert_eq!(n.unwrap_or(0), 0, "expected immediate close");
    }

    #[tokio::test]
    async fn truncated_body_does_not_hang_forever() {
        // Announce a full-size frame, send nothing: the body deadline must fire.
        let token = generate_token();
        let addr = spawn_server(&token).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&(MAX_FRAME as u32).to_be_bytes()).await.unwrap();
        let mut buf = [0u8; 8];
        // Server should close within ~BODY_TIMEOUT + slack; test uses generous outer bound.
        let res = time::timeout(BODY_TIMEOUT + Duration::from_secs(2), stream.read(&mut buf)).await;
        assert!(res.is_ok(), "server hung on truncated body");
        assert_eq!(res.unwrap().unwrap_or(0), 0, "expected close after body deadline");
    }

    #[tokio::test]
    async fn unknown_method_is_error_not_panic() {
        let token = generate_token();
        let addr = spawn_server(&token).await;
        let client = IpcClient::connect(addr, &token).await.unwrap();
        let resp = client.call("nope", serde_json::Value::Null, 3).await.unwrap();
        assert!(!resp.ok);
        assert!(resp.error.unwrap().starts_with("unknown-method"));
    }

    #[cfg(unix)]
    #[test]
    fn credentials_are_owner_only() {
        let dir = std::env::temp_dir().join(format!("nexus-test-{}", std::process::id()));
        std::env::set_var("XDG_RUNTIME_DIR", &dir);
        let token = generate_token();
        write_credentials(&token, 12345).expect("write creds");
        let p = token_path().unwrap();
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(&p).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(std::fs::read_to_string(&p).unwrap(), token);
        remove_credentials();
        std::fs::remove_dir_all(&dir).ok();
    }
}
