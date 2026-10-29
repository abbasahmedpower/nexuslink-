//! Authenticated local IPC server (v0.4).
//!
//! Security model:
//! - Binds ONLY to 127.0.0.1. No LAN exposure, ever.
//! - Every connection must present the shared session token (constant-time compare)
//!   before any command is accepted. The token lives in a mode-0600 file under the
//!   user's runtime directory and is regenerated every process start.
//! - Requests are length-prefixed JSON with hard bounds (frame size, line rate).
//! - One connection = one sequential request stream; concurrency across connections
//!   is bounded by a semaphore so a client flood cannot exhaust task slots.

use std::{io, sync::Arc, time::Duration};
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

/// Cryptographically random 256-bit hex token from the OS CSPRNG.
/// On unix this reads 32 bytes from /dev/urandom (non-blocking once initialized);
/// on other platforms callers should use an OS getrandom binding before shipping.
pub fn generate_token() -> String {
    use std::io::Read;
    let mut bytes = [0u8; 32];
    let entropy = std::fs::File::open("/dev/urandom").and_then(|mut f| {
        f.read_exact(&mut bytes)?;
        Ok(())
    });
    match entropy {
        Ok(()) => {}
        _ => {
            // Deterministic fallback is NOT acceptable for auth material: panic loudly
            // so the daemon refuses to start with a weak token instead of silently
            // exposing an unauthenticated control interface.
            panic!("no OS entropy source available; refusing to generate IPC token");
        }
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

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

/// Async command handler. Returning a boxed future keeps the IPC layer decoupled
/// from whatever concurrency the daemon needs (locks, process waits, etc.).
pub type Handler =
    Arc<dyn Fn(Request) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> + Send + Sync + 'static>;

pub struct IpcServer {
    listener: TcpListener,
    token: Arc<String>,
    handler: Handler,
    slots: Arc<Semaphore>,
}

impl IpcServer {
    /// Bind loopback-only on the default port. Fails if the port is taken
    /// (no silent port drift — clients must never guess where the control plane is).
    pub async fn bind(token: String, handler: Handler) -> io::Result<Self> {
        Self::bind_port(token, handler, DEFAULT_PORT).await
    }

    /// Test-only binding on an explicit port. Production code always uses `bind`.
    #[cfg(test)]
    pub async fn bind_port(token: String, handler: Handler, port: u16) -> io::Result<Self> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
        Ok(Self { listener, token: Arc::new(token), handler, slots: Arc::new(Semaphore::new(16)) })
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
    loop {
        // 4-byte big-endian length prefix, then the JSON body.
        let n = time::timeout(Duration::from_secs(30), stream.read_exact(&mut len_buf)).await;
        match n {
            Err(_) => return Ok(()), // idle timeout: reclaim the socket
            Ok(Err(e)) => return Err(e),
            Ok(Ok(_n)) => {}
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
        let mut body = vec![0u8; len];
        time::timeout(Duration::from_secs(5), stream.read_exact(&mut body))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "slow body"))??;
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
    let body = serde_json::to_vec(resp).unwrap_or_else(|_| b"{\"seq\":0,\"ok\":false}".to_vec());
    if body.len() > MAX_FRAME {
        let err = Response::failure(resp.seq, "response-too-large");
        let body = serde_json::to_vec(&err).unwrap_or_default();
        w.write_all(&(body.len() as u32).to_be_bytes()).await?;
        w.write_all(&body).await?;
        return w.flush().await;
    }
    w.write_all(&(body.len() as u32).to_be_bytes()).await?;
    w.write_all(&body).await?;
    w.flush().await
}

/// Client helper used by tests and by the daemon's own health checks.
pub struct IpcClient {
    stream: Mutex<TcpStream>,
    token: String,
}

impl IpcClient {
    pub async fn connect(addr: std::net::SocketAddr, token: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream: Mutex::new(stream), token: token.to_string() })
    }

    pub async fn call(&self, method: &str, params: serde_json::Value, seq: u64) -> io::Result<Response> {
        let req = serde_json::to_vec(&serde_json::json!({
            "token": self.token, "seq": seq, "method": method, "params": params
        }))
        .map_err(io::Error::other)?;
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
                    "boom" => Response::failure(req.seq, "internal"),
                    other => Response::failure(req.seq, format!("unknown-method:{other}")),
                }
            })
        })
    }

    async fn spawn_server(token: &str) -> std::net::SocketAddr {
        // Port 0 = OS-assigned ephemeral: parallel test binaries never collide.
        let server = IpcServer::bind_port(token.to_string(), echo_handler(), 0).await.expect("bind");
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
        // Correct token works.
        let client = IpcClient::connect(addr, &token).await.unwrap();
        let resp = client.call("echo", serde_json::json!({"hello": "world"}), 7).await.unwrap();
        assert!(resp.ok);
        assert_eq!(resp.seq, 7);
        assert_eq!(resp.data.unwrap()["hello"], "world");
        // Wrong token is rejected without reaching the handler.
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
        // Announce a frame larger than MAX_FRAME; server must close, not allocate.
        stream.write_all(&((MAX_FRAME as u32 + 10)).to_be_bytes()).await.unwrap();
        let mut buf = [0u8; 8];
        // read should hit EOF quickly because the server closes the connection.
        let n = time::timeout(Duration::from_secs(3), stream.read(&mut buf)).await.expect("timeout");
        assert_eq!(n.unwrap_or(0), 0, "expected immediate close");
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
}
