use std::env;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceOrigin {
    KdeConnect,
    Adb,
    UntrustedLanDiscovery,
}

#[derive(Debug, Clone)]
pub struct DeviceInfo {
    pub id: String,
    pub name: String,
    pub origin: DeviceOrigin,
    pub paired: bool,
    pub reachable: bool,
    pub address: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Screen,
    Camera,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Debug, Clone)]
pub struct StreamSession {
    pub session_id: String,
    pub kind: StreamKind,
    pub device_serial: Option<String>,
    pub state: SessionState,
    pub started_at_ms: u64,
    pub pid: Option<u32>,
    pub exit_code: Option<i32>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DaemonCapabilities {
    pub protocol_version: u32,
    pub platform: String,
    pub scrcpy_available: bool,
    pub kdeconnect_available: bool,
    pub features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidRequest,
    InvalidDeviceId,
    DeviceNotFound,
    ToolNotFound,
    ToolExecutionFailed,
    ToolTimeout,
    SessionAlreadyRunning,
    SessionNotFound,
    IpcAuthenticationRequired,
    PayloadTooLarge,
    InternalError,
}

#[derive(Debug, Clone)]
pub struct DomainError {
    pub code: ErrorCode,
    pub message: String,
    pub detail: Option<String>,
}

pub fn is_valid_device_id(id: &str) -> bool {
    if id.is_empty() || id.len() > 128 || id.starts_with('-') {
        return false;
    }
    id.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == ':' || c == '-')
}

pub struct AuthConfig {
    pub token: String,
    pub token_path: PathBuf,
}

impl AuthConfig {
    pub fn get_user_runtime_dir() -> PathBuf {
        if let Ok(dir) = env::var("XDG_RUNTIME_DIR") {
            return PathBuf::from(dir).join("nexus_hub");
        }
        if let Ok(appdata) = env::var("LOCALAPPDATA") {
            return PathBuf::from(appdata).join("NexusHub").join("run");
        }
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home).join(".nexus_hub").join("run");
        }
        if let Ok(profile) = env::var("USERPROFILE") {
            return PathBuf::from(profile).join(".nexus_hub").join("run");
        }
        env::temp_dir().join("nexus_hub_run")
    }

    pub fn init_per_user() -> Result<Self, String> {
        let run_dir = Self::get_user_runtime_dir();
        fs::create_dir_all(&run_dir).map_err(|e| e.to_string())?;
        let token_path = run_dir.join("nexus_ipc.token");
        let token = format!(
            "nexus-auth-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        fs::write(&token_path, token.as_bytes()).map_err(|e| e.to_string())?;
        Ok(Self { token, token_path })
    }
}
