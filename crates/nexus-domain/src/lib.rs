use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_IPC_FRAME_SIZE: usize = 65536; // 64 KiB
pub const MAX_OUTPUT_CAPTURE_SIZE: usize = 12000;

/// Validates device IDs / serials to prevent path traversal, switch injection, or command injection.
/// Allowed: 1..=128 chars of [A-Za-z0-9_.:-]
pub fn is_valid_device_id(id: &str) -> bool {
    if id.is_empty() || id.len() > 128 {
        return false;
    }
    // Reject anything starting with a dash to prevent CLI switch confusion
    if id.starts_with('-') {
        return false;
    }
    id.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == ':' || c == '-')
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceOrigin {
    KdeConnect,
    Adb,
    UntrustedLanDiscovery,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub id: String,
    pub name: String,
    pub origin: DeviceOrigin,
    pub paired: bool,
    pub reachable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Screen,
    Camera,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSession {
    pub session_id: String,
    pub kind: StreamKind,
    pub device_serial: Option<String>,
    pub state: SessionState,
    pub started_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonCapabilities {
    pub protocol_version: u32,
    pub platform: String,
    pub scrcpy_available: bool,
    pub kdeconnect_available: bool,
    pub features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl DomainError {
    pub fn new(code: ErrorCode, msg: impl Into<String>) -> Self {
        Self {
            code,
            message: msg.into(),
            detail: None,
        }
    }

    pub fn with_detail(code: ErrorCode, msg: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code,
            message: msg.into(),
            detail: Some(detail.into()),
        }
    }
}

impl std::fmt::Display for DomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for DomainError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_device_ids_strictly() {
        assert!(is_valid_device_id("device-123_test.0:x"));
        assert!(!is_valid_device_id(""));
        assert!(!is_valid_device_id("--switch"));
        assert!(!is_valid_device_id("id with spaces"));
        assert!(!is_valid_device_id("id;rm -rf /"));
        assert!(!is_valid_device_id(&"a".repeat(129)));
    }
}
