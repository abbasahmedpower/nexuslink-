//! nexus-domain — core data models and typed errors.
//!
//! This crate has NO async runtime, NO I/O, and NO dependencies on any adapter.
//! Everything here is plain data + validation, which keeps the domain unit-testable
//! in isolation (micro-level: service boundaries are enforced by the dependency graph).

use serde::{Deserialize, Serialize};

pub const IPC_VERSION: u32 = 1;

/// Typed, structured error surface shared across crates. Adapters map their
/// internal failures into these variants so clients never parse English strings.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NexusError {
    #[error("invalid-input")]
    InvalidInput,
    #[error("invalid-device-id")]
    InvalidDeviceId,
    #[error("unauthorized")]
    Unauthorized,
    #[error("not-found:{0}")]
    NotFound(String),
    #[error("already-running:{0}")]
    AlreadyRunning(String),
    #[error("tool-unavailable:{0}")]
    ToolUnavailable(String),
    #[error("spawn-failed:{0}")]
    SpawnFailed(String),
    #[error("rate-limited")]
    RateLimited,
    #[error("internal")]
    Internal,
}

impl NexusError {
    /// Stable machine-readable code for IPC responses (never localized, never free-form).
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid-input",
            Self::InvalidDeviceId => "invalid-device-id",
            Self::Unauthorized => "unauthorized",
            Self::NotFound(_) => "not-found",
            Self::AlreadyRunning(_) => "already-running",
            Self::ToolUnavailable(_) => "tool-unavailable",
            Self::SpawnFailed(_) => "spawn-failed",
            Self::RateLimited => "rate-limited",
            Self::Internal => "internal",
        }
    }
}

/// Validate a device id / ADB serial before it ever reaches an argv array.
/// Injection firewall: no shell is used anywhere, but option-smuggling via
/// crafted ids must also be impossible at the upstream-tool boundary.
pub fn valid_device_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum Command {
    /// Health check / contract probe. Always available, no side effects.
    Ping,
    /// Current trusted/untrusted peer list from the discovery engine.
    DevicesList,
    /// Start scrcpy screen mirroring. `serial` optional (single-device default).
    MirrorStart {
        #[serde(default)]
        serial: Option<String>,
    },
    /// Start scrcpy camera streaming.
    CameraStart {
        #[serde(default)]
        serial: Option<String>,
    },
    /// Stop a managed process by its supervisor handle.
    ProcessStop { handle: u64 },
    /// Snapshot of all supervised processes.
    ProcessesList,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessInfo {
    pub handle: u64,
    pub kind: String,
    pub running: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub id: String,
    pub name: String,
    pub address: String,
    pub paired: bool,
}

/// Parse and validate a command coming over IPC. Returns a typed error on any
/// malformed or out-of-contract input; never panics on untrusted data.
pub fn parse_command(json: &serde_json::Value) -> Result<Command, NexusError> {
    serde_json::from_value::<Command>(json.clone()).map_err(|_| NexusError::InvalidInput)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_commands() {
        let c = Command::MirrorStart { serial: Some("emulator-5554".into()) };
        let j = serde_json::to_value(&c).unwrap();
        assert_eq!(j["method"], "mirror_start");
        let back = parse_command(&j).unwrap();
        assert!(matches!(back, Command::MirrorStart { serial: Some(s) } if s == "emulator-5554"));
    }

    #[test]
    fn accepts_missing_serial() {
        let c = parse_command(&serde_json::json!({ "method": "mirror_start" })).unwrap();
        assert!(matches!(c, Command::MirrorStart { serial: None }));
    }

    #[test]
    fn rejects_unknown_and_malformed() {
        assert!(parse_command(&serde_json::json!({ "method": "rm_rf" })).is_err());
        assert!(parse_command(&serde_json::json!({ "method": "process_stop", "handle": "x" })).is_err());
        assert!(parse_command(&serde_json::json!("just a string")).is_err());
    }

    #[test]
    fn device_id_firewall() {
        assert!(valid_device_id("emulator-5554"));
        assert!(valid_device_id("RFC1Y7WZ9HMLTVL9"));
        assert!(valid_device_id("a.b:c-d_1"));
        assert!(!valid_device_id(""));
        assert!(!valid_device_id("--wrap=start-server-under-cover"));
        assert!(!valid_device_id("id with spaces"));
        assert!(!valid_device_id("semi;colon"));
        assert!(!valid_device_id(&"x".repeat(129)));
    }
}
