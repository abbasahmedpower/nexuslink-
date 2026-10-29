//! Typed command contracts shared by the IPC server and every client (Flutter, CLI).
//! Keeping these in one place means the wire format is versioned with the code that
//! produces it — no stringly-typed drift between UI and core.

use serde::{Deserialize, Serialize};

pub const IPC_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum Command {
    /// Health check / contract probe. Always available, no side effects.
    Ping,
    /// Current trusted/untrusted peer list from the discovery engine.
    DevicesList,
    /// Start scrcpy screen mirroring. `serial` optional (single-device default).
    MirrorStart { serial: Option<String> },
    /// Start scrcpy camera streaming.
    CameraStart { serial: Option<String> },
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

/// Parse and validate a command coming over IPC. Returns an error string on any
/// malformed or out-of-contract input; never panics on untrusted data.
pub fn parse_command(json: &serde_json::Value) -> Result<Command, String> {
    serde_json::from_value::<Command>(json.clone())
        .map_err(|e| format!("invalid-command: {e}"))
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
        matches!(back, Command::MirrorStart { serial: Some(s) } if s == "emulator-5554");
    }

    #[test]
    fn rejects_unknown_and_malformed() {
        assert!(parse_command(&serde_json::json!({"method": "rm_rf"})).is_err());
        assert!(parse_command(&serde_json::json!({"method": "process_stop", "handle": "not-a-number"})).is_err());
        assert!(parse_command(&serde_json::json!("just a string")).is_err());
    }

    #[test]
    fn accepts_missing_serial() {
        let c = parse_command(&serde_json::json!({"method": "mirror_start"})).unwrap();
        matches!(c, Command::MirrorStart { serial: None });
    }
}
