use nexus_domain::{DomainError, ErrorCode, StreamSession, MAX_IPC_FRAME_SIZE};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", content = "payload", rename_all = "snake_case")]
pub enum IpcRequest {
    Handshake {
        protocol_version: u32,
        client_name: String,
        auth_token: Option<String>,
    },
    GetCapabilities,
    GetDiscoveryPeers,
    ListDevices,
    PairDevice {
        device_id: String,
    },
    PingDevice {
        device_id: String,
    },
    ShareFile {
        device_id: String,
        file_or_url: String,
    },
    StartMirror {
        serial: Option<String>,
        camera: bool,
        record: Option<String>,
    },
    StopMirror {
        session_id: Option<String>,
    },
    GetSession,
    SubscribeEvents,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IpcResponse {
    Ok {
        #[serde(skip_serializing_if = "Option::is_none")]
        data: Option<serde_json::Value>,
    },
    Error {
        error: DomainError,
    },
}

impl IpcResponse {
    pub fn ok_empty() -> Self {
        IpcResponse::Ok { data: None }
    }

    pub fn ok_data<T: Serialize>(val: T) -> Result<Self, DomainError> {
        match serde_json::to_value(val) {
            Ok(v) => Ok(IpcResponse::Ok { data: Some(v) }),
            Err(e) => Err(DomainError::new(
                ErrorCode::InternalError,
                format!("Serialization error: {}", e),
            )),
        }
    }

    pub fn err(err: DomainError) -> Self {
        IpcResponse::Error { error: err }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub enum IpcNotification {
    SessionStarted(StreamSession),
    SessionExited {
        session_id: String,
        exit_code: Option<i32>,
    },
    SessionFailed {
        session_id: String,
        error: String,
    },
    ResyncRequired {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireMessage {
    pub id: u64,
    #[serde(flatten)]
    pub body: WireMessageBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WireMessageBody {
    Request(IpcRequest),
    Response(IpcResponse),
    Notification(IpcNotification),
}

/// Validates raw frame size before parsing
pub fn validate_frame_len(len: usize) -> Result<(), DomainError> {
    if len > MAX_IPC_FRAME_SIZE {
        Err(DomainError::new(
            ErrorCode::PayloadTooLarge,
            format!(
                "Message frame size {} exceeds limit of {} bytes",
                len, MAX_IPC_FRAME_SIZE
            ),
        ))
    } else {
        Ok(())
    }
}
