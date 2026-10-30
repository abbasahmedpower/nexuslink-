use crate::ProtocolError;
use nexus_domain::is_valid_device_id;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub const MAX_KDE_PACKET_SIZE: usize = 8192; // 8 KiB hard memory bound
pub const KDE_DEFAULT_UDP_PORT: u16 = 1716;
pub const KDE_CONNECT_PROTOCOL_VERSION: u32 = 7;

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    Phone,
    Tablet,
    Laptop,
    #[default]
    Desktop,
    Tv,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityBody {
    pub device_id: String,
    pub device_name: String,
    pub device_type: DeviceType,
    pub protocol_version: u32,
    #[serde(default)]
    pub tcp_port: Option<u16>,
    #[serde(default)]
    pub incoming_capabilities: Vec<String>,
    #[serde(default)]
    pub outgoing_capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairPayload {
    pub pair: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatteryPayload {
    pub current_charge: u8,
    pub is_charging: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold_event: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KdePacketBody {
    Identity(IdentityBody),
    Pair(PairPayload),
    Battery(BatteryPayload),
    Ping,
    Custom {
        packet_type: String,
        body: serde_json::Value,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KdePacket {
    pub id: u64,
    #[serde(rename = "type")]
    pub packet_type: String,
    pub body: Value,
}

impl KdePacket {
    /// Creates a new `kdeconnect.identity` announcement packet.
    pub fn new_identity(
        device_id: impl Into<String>,
        device_name: impl Into<String>,
        device_type: DeviceType,
        tcp_port: Option<u16>,
        incoming_capabilities: Vec<String>,
        outgoing_capabilities: Vec<String>,
    ) -> Result<Self, ProtocolError> {
        let dev_id = device_id.into();
        if !is_valid_device_id(&dev_id) {
            return Err(ProtocolError::InvalidDeviceId(dev_id));
        }

        let body = IdentityBody {
            device_id: dev_id,
            device_name: device_name.into(),
            device_type,
            protocol_version: KDE_CONNECT_PROTOCOL_VERSION,
            tcp_port,
            incoming_capabilities,
            outgoing_capabilities,
        };

        let body_val =
            serde_json::to_value(body).map_err(|e| ProtocolError::MalformedJson(e.to_string()))?;

        Ok(Self {
            id: current_timestamp_ms(),
            packet_type: "kdeconnect.identity".to_string(),
            body: body_val,
        })
    }

    /// Creates a new `kdeconnect.pair` packet (true to request/accept, false to unpair).
    pub fn new_pair(pair: bool) -> Self {
        Self {
            id: current_timestamp_ms(),
            packet_type: "kdeconnect.pair".to_string(),
            body: serde_json::json!({ "pair": pair }),
        }
    }

    /// Creates a new `kdeconnect.ping` packet.
    pub fn new_ping() -> Self {
        Self {
            id: current_timestamp_ms(),
            packet_type: "kdeconnect.ping".to_string(),
            body: serde_json::json!({}),
        }
    }

    /// Creates a new `kdeconnect.battery` status packet.
    pub fn new_battery(current_charge: u8, is_charging: bool) -> Self {
        Self {
            id: current_timestamp_ms(),
            packet_type: "kdeconnect.battery".to_string(),
            body: serde_json::json!({
                "currentCharge": current_charge.min(100),
                "isCharging": is_charging
            }),
        }
    }

    /// Parses a raw buffer with strict Nano-level bounds enforcement.
    /// Rejects any packet exceeding `MAX_KDE_PACKET_SIZE` before parsing.
    pub fn parse_bounded(input: &[u8]) -> Result<Self, ProtocolError> {
        if input.len() > MAX_KDE_PACKET_SIZE {
            return Err(ProtocolError::PacketTooLarge {
                actual: input.len(),
                max: MAX_KDE_PACKET_SIZE,
            });
        }

        let trimmed = input.strip_suffix(b"\n").unwrap_or(input);
        let trimmed = trimmed.strip_suffix(b"\r").unwrap_or(trimmed);

        serde_json::from_slice(trimmed).map_err(|e| ProtocolError::MalformedJson(e.to_string()))
    }

    /// Serializes packet to wire format ending with `\n`.
    pub fn to_wire_bytes(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut bytes =
            serde_json::to_vec(self).map_err(|e| ProtocolError::MalformedJson(e.to_string()))?;
        bytes.push(b'\n');

        if bytes.len() > MAX_KDE_PACKET_SIZE {
            return Err(ProtocolError::PacketTooLarge {
                actual: bytes.len(),
                max: MAX_KDE_PACKET_SIZE,
            });
        }

        Ok(bytes)
    }

    /// Extracts typed packet body if matched.
    pub fn extract_body(&self) -> Result<KdePacketBody, ProtocolError> {
        match self.packet_type.as_str() {
            "kdeconnect.identity" => {
                let identity: IdentityBody = serde_json::from_value(self.body.clone())
                    .map_err(|e| ProtocolError::MalformedJson(e.to_string()))?;
                if !is_valid_device_id(&identity.device_id) {
                    return Err(ProtocolError::InvalidDeviceId(identity.device_id));
                }
                Ok(KdePacketBody::Identity(identity))
            }
            "kdeconnect.pair" => {
                let pair: PairPayload = serde_json::from_value(self.body.clone())
                    .map_err(|e| ProtocolError::MalformedJson(e.to_string()))?;
                Ok(KdePacketBody::Pair(pair))
            }
            "kdeconnect.battery" => {
                let battery: BatteryPayload = serde_json::from_value(self.body.clone())
                    .map_err(|e| ProtocolError::MalformedJson(e.to_string()))?;
                Ok(KdePacketBody::Battery(battery))
            }
            "kdeconnect.ping" => Ok(KdePacketBody::Ping),
            _ => Ok(KdePacketBody::Custom {
                packet_type: self.packet_type.clone(),
                body: self.body.clone(),
            }),
        }
    }
}
