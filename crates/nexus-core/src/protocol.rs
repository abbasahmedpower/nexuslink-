use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_DATAGRAM: usize = 8192;
pub const KDE_PORT: u16 = 1716;
#[allow(dead_code)]
pub const PROTOCOL_VERSION: u32 = 7; // Compatibility hypothesis; test against real peer.

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Packet {
    pub id: u64,
    #[serde(rename = "type")]
    pub kind: String,
    pub body: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    pub device_id: String,
    pub device_name: String,
    pub device_type: String,
    pub protocol_version: u32,
    #[serde(default)]
    pub tcp_port: Option<u16>,
}

pub fn parse_identity(input: &[u8]) -> Option<Identity> {
    if input.len() > MAX_DATAGRAM {
        return None;
    }
    let packet: Packet = serde_json::from_slice(input).ok()?;
    if packet.kind != "kdeconnect.identity" {
        return None;
    }
    let peer: Identity = serde_json::from_value(packet.body).ok()?;
    if peer.device_id.is_empty()
        || peer.device_id.len() > 128
        || peer.device_name.is_empty()
        || peer.device_name.len() > 256
        || peer.device_type.len() > 32
        || peer.protocol_version == 0
        || peer.tcp_port == Some(0)
    {
        return None;
    }
    Some(peer)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_malformed_and_wrong_types() {
        assert!(parse_identity(b"garbage").is_none());
        assert!(parse_identity(&vec![b'x'; MAX_DATAGRAM + 1]).is_none());
        assert!(parse_identity(br#"{"id":1,"type":"kdeconnect.ping","body":{}}"#).is_none());
        assert!(parse_identity(br#"{"id":1,"type":"kdeconnect.identity","body":{"deviceId":"","deviceName":"x","deviceType":"phone","protocolVersion":7}}"#).is_none());
    }
    #[test]
    fn accepts_identity() {
        let raw = br#"{"id":1,"type":"kdeconnect.identity","body":{"deviceId":"abc","deviceName":"Phone","deviceType":"phone","protocolVersion":7}}"#;
        assert_eq!(parse_identity(raw).unwrap().device_name, "Phone");
    }
}
