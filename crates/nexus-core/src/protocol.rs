use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_DATAGRAM: usize = 8192;
pub const KDE_PORT: u16 = 1716;
pub const PROTOCOL_VERSION: u32 = 7; // Verify against the currently installed peer.

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
    if input.len() > MAX_DATAGRAM { return None; }
    let packet: Packet = serde_json::from_slice(input).ok()?;
    if packet.kind != "kdeconnect.identity" { return None; }
    let peer: Identity = serde_json::from_value(packet.body).ok()?;
    if peer.device_id.len() > 256 || peer.device_id.is_empty() ||
       peer.device_name.len() > 256 || peer.protocol_version == 0 { return None; }
    Some(peer)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_invalid() {
        assert!(parse_identity(b"garbage").is_none());
        assert!(parse_identity(&vec![b'x'; MAX_DATAGRAM + 1]).is_none());
        assert!(parse_identity(br#"{"id":1,"type":"kdeconnect.ping","body":{}}"#).is_none());
    }
    #[test]
    fn accepts_minimal_identity() {
        let x = br#"{"id":1,"type":"kdeconnect.identity","body":{"deviceId":"abc","deviceName":"Phone","deviceType":"phone","protocolVersion":7}}"#;
        assert_eq!(parse_identity(x).unwrap().device_name, "Phone");
    }
}
