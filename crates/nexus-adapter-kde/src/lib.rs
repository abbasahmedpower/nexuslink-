//! nexus-adapter-kde — KDE Connect protocol adapter (protocol re-implementation in Rust).
//!
//! Licensing note: we REIMPLEMENT the wire protocol rather than copy GPL sources.
//! Discovery is passive and untrusted by default; pairing/TLS land in v0.6.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    net::IpAddr,
    time::{Duration, Instant},
};

pub const MAX_DATAGRAM: usize = 8192;
pub const KDE_PORT: u16 = 1716;
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

/// Parse one discovery datagram into a validated identity. Every field bound is
/// checked BEFORE anything enters the peer store (nano-level: malformed input
/// must be impossible to smuggle past the parser).
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

// ---------------------------------------------------------------------------
// Peer store
// ---------------------------------------------------------------------------

pub const MAX_PEERS: usize = 128;
pub const PEER_TTL: Duration = Duration::from_secs(90);

#[derive(Clone, Serialize)]
pub struct Peer {
    pub id: String,
    pub name: String,
    pub address: String,
    pub paired: bool,
}

struct Record {
    peer: Peer,
    last_seen: Instant,
}

#[derive(Default)]
pub struct PeerStore {
    peers: HashMap<String, Record>,
}

impl PeerStore {
    pub fn observe(&mut self, identity: Identity, source: IpAddr, now: Instant) {
        self.prune(now);
        if self.peers.len() >= MAX_PEERS && !self.peers.contains_key(&identity.device_id) {
            // Full peer table: reject new identities, never silently evict a potentially active peer.
            return;
        }
        self.peers.insert(
            identity.device_id.clone(),
            Record {
                peer: Peer {
                    id: identity.device_id,
                    name: identity.device_name,
                    address: source.to_string(),
                    paired: false,
                },
                last_seen: now,
            },
        );
    }
    pub fn prune(&mut self, now: Instant) {
        self.peers
            .retain(|_, v| now.saturating_duration_since(v.last_seen) < PEER_TTL);
    }
    pub fn snapshot(&mut self, now: Instant) -> Vec<Peer> {
        self.prune(now);
        let mut out: Vec<Peer> = self.peers.values().map(|r| r.peer.clone()).collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }
    /// Mark a peer as paired (v0.6 will gate this behind a real TLS handshake
    /// and user confirmation on both ends). Returns false for unknown peers.
    pub fn mark_paired(&mut self, id: &str) -> bool {
        match self.peers.get_mut(id) {
            Some(r) => {
                r.peer.paired = true;
                true
            }
            None => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Session channel helpers (TCP 1716-adjacent, bounded framing)
// ---------------------------------------------------------------------------

/// Build a `kdeconnect.ping` packet envelope. Fixed shape — no caller-controlled
/// strings enter the wire except the validated device id at the transport layer.
pub fn ping_packet(id: u64) -> String {
    serde_json::json!({
        "id": id, "type": "kdeconnect.ping",
        "body": { "sourceDeviceId": "nexus-hub" }
    })
    .to_string()
}

/// Build a `kdeconnect.pair` request envelope.
pub fn pair_packet(id: u64, our_device_id: &str) -> String {
    serde_json::json!({
        "id": id, "type": "kdeconnect.pair",
        "body": { "pair": true, "requestId": format!("nx-{id}"), "deviceId": our_device_id }
    })
    .to_string()
}

/// Read one newline-delimited KDE Connect packet with a HARD byte bound enforced
/// incrementally (nano-level: an unbounded/never-terminated line can never exhaust
/// memory — we cap while reading, not after).
pub async fn read_packet_bounded<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut tokio::io::BufReader<R>,
    max_bytes: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};
    let mut line = Vec::new();
    // Nano-level bound: cap consumption at max_bytes+1 *before* checking for a
    // newline, so an unterminated flood can never allocate beyond the limit.
    (&mut *reader)
        .take((max_bytes + 1) as u64)
        .read_until(b'\n', &mut line)
        .await?;
    if line.is_empty() {
        return Ok(None); // clean EOF
    }
    if line.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("packet exceeds {max_bytes} bytes"),
        ));
    }
    // Trim trailing CRLF/LF.
    while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
        line.pop();
    }
    Ok(Some(line))
}

/// Parse a response packet type from raw JSON without trusting any content fields.
pub fn packet_type(raw: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(raw).ok()?;
    v.get("type")?.as_str().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(id: String) -> Identity {
        Identity {
            device_id: id,
            device_name: "Phone".into(),
            device_type: "phone".into(),
            protocol_version: 7,
            tcp_port: None,
        }
    }

    #[test]
    fn rejects_malformed_and_wrong_types() {
        assert!(parse_identity(b"garbage").is_none());
        assert!(parse_identity(&vec![b'x'; MAX_DATAGRAM + 1]).is_none());
        assert!(parse_identity(br#"{"id":1,"type":"kdeconnect.ping","body":{}}"#).is_none());
        assert!(parse_identity(
            br#"{"id":1,"type":"kdeconnect.identity","body":{"deviceId":"","deviceName":"x","deviceType":"phone","protocolVersion":7}}"#
        )
        .is_none());
    }

    #[test]
    fn accepts_identity() {
        let raw = br#"{"id":1,"type":"kdeconnect.identity","body":{"deviceId":"abc","deviceName":"Phone","deviceType":"phone","protocolVersion":7}}"#;
        assert_eq!(parse_identity(raw).unwrap().device_name, "Phone");
    }

    #[test]
    fn expires_and_limits_peers() {
        let mut store = PeerStore::default();
        let now = Instant::now();
        for i in 0..MAX_PEERS + 50 {
            store.observe(identity(format!("d{i}")), "127.0.0.1".parse().unwrap(), now);
        }
        assert_eq!(store.snapshot(now).len(), MAX_PEERS);
        assert!(store.snapshot(now + PEER_TTL).is_empty());
    }

    #[test]
    fn updates_existing_peer_even_when_full() {
        let mut store = PeerStore::default();
        let now = Instant::now();
        for i in 0..MAX_PEERS {
            store.observe(identity(format!("d{i}")), "127.0.0.1".parse().unwrap(), now);
        }
        store.observe(identity("d0".into()), "127.0.0.2".parse().unwrap(), now);
        assert_eq!(
            store
                .snapshot(now)
                .iter()
                .find(|p| p.id == "d0")
                .unwrap()
                .address,
            "127.0.0.2"
        );
    }

    #[test]
    fn mark_paired_only_for_known_peers() {
        let mut store = PeerStore::default();
        let now = Instant::now();
        store.observe(identity("d1".into()), "127.0.0.1".parse().unwrap(), now);
        assert!(!store.mark_paired("nope"));
        assert!(store.mark_paired("d1"));
        assert!(
            store
                .snapshot(now)
                .iter()
                .find(|p| p.id == "d1")
                .unwrap()
                .paired
        );
    }

    #[tokio::test]
    async fn bounded_packet_read_rejects_oversize() {
        use tokio::io::BufReader;
        // A never-terminated huge line must fail with a bound error, not hang or balloon.
        let big = vec![b'a'; MAX_DATAGRAM + 10];
        let cursor = BufReader::new(&big[..]);
        let mut reader = cursor;
        let res = read_packet_bounded(&mut reader, MAX_DATAGRAM).await;
        assert!(res.unwrap_err().kind() == std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn bounded_packet_read_accepts_normal() {
        let payload = format!("{}\n", ping_packet(42));
        let mut reader = tokio::io::BufReader::new(payload.as_bytes());
        let raw = read_packet_bounded(&mut reader, MAX_DATAGRAM)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(packet_type(&raw).as_deref(), Some("kdeconnect.ping"));
    }
}
