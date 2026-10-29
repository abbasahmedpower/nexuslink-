use serde::Serialize;
use std::{collections::HashMap, net::IpAddr, time::{Duration, Instant}};
use crate::protocol::Identity;

pub const MAX_PEERS: usize = 128;
pub const PEER_TTL: Duration = Duration::from_secs(90);

#[derive(Clone, Serialize)]
pub struct Peer {
    pub id: String,
    pub name: String,
    pub address: String,
    pub paired: bool,
}

struct Record { peer: Peer, last_seen: Instant }

#[derive(Default)]
pub struct PeerStore { peers: HashMap<String, Record> }

impl PeerStore {
    pub fn observe(&mut self, identity: Identity, source: IpAddr, now: Instant) {
        self.prune(now);
        if self.peers.len() >= MAX_PEERS && !self.peers.contains_key(&identity.device_id) {
            // Full peer table: reject new identities, never silently evict a potentially active peer.
            return;
        }
        self.peers.insert(identity.device_id.clone(), Record {
            peer: Peer { id: identity.device_id, name: identity.device_name, address: source.to_string(), paired: false },
            last_seen: now,
        });
    }
    pub fn prune(&mut self, now: Instant) {
        self.peers.retain(|_, v| now.saturating_duration_since(v.last_seen) < PEER_TTL);
    }
    pub fn snapshot(&mut self, now: Instant) -> Vec<Peer> {
        self.prune(now);
        let mut out: Vec<Peer> = self.peers.values().map(|r| r.peer.clone()).collect();
        out.sort_by(|a,b| a.id.cmp(&b.id));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity(id: String) -> Identity { Identity { device_id: id, device_name: "Phone".into(), device_type: "phone".into(), protocol_version: 7, tcp_port: None } }
    #[test]
    fn expires_and_limits_peers() {
        let mut store = PeerStore::default(); let now = Instant::now();
        for i in 0..MAX_PEERS + 50 { store.observe(identity(format!("d{i}")), "127.0.0.1".parse().unwrap(), now); }
        assert_eq!(store.snapshot(now).len(), MAX_PEERS);
        assert!(store.snapshot(now + PEER_TTL).is_empty());
    }
    #[test]
    fn updates_existing_peer_even_when_full() {
        let mut store = PeerStore::default(); let now = Instant::now();
        for i in 0..MAX_PEERS { store.observe(identity(format!("d{i}")), "127.0.0.1".parse().unwrap(), now); }
        store.observe(identity("d0".into()), "127.0.0.2".parse().unwrap(), now);
        assert_eq!(store.snapshot(now).iter().find(|p| p.id == "d0").unwrap().address, "127.0.0.2");
    }
}
