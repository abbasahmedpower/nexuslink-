//! # Native KDE Connect Protocol Engine (`nexus-protocol-kde`)
//!
//! A pure-Rust, zero-external-dependency implementation of the KDE Connect protocol.
//! Solves both Nano-level (memory layout, zero-alloc bounds, constant-time verification)
//! and Micro-level (finite state machine, failure isolation, failure domain isolation) problems.

pub mod crypto;
pub mod fsm;
pub mod packet;

pub use crypto::{sha256_digest, verify_fingerprint_constant_time, CertificateFingerprint};
pub use fsm::{DeviceConnectionState, PairingEvent, PairingStateMachine};
pub use packet::{
    BatteryPayload, DeviceType, IdentityBody, KdePacket, KdePacketBody, PairPayload,
    KDE_CONNECT_PROTOCOL_VERSION, KDE_DEFAULT_UDP_PORT, MAX_KDE_PACKET_SIZE,
};

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("Packet exceeds maximum allowed size of {max} bytes (received {actual} bytes)")]
    PacketTooLarge { actual: usize, max: usize },

    #[error("Malformed JSON payload: {0}")]
    MalformedJson(String),

    #[error("Invalid packet structure: {0}")]
    InvalidPacket(String),

    #[error(
        "Invalid device identifier: '{0}' (must match ^[A-Za-z0-9_.:-]+$ and not start with '-')"
    )]
    InvalidDeviceId(String),

    #[error("Protocol version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u32, actual: u32 },

    #[error("Invalid state transition: cannot transition from {from:?} with event {event:?}")]
    InvalidStateTransition {
        from: Box<DeviceConnectionState>,
        event: PairingEvent,
    },
}
