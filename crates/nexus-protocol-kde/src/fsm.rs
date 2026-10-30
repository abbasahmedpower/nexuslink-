//! Finite State Machine for Native KDE Connect Device Lifecycle.
//! Enforces micro-level determinism, explicit state tracking, and failure isolation.

use crate::packet::IdentityBody;
use crate::ProtocolError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceConnectionState {
    /// Discovered via passive UDP broadcast, not yet paired or connected
    Discovered { identity: IdentityBody },
    /// We sent a pairing request; waiting for peer user to accept
    PairingRequestedByUs { identity: IdentityBody },
    /// Peer sent a pairing request; waiting for local user to accept
    PairingRequestedByPeer { identity: IdentityBody },
    /// Mutual pairing confirmed; stored in trusted store with certificate fingerprint
    Paired {
        identity: IdentityBody,
        fingerprint: String,
    },
    /// TLS connection established and ready for bidirectional payload exchange
    Connected {
        identity: IdentityBody,
        fingerprint: String,
    },
    /// Explicitly unpaired or connection terminated
    Unpaired { device_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairingEvent {
    IdentityUpdated(IdentityBody),
    SendPairRequest,
    ReceivePairRequest,
    AcceptPairing { fingerprint: String },
    RejectPairing,
    TlsHandshakeSuccess,
    TlsDisconnected,
    UnpairCommand,
}

#[derive(Debug, Clone)]
pub struct PairingStateMachine {
    state: DeviceConnectionState,
}

impl PairingStateMachine {
    pub fn new(identity: IdentityBody) -> Self {
        Self {
            state: DeviceConnectionState::Discovered { identity },
        }
    }

    pub fn state(&self) -> &DeviceConnectionState {
        &self.state
    }

    pub fn device_id(&self) -> &str {
        match &self.state {
            DeviceConnectionState::Discovered { identity }
            | DeviceConnectionState::PairingRequestedByUs { identity }
            | DeviceConnectionState::PairingRequestedByPeer { identity }
            | DeviceConnectionState::Paired { identity, .. }
            | DeviceConnectionState::Connected { identity, .. } => &identity.device_id,
            DeviceConnectionState::Unpaired { device_id } => device_id,
        }
    }

    pub fn is_paired(&self) -> bool {
        matches!(
            self.state,
            DeviceConnectionState::Paired { .. } | DeviceConnectionState::Connected { .. }
        )
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.state, DeviceConnectionState::Connected { .. })
    }

    /// Transitions the state machine based on a concrete incoming or outgoing event.
    /// Returns the updated state or an `InvalidStateTransition` error.
    pub fn transition(
        &mut self,
        event: PairingEvent,
    ) -> Result<&DeviceConnectionState, ProtocolError> {
        let current = self.state.clone();
        let next_state = match (&current, event.clone()) {
            // Update identity details while in discovered state
            (DeviceConnectionState::Discovered { .. }, PairingEvent::IdentityUpdated(id)) => {
                DeviceConnectionState::Discovered { identity: id }
            }

            // Local user initiates pairing request
            (DeviceConnectionState::Discovered { identity }, PairingEvent::SendPairRequest) => {
                DeviceConnectionState::PairingRequestedByUs {
                    identity: identity.clone(),
                }
            }

            // Peer device initiates pairing request
            (DeviceConnectionState::Discovered { identity }, PairingEvent::ReceivePairRequest) => {
                DeviceConnectionState::PairingRequestedByPeer {
                    identity: identity.clone(),
                }
            }

            // Peer accepted our pairing request or local user accepted peer's request
            (
                DeviceConnectionState::PairingRequestedByUs { identity },
                PairingEvent::AcceptPairing { fingerprint },
            )
            | (
                DeviceConnectionState::PairingRequestedByPeer { identity },
                PairingEvent::AcceptPairing { fingerprint },
            ) => {
                // Cryptographic validation: Verify fingerprint format before transitioning to Paired state
                if crate::crypto::CertificateFingerprint::parse_str(&fingerprint).is_none() {
                    return Err(ProtocolError::InvalidPacket(format!(
                        "Invalid certificate fingerprint format for device {}",
                        identity.device_id
                    )));
                }
                DeviceConnectionState::Paired {
                    identity: identity.clone(),
                    fingerprint,
                }
            }

            // Either side rejects pairing
            (
                DeviceConnectionState::PairingRequestedByUs { identity }
                | DeviceConnectionState::PairingRequestedByPeer { identity },
                PairingEvent::RejectPairing,
            ) => DeviceConnectionState::Discovered {
                identity: identity.clone(),
            },

            // TLS handshake succeeds after being paired
            (
                DeviceConnectionState::Paired {
                    identity,
                    fingerprint,
                },
                PairingEvent::TlsHandshakeSuccess,
            ) => DeviceConnectionState::Connected {
                identity: identity.clone(),
                fingerprint: fingerprint.clone(),
            },

            // TLS connection drops; revert back to paired state (ready for reconnect)
            (
                DeviceConnectionState::Connected {
                    identity,
                    fingerprint,
                },
                PairingEvent::TlsDisconnected,
            ) => DeviceConnectionState::Paired {
                identity: identity.clone(),
                fingerprint: fingerprint.clone(),
            },

            // Unpair command from paired or connected state
            (
                DeviceConnectionState::Paired { identity, .. }
                | DeviceConnectionState::Connected { identity, .. },
                PairingEvent::UnpairCommand,
            ) => DeviceConnectionState::Unpaired {
                device_id: identity.device_id.clone(),
            },

            // Invalid transition attempted
            _ => {
                return Err(ProtocolError::InvalidStateTransition {
                    from: Box::new(current),
                    event: Box::new(event),
                });
            }
        };

        self.state = next_state;
        Ok(&self.state)
    }
}
