//! Wire encoding for the Nexus IPC frame protocol.
//!
//! Design goals (nano/micro level):
//! - **Bounded allocation before decode.** Every encoder returns a frame whose
//!   length is provably `<= MAX_IPC_FRAME_SIZE` or an error; no unbounded
//!   intermediate buffers are ever built from untrusted input.
//! - **Zero extra copies on transport write.** Encoders produce the exact bytes
//!   to put on the socket (JSON: payload + `\n`; binary: header + payload).
//!   The server writes them with a single `write_all`, so there is no second
//!   concatenation copy of up to 64 KiB per response.
//! - **Fail-closed negotiation.** If anything goes wrong while negotiating the
//!   binary codec, both peers fall back to JSON without breaking the session.
//!
//! Frame formats:
//! - `Json`: NDJSON line, UTF-8, newline-delimited (legacy default, always
//!   supported).
//! - `Bincode1`: `[u8 codec_tag = 0xB1][bincode 1.x payload]`. The leading tag
//!   byte makes the first byte of every binary frame (`0xB1`) impossible at the
//!   start of any valid JSON frame (`{`, whitespace, etc.), so decoders can
//!   classify frames by sniffing byte 0 with zero parsing cost.

use crate::contract::WireMessage;
use nexus_domain::{DomainError, ErrorCode, MAX_IPC_FRAME_SIZE};
use serde::{Deserialize, Serialize};

/// Codec identifier carried in the handshake and used as the binary frame tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireCodec {
    /// NDJSON, newline-delimited (default, interoperable).
    Json,
    /// bincode 1.x with a 1-byte `0xB1` codec tag prefix.
    Bincode1,
}

pub const BINCODE_TAG: u8 = 0xB1;

impl WireCodec {
    pub fn as_str(&self) -> &'static str {
        match self {
            WireCodec::Json => "json",
            WireCodec::Bincode1 => "bincode1",
        }
    }
}

/// Classifies a raw inbound frame by its first byte, without parsing it.
///
/// This is safe because the two frame formats have disjoint first-byte sets:
/// JSON values cannot begin with `0xB1` (invalid UTF-8 lead byte / not a JSON
/// structural character), and every binary frame begins with `BINCODE_TAG`.
pub fn sniff_codec(frame: &[u8]) -> WireCodec {
    if frame.first() == Some(&BINCODE_TAG) {
        WireCodec::Bincode1
    } else {
        WireCodec::Json
    }
}

/// Encodes a message into the exact bytes to write on the wire.
///
/// Returns `Err(PayloadTooLarge)` *before* the frame leaves the process if the
/// encoded size would exceed [`MAX_IPC_FRAME_SIZE`], mirroring the inbound
/// bound. For JSON the trailing `\n` is included in the budget.
pub fn encode(msg: &WireMessage, codec: WireCodec) -> Result<Vec<u8>, DomainError> {
    let out = match codec {
        WireCodec::Json => {
            let mut s = serde_json::to_vec(msg)
                .map_err(|e| enc_err("serialization failed", &e))?;
            if s.len() + 1 > MAX_IPC_FRAME_SIZE {
                return Err(too_large(s.len() + 1));
            }
            s.push(b'\n');
            s
        }
        WireCodec::Bincode1 => {
            let payload =
                bincode::serialize(msg).map_err(|e| enc_err("serialization failed", &e))?;
            if payload.len() + 1 > MAX_IPC_FRAME_SIZE {
                return Err(too_large(payload.len() + 1));
            }
            let mut buf = Vec::with_capacity(payload.len() + 1);
            buf.push(BINCODE_TAG);
            buf.extend_from_slice(&payload);
            buf
        }
    };
    Ok(out)
}

/// Decodes one complete inbound frame.
///
/// The caller must have already bounded the frame size (see
/// `read_bounded_frame`); this function additionally re-checks the bound and
/// enforces it *before* handing bytes to the deserializer, so a misconfigured
/// caller still cannot trigger an unbounded parse.
pub fn decode(frame: &[u8]) -> Result<WireMessage, DomainError> {
    if frame.len() > MAX_IPC_FRAME_SIZE {
        return Err(too_large(frame.len()));
    }
    match sniff_codec(frame) {
        WireCodec::Json => {
            let s = std::str::from_utf8(frame)
                .map_err(|e| dec_err("invalid UTF-8 in JSON frame", &e))?;
            serde_json::from_str(s.trim_end_matches(['\r', '\n']))
                .map_err(|e| dec_err("JSON parse error", &e))
        }
        WireCodec::Bincode1 => {
            // Single configuration shared by encode/decode: fixed-size ints,
            // little-endian, bincode 1.x defaults. Kept private so the two
            // sides can never drift apart.
            bincode::deserialize::<WireMessage>(&frame[1..])
                .map_err(|e| dec_err("bincode parse error", &e))
        }
    }
}

fn too_large(len: usize) -> DomainError {
    DomainError::new(
        ErrorCode::PayloadTooLarge,
        format!(
            "Frame size {} exceeds limit of {} bytes",
            len, MAX_IPC_FRAME_SIZE
        ),
    )
}

fn enc_err(ctx: &str, e: &dyn std::error::Error) -> DomainError {
    DomainError::with_detail(ErrorCode::InternalError, "IPC encode error", format!("{}: {}", ctx, e))
}

fn dec_err(ctx: &str, e: &dyn std::error::Error) -> DomainError {
    DomainError::with_detail(ErrorCode::InvalidRequest, "IPC decode error", format!("{}: {}", ctx, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{IpcNotification, IpcRequest, IpcResponse, WireMessageBody};

    fn sample() -> WireMessage {
        WireMessage {
            id: 42,
            body: WireMessageBody::Request(IpcRequest::StartMirror {
                serial: Some("emulator-5554".into()),
                camera: false,
                record: None,
            }),
        }
    }

    #[test]
    fn json_roundtrip_and_sniffing() {
        for codec in [WireCodec::Json, WireCodec::Bincode1] {
            let bytes = encode(&sample(), codec).unwrap();
            assert_eq!(sniff_codec(&bytes), codec, "codec must be self-identifying");
            let back = decode(&bytes).unwrap();
            assert_eq!(back.id, 42);
            match back.body {
                WireMessageBody::Request(IpcRequest::StartMirror { serial, .. }) => {
                    assert_eq!(serial.as_deref(), Some("emulator-5554"));
                }
                _ => panic!("wrong variant after roundtrip"),
            }
        }
    }

    #[test]
    fn responses_and_notifications_roundtrip_binary() {
        let resp = WireMessage {
            id: 7,
            body: WireMessageBody::Response(IpcResponse::ok_data(vec![1u8, 2, 3]).unwrap()),
        };
        let notif = WireMessage {
            id: 0,
            body: WireMessageBody::Notification(IpcNotification::ResyncRequired {
                reason: "lagged 5".into(),
            }),
        };
        for msg in [resp, notif] {
            let bytes = encode(&msg, WireCodec::Bincode1).unwrap();
            assert_eq!(decode(&bytes).unwrap().id, msg.id);
        }
    }

    #[test]
    fn oversized_frames_are_rejected_before_decode() {
        // Craft a frame claiming to be binary but larger than the hard bound.
        let mut huge = vec![BINCODE_TAG];
        huge.resize(MAX_IPC_FRAME_SIZE + 1, 0u8);
        let err = decode(&huge).unwrap_err();
        assert_eq!(err.code, ErrorCode::PayloadTooLarge);
    }

    #[test]
    fn encoder_never_emits_oversized_frames() {
        let fat = WireMessage {
            id: 1,
            body: WireMessageBody::Notification(IpcNotification::SessionFailed {
                session_id: "s".into(),
                error: "x".repeat(MAX_IPC_FRAME_SIZE),
            }),
        };
        for codec in [WireCodec::Json, WireCodec::Bincode1] {
            let err = encode(&fat, codec).unwrap_err();
            assert_eq!(err.code, ErrorCode::PayloadTooLarge);
        }
    }

    #[test]
    fn garbage_frames_fail_closed() {
        assert!(decode(b"not json at all").is_err());
        assert!(decode(&[BINCODE_TAG, 0xff, 0xfe, 0xfd]).is_err());
        assert!(decode(b"").is_err());
    }
}
