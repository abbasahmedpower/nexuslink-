use async_trait::async_trait;
use nexus_application::{HubApplication, KdePort, ScrcpyPort};
use nexus_domain::{
    DeviceInfo, DeviceOrigin, DomainError, ErrorCode, SessionEvent, StreamSession,
    MAX_IPC_FRAME_SIZE, PROTOCOL_VERSION,
};
use nexus_ipc::contract::{IpcRequest, IpcResponse, WireMessage, WireMessageBody};
use nexus_ipc::{wire, AuthConfig, IpcServer, WireCodec};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

struct MockKde;

#[async_trait]
impl KdePort for MockKde {
    async fn check_available(&self) -> bool {
        true
    }
    async fn list_devices(&self) -> Result<Vec<DeviceInfo>, DomainError> {
        Ok(vec![DeviceInfo {
            id: "device-1".into(),
            name: "Test Phone".into(),
            origin: DeviceOrigin::KdeConnect,
            paired: true,
            reachable: true,
            address: None,
        }])
    }
    async fn pair_device(&self, _dev: &str) -> Result<String, DomainError> {
        Ok("paired".into())
    }
    async fn ping_device(&self, _dev: &str) -> Result<String, DomainError> {
        Ok("pinged".into())
    }
    async fn share_file(&self, _dev: &str, _file: &str) -> Result<String, DomainError> {
        Ok("shared".into())
    }
}

struct MockScrcpy;

#[async_trait]
impl ScrcpyPort for MockScrcpy {
    async fn check_available(&self) -> bool {
        true
    }
    fn subscribe(&self) -> broadcast::Receiver<SessionEvent> {
        let (tx, _) = broadcast::channel(16);
        tx.subscribe()
    }
    async fn get_active_session(&self) -> Option<StreamSession> {
        None
    }
    async fn start_mirror(
        &self,
        _serial: Option<String>,
        _camera: bool,
        _record: Option<String>,
    ) -> Result<StreamSession, DomainError> {
        Err(DomainError::new(ErrorCode::ToolNotFound, "mock"))
    }
    async fn stop_mirror(&self, _sid: Option<String>) -> Result<StreamSession, DomainError> {
        Err(DomainError::new(ErrorCode::SessionNotFound, "mock"))
    }
}

fn create_test_server(port: u16) -> (Arc<IpcServer>, String) {
    let kde = Arc::new(MockKde);
    let scrcpy = Arc::new(MockScrcpy);
    let app = HubApplication::new(kde, scrcpy);
    let auth = AuthConfig {
        token: "test_secret_token_1234567890abcdef".into(),
        token_path: PathBuf::from("fake_token_path"),
    };
    let token = auth.token.clone();
    let server = Arc::new(IpcServer::new(app, auth));
    let server_clone = server.clone();
    tokio::spawn(async move {
        let _ = server_clone.run_tcp_loopback(port).await;
    });
    (server, token)
}

async fn connect(port: u16) -> (BufReader<tokio::net::tcp::OwnedReadHalf>, tokio::net::tcp::OwnedWriteHalf) {
    let stream = TcpStream::connect(("127.0.0.1", port)).await.expect("connect");
    let (r, w) = stream.into_split();
    (BufReader::new(r), w)
}

/// Reads one frame of ANY codec (the transport is self-identifying by first byte).
async fn read_any_frame<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> WireMessage {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let avail = match reader.fill_buf().await {
            Ok([]) => panic!("EOF while awaiting a frame"),
            Ok(b) => b,
            Err(e) => panic!("read error: {e}"),
        };
        if let Some(pos) = avail.iter().position(|&b| b == b'\n') {
            buf.extend_from_slice(&avail[..pos]);
            reader.consume(pos + 1);
            break;
        } else {
            let take = avail.len();
            buf.extend_from_slice(avail);
            reader.consume(take);
        }
    }
    if buf.ends_with(b"\r") {
        buf.pop();
    }
    wire::decode(&buf).expect("frame must decode under either codec")
}

async fn write_json(writer: &mut tokio::net::tcp::OwnedWriteHalf, msg: &WireMessage) {
    let bytes = wire::encode(msg, WireCodec::Json).unwrap();
    writer.write_all(&bytes).await.unwrap();
    writer.flush().await.unwrap();
}

#[test]
fn test_constant_time_token_verify() {
    let auth = AuthConfig {
        token: "a1b2c3d4e5f6".into(),
        token_path: PathBuf::from("dummy"),
    };
    assert!(auth.verify_token("a1b2c3d4e5f6"));
    assert!(!auth.verify_token("a1b2c3d4e5f7"));
    assert!(!auth.verify_token("short"));
    assert!(!auth.verify_token(""));
}

#[tokio::test]
async fn test_ipc_handshake_and_authenticated_request() {
    let port = 19871;
    let (_server, token) = create_test_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let (mut reader, mut writer) = connect(port).await;

    // 1. Send Handshake (legacy shape: no codec field at all)
    let hs_req = WireMessage {
        id: 1,
        body: WireMessageBody::Request(IpcRequest::Handshake {
            protocol_version: PROTOCOL_VERSION,
            client_name: "test_suite".into(),
            auth_token: Some(token),
            codec: None,
        }),
    };
    write_json(&mut writer, &hs_req).await;

    let wire_resp = read_any_frame(&mut reader).await;
    match wire_resp.body {
        WireMessageBody::Response(IpcResponse::Ok { .. }) => {}
        other => panic!("Expected Ok response, got {:?}", other),
    }

    // 2. Send authenticated command: ListDevices
    let list_req = WireMessage {
        id: 2,
        body: WireMessageBody::Request(IpcRequest::ListDevices),
    };
    write_json(&mut writer, &list_req).await;

    let wire_resp2 = read_any_frame(&mut reader).await;
    match wire_resp2.body {
        WireMessageBody::Response(IpcResponse::Ok { data: Some(v) }) => {
            let devs: Vec<DeviceInfo> = serde_json::from_value(v).unwrap();
            assert_eq!(devs.len(), 1);
            assert_eq!(devs[0].id, "device-1");
        }
        other => panic!("Expected Ok with data, got {:?}", other),
    }
}

#[tokio::test]
async fn test_codec_negotiation_bincode_roundtrip() {
    let port = 19874;
    let (_server, token) = create_test_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let (mut reader, mut writer) = connect(port).await;

    // JSON handshake requesting bincode1 preference
    let hs = WireMessage {
        id: 1,
        body: WireMessageBody::Request(IpcRequest::Handshake {
            protocol_version: PROTOCOL_VERSION,
            client_name: "binclient".into(),
            auth_token: Some(token),
            codec: Some("bincode1".into()),
        }),
    };
    write_json(&mut writer, &hs).await;
    let resp = read_any_frame(&mut reader).await;
    assert!(matches!(resp.body, WireMessageBody::Response(IpcResponse::Ok { .. })));

    // Negotiate: request sent in JSON, ACK comes back in BINCODE (self-identifying).
    let neg = WireMessage {
        id: 2,
        body: WireMessageBody::Request(IpcRequest::NegotiateCodec {
            codec: "bincode1".into(),
        }),
    };
    write_json(&mut writer, &neg).await;
    let ack = read_any_frame(&mut reader).await;
    match ack.body {
        WireMessageBody::Response(IpcResponse::Ok { data: Some(v) }) => {
            assert_eq!(v.as_str(), Some("bincode1"));
        }
        other => panic!("expected Ok(bincode1), got {:?}", other),
    }

    // From now on the client speaks binary too.
    let req = WireMessage {
        id: 3,
        body: WireMessageBody::Request(IpcRequest::ListDevices),
    };
    let bytes = wire::encode(&req, WireCodec::Bincode1).unwrap();
    assert_eq!(bytes[0], wire::BINCODE_TAG);
    writer.write_all(&bytes).await.unwrap();
    writer.flush().await.unwrap();

    let resp = read_any_frame(&mut reader).await;
    match resp.body {
        WireMessageBody::Response(IpcResponse::Ok { data: Some(v) }) => {
            let devs: Vec<DeviceInfo> = serde_json::from_value(v).unwrap();
            assert_eq!(devs[0].id, "device-1");
        }
        other => panic!("expected Ok with devices over bincode, got {:?}", other),
    }

    // Downgrade back to JSON mid-session and confirm framing follows.
    let down = WireMessage {
        id: 4,
        body: WireMessageBody::Request(IpcRequest::NegotiateCodec {
            codec: "json".into(),
        }),
    };
    let dbytes = wire::encode(&down, WireCodec::Bincode1).unwrap();
    writer.write_all(&dbytes).await.unwrap();
    writer.flush().await.unwrap();
    let ack2 = read_any_frame(&mut reader).await;
    assert!(matches!(ack2.body, WireMessageBody::Response(IpcResponse::Ok { .. })));

    let json_req = WireMessage {
        id: 5,
        body: WireMessageBody::Request(IpcRequest::GetCapabilities),
    };
    write_json(&mut writer, &json_req).await;
    let caps = read_any_frame(&mut reader).await;
    assert!(matches!(caps.body, WireMessageBody::Response(IpcResponse::Ok { .. })));
}

#[tokio::test]
async fn test_unknown_codec_negotiation_is_rejected_without_breaking_session() {
    let port = 19875;
    let (_server, token) = create_test_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let (mut reader, mut writer) = connect(port).await;
    let hs = WireMessage {
        id: 1,
        body: WireMessageBody::Request(IpcRequest::Handshake {
            protocol_version: PROTOCOL_VERSION,
            client_name: "confused-client".into(),
            auth_token: Some(token),
            codec: Some("protobuf".into()), // unsupported
        }),
    };
    write_json(&mut writer, &hs).await;
    assert!(matches!(
        read_any_frame(&mut reader).await.body,
        WireMessageBody::Response(IpcResponse::Ok { .. })
    ));

    let neg = WireMessage {
        id: 2,
        body: WireMessageBody::Request(IpcRequest::NegotiateCodec {
            codec: "protobuf".into(),
        }),
    };
    write_json(&mut writer, &neg).await;
    let err = read_any_frame(&mut reader).await;
    match err.body {
        WireMessageBody::Response(IpcResponse::Error { error }) => {
            assert_eq!(error.code, ErrorCode::InvalidRequest);
        }
        other => panic!("expected InvalidRequest error, got {:?}", other),
    }

    // Session must remain fully usable on JSON afterwards.
    let req = WireMessage {
        id: 3,
        body: WireMessageBody::Request(IpcRequest::ListDevices),
    };
    write_json(&mut writer, &req).await;
    assert!(matches!(
        read_any_frame(&mut reader).await.body,
        WireMessageBody::Response(IpcResponse::Ok { .. })
    ));
}

#[tokio::test]
async fn test_negotiation_requires_authentication() {
    let port = 19876;
    let (_server, _token) = create_test_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let (mut reader, mut writer) = connect(port).await;
    let neg = WireMessage {
        id: 1,
        body: WireMessageBody::Request(IpcRequest::NegotiateCodec {
            codec: "bincode1".into(),
        }),
    };
    write_json(&mut writer, &neg).await;
    let resp = read_any_frame(&mut reader).await;
    match resp.body {
        WireMessageBody::Response(IpcResponse::Error { error }) => {
            assert_eq!(error.code, ErrorCode::IpcAuthenticationRequired);
        }
        other => panic!("expected auth-required error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ipc_rejects_unauthenticated_request() {
    let port = 19872;
    let (_server, _token) = create_test_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let (mut reader, mut writer) = connect(port).await;

    let req = WireMessage {
        id: 1,
        body: WireMessageBody::Request(IpcRequest::GetCapabilities),
    };
    write_json(&mut writer, &req).await;

    let resp = read_any_frame(&mut reader).await;
    match resp.body {
        WireMessageBody::Response(IpcResponse::Error { error }) => {
            assert_eq!(error.code, ErrorCode::IpcAuthenticationRequired);
        }
        other => panic!("Expected IpcAuthenticationRequired error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_ipc_rejects_oversized_frame() {
    let port = 19873;
    let (_server, _token) = create_test_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let (mut reader, mut writer) = connect(port).await;

    // Send frame larger than MAX_IPC_FRAME_SIZE (64 KiB)
    let huge_line = format!("{{\"pad\":\"{}\"}}\n", "a".repeat(MAX_IPC_FRAME_SIZE + 100));
    writer.write_all(huge_line.as_bytes()).await.unwrap();

    let mut resp_line = String::new();
    let read_res = reader.read_line(&mut resp_line).await;
    assert!(read_res.is_ok());
    let wire_resp: WireMessage = serde_json::from_str(&resp_line).unwrap();
    match wire_resp.body {
        WireMessageBody::Response(IpcResponse::Error { error }) => {
            assert_eq!(error.code, ErrorCode::PayloadTooLarge);
        }
        other => panic!("Expected PayloadTooLarge error, got {:?}", other),
    }
}
