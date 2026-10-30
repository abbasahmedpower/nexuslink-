use async_trait::async_trait;
use nexus_application::{HubApplication, KdePort, ScrcpyPort};
use nexus_domain::{
    DeviceInfo, DeviceOrigin, DomainError, ErrorCode, SessionEvent, StreamSession,
    MAX_IPC_FRAME_SIZE, PROTOCOL_VERSION,
};
use nexus_ipc::contract::{IpcRequest, IpcResponse, WireMessage, WireMessageBody};
use nexus_ipc::{AuthConfig, IpcServer};
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

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("Failed to connect");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // 1. Send Handshake
    let hs_req = WireMessage {
        id: 1,
        body: WireMessageBody::Request(IpcRequest::Handshake {
            protocol_version: PROTOCOL_VERSION,
            client_name: "test_suite".into(),
            auth_token: Some(token),
        }),
    };
    let line = format!("{}\n", serde_json::to_string(&hs_req).unwrap());
    writer.write_all(line.as_bytes()).await.unwrap();

    let mut resp_line = String::new();
    reader.read_line(&mut resp_line).await.unwrap();
    let wire_resp: WireMessage = serde_json::from_str(&resp_line).unwrap();
    match wire_resp.body {
        WireMessageBody::Response(IpcResponse::Ok { .. }) => {}
        other => panic!("Expected Ok response, got {:?}", other),
    }

    // 2. Send authenticated command: ListDevices
    let list_req = WireMessage {
        id: 2,
        body: WireMessageBody::Request(IpcRequest::ListDevices),
    };
    let line = format!("{}\n", serde_json::to_string(&list_req).unwrap());
    writer.write_all(line.as_bytes()).await.unwrap();

    let mut resp_line2 = String::new();
    reader.read_line(&mut resp_line2).await.unwrap();
    let wire_resp2: WireMessage = serde_json::from_str(&resp_line2).unwrap();
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
async fn test_ipc_rejects_unauthenticated_request() {
    let port = 19872;
    let (_server, _token) = create_test_server(port);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("Failed to connect");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Send GetCapabilities directly without handshake
    let req = WireMessage {
        id: 1,
        body: WireMessageBody::Request(IpcRequest::GetCapabilities),
    };
    let line = format!("{}\n", serde_json::to_string(&req).unwrap());
    writer.write_all(line.as_bytes()).await.unwrap();

    let mut resp_line = String::new();
    reader.read_line(&mut resp_line).await.unwrap();
    let wire_resp: WireMessage = serde_json::from_str(&resp_line).unwrap();
    match wire_resp.body {
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

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("Failed to connect");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

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
