//! Nexus Hub core daemon (v0.4 — clean composition root)
//!
//! Startup order (shutdown is the reverse):
//! 1. Initialize adapters (scrcpy + kdeconnect-cli)
//! 2. Build HubApplication
//! 3. Create per-user authenticated IPC server
//! 4. Start optional passive discovery (UDP 1716) — untrusted by design
//! 5. Wait for Ctrl+C then shut down cleanly

use nexus_adapter_kde::CliKdeAdapter;
use nexus_adapter_scrcpy::ProcessScrcpySupervisor;
use nexus_application::HubApplication;
use nexus_ipc::{AuthConfig, IpcServer, IPC_TCP_FALLBACK_PORT};
use std::{
    env,
    error::Error,
    net::Ipv4Addr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{net::UdpSocket, time};
use tracing::{error, info, warn};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    info!("Nexus Hub v0.4 starting...");

    // ------------------------------------------------------------------
    // 1. Device identity (used only for discovery broadcast)
    // ------------------------------------------------------------------
    let device_id = env::var("NEXUS_DEVICE_ID").unwrap_or_else(|_| {
        format!("nexus-session-{}-{}", std::process::id(), now_ms())
    });
    let device_name =
        env::var("NEXUS_DEVICE_NAME").unwrap_or_else(|_| "Nexus Desktop".into());

    // ------------------------------------------------------------------
    // 2. Initialize adapters (official tools)
    // ------------------------------------------------------------------
    let kde_path =
        env::var("NEXUS_KDECONNECT_PATH").unwrap_or_else(|_| "kdeconnect-cli".into());
    let scrcpy_path =
        env::var("NEXUS_SCRCPY_PATH").unwrap_or_else(|_| "scrcpy".into());

    let kde_adapter = Arc::new(CliKdeAdapter::new(kde_path));
    let scrcpy_supervisor = Arc::new(ProcessScrcpySupervisor::new(scrcpy_path));

    // ------------------------------------------------------------------
    // 3. Build application layer
    // ------------------------------------------------------------------
    let app = HubApplication::new(kde_adapter, scrcpy_supervisor);

    // ------------------------------------------------------------------
    // 4. Authenticated per-user IPC server
    // ------------------------------------------------------------------
    let auth = AuthConfig::init_per_user()?;
    info!("Auth token written to {}", auth.token_path.display());

    let ipc_server = Arc::new(IpcServer::new(app, auth));

    let ipc_task = {
        let server = ipc_server.clone();
        tokio::spawn(async move {
            if let Err(e) = server.run_tcp_loopback(IPC_TCP_FALLBACK_PORT).await {
                error!("IPC server error: {}", e);
            }
        })
    };

    // ------------------------------------------------------------------
    // 5. Optional passive discovery (UDP 1716) — untrusted by design
    // ------------------------------------------------------------------
    let udp = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 1716)).await {
        Ok(socket) => {
            let _ = socket.set_broadcast(true);
            Some(Arc::new(socket))
        }
        Err(e) => {
            warn!("Could not bind UDP 1716 for discovery: {e}");
            None
        }
    };

    let broadcaster = if let Some(socket) = udp.clone() {
        let id = device_id.clone();
        let name = device_name.clone();
        Some(tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_secs(10));
            loop {
                ticker.tick().await;
                let payload = format!(
                    "{}\n",
                    serde_json::json!({
                        "id": now_ms(),
                        "type": "kdeconnect.identity",
                        "body": {
                            "deviceId": id,
                            "deviceName": name,
                            "deviceType": "laptop",
                            "protocolVersion": 7,
                            "tcpPort": 1716,
                            "incomingCapabilities": [],
                            "outgoingCapabilities": []
                        }
                    })
                );
                if let Err(e) = socket
                    .send_to(payload.as_bytes(), (Ipv4Addr::BROADCAST, 1716))
                    .await
                {
                    warn!("Discovery broadcast failed: {e}");
                }
            }
        }))
    } else {
        None
    };

    info!("Nexus Hub is ready");
    info!("  • Authenticated IPC on 127.0.0.1:{}", IPC_TCP_FALLBACK_PORT);
    if broadcaster.is_some() {
        info!("  • Passive discovery broadcasting on UDP 1716");
    }

    // ------------------------------------------------------------------
    // 6. Wait for shutdown signal
    // ------------------------------------------------------------------
    tokio::signal::ctrl_c().await?;
    info!("Shutdown requested...");

    // Clean shutdown (reverse order)
    if let Some(task) = broadcaster {
        task.abort();
    }
    ipc_task.abort();

    info!("Shutdown complete");
    Ok(())
}