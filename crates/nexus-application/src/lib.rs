use nexus_adapter_kde::KdeAdapter;
use nexus_adapter_scrcpy::{ScrcpySupervisor, SessionEvent};
use nexus_domain::{
    DaemonCapabilities, DeviceInfo, DomainError, StreamSession, PROTOCOL_VERSION,
};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct HubApplication {
    kde: Arc<dyn panAdapter>,
    scrcpy: Arc<dyn ScrcpySupervisor>,
}

impl HubApplication {
    pub fn new(kde: Arc<dyn KdeAdapter>, scrcpy: Arc<dyn ScrcpySupervisor>) -> Self {
        Self { kde, scrcpy }
    }

    pub async fn get_capabilities(&self) -> DaemonCapabilities {
        let scrcpy_available = self.scrcpy.check_available().await;
        let kdeconnect_available = self.kde.check_available().await;

        DaemonCapabilities {
            protocol_version: PROTOCOL_VERSION,
            platform: std::env::consts::OS.to_string(),
            scrcpy_available,
            kdeconnect_available,
            features: vec![
                "list_devices".into(),
                "pair_device".into(),
                "ping_device".into(),
                "share_file".into(),
                "start_mirror".into(),
                "stop_mirror".into(),
                "get_session".into(),
            ],
        }
    }

    pub async fn list_devices(&self) -> Result<Vec<DeviceInfo>, DomainError> {
        self.kde.list_devices().await
    }

    pub async fn pair_device(&self, device_id: &str) -> Result<String, DomainError> {
        self.kde.pair_device(device_id).await
    }

    pub async fn ping_device(&self, device_id: &str) -> Result<String, DomainError> {
        self.kde.ping_device(device_id).await
    }

    pub async fn share_file(
        &self,
        device_id: &str,
        file_or_url: &str,
    ) -> Result<String, DomainError> {
        self.kde.share_file(device_id, file_or_url).await
    }

    pub async fn start_mirror(
        &self,
        serial: Option<String>,
        camera: bool,
        record: Option<String>,
    ) -> Result<StreamSession, DomainError> {
        self.scrcpy.start_mirror(serial, camera, record).await
    }

    pub async fn stop_mirror(
        &self,
        session_id: Option<String>,
    ) -> Result<StreamSession, DomainError> {
        self.scrcpy.stop_mirror(session_id).await
    }

    pub async fn get_active_session(&self) -> Option<StreamSession> {
        self.scrcpy.get_active_session().await
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<SessionEvent> {
        self.scrcpy.subscribe()
    }
}