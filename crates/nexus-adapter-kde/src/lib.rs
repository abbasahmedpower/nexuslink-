use async_trait::async_trait;
use nexus_application::KdePort;
use nexus_domain::{
    is_valid_device_id, DeviceInfo, DeviceOrigin, DomainError, ErrorCode, MAX_OUTPUT_CAPTURE_SIZE,
};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

/// Re-export KdePort as KdeAdapter for backwards compatibility if needed.
pub use nexus_application::KdePort as KdeAdapter;

#[derive(Clone)]
pub struct CliKdeAdapter {
    executable: PathBuf,
    timeout: Duration,
}

impl CliKdeAdapter {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            timeout: Duration::from_secs(12),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn run_cli(&self, args: &[&str]) -> Result<String, DomainError> {
        let mut cmd = Command::new(&self.executable);
        cmd.args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Spawn child safely without shell
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                return Err(DomainError::with_detail(
                    ErrorCode::ToolNotFound,
                    format!("Failed to spawn kdeconnect-cli: {}", e),
                    e.to_string(),
                ));
            }
        };

        let mut stdout_pipe = child.stdout.take().unwrap();
        let mut stderr_pipe = child.stderr.take().unwrap();

        let run_fut = async {
            let mut out_buf = Vec::new();
            let mut err_buf = Vec::new();

            let drain_stdout = async {
                let mut limited = (&mut stdout_pipe).take(MAX_OUTPUT_CAPTURE_SIZE as u64);
                let _ = limited.read_to_end(&mut out_buf).await;
                let mut sink = tokio::io::sink();
                let _ = tokio::io::copy(&mut stdout_pipe, &mut sink).await;
            };

            let drain_stderr = async {
                let mut limited = (&mut stderr_pipe).take(MAX_OUTPUT_CAPTURE_SIZE as u64);
                let _ = limited.read_to_end(&mut err_buf).await;
                let mut sink = tokio::io::sink();
                let _ = tokio::io::copy(&mut stderr_pipe, &mut sink).await;
            };

            let ((), (), status) = tokio::join!(drain_stdout, drain_stderr, child.wait());
            let status = status.map_err(|e| e.to_string())?;
            Ok::<_, String>((status, out_buf, err_buf))
        };

        match tokio::time::timeout(self.timeout, run_fut).await {
            Ok(Ok((status, out, err))) => {
                let mut combined = String::from_utf8_lossy(&out).trim().to_string();
                let err_str = String::from_utf8_lossy(&err).trim().to_string();
                if !err_str.is_empty() {
                    if !combined.is_empty() {
                        combined.push('\n');
                    }
                    combined.push_str(&err_str);
                }
                if combined.len() > MAX_OUTPUT_CAPTURE_SIZE {
                    combined.truncate(MAX_OUTPUT_CAPTURE_SIZE);
                }

                if status.success() {
                    Ok(if combined.is_empty() {
                        "OK".to_string()
                    } else {
                        combined
                    })
                } else {
                    Err(DomainError::with_detail(
                        ErrorCode::ToolExecutionFailed,
                        format!("kdeconnect-cli failed with status: {}", status),
                        combined,
                    ))
                }
            }
            Ok(Err(e)) => Err(DomainError::with_detail(
                ErrorCode::ToolExecutionFailed,
                "IO error during kdeconnect-cli execution",
                e,
            )),
            Err(_) => {
                let _ = child.kill().await;
                Err(DomainError::new(
                    ErrorCode::ToolTimeout,
                    "kdeconnect-cli timed out. Check the KDE Connect daemon.",
                ))
            }
        }
    }
}

#[async_trait]
impl KdePort for CliKdeAdapter {
    async fn check_available(&self) -> bool {
        let mut cmd = Command::new(&self.executable);
        cmd.arg("--version")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match tokio::time::timeout(Duration::from_secs(3), cmd.status()).await {
            Ok(Ok(status)) => status.success(),
            _ => false,
        }
    }

    async fn list_devices(&self) -> Result<Vec<DeviceInfo>, DomainError> {
        let output = self
            .run_cli(&["--list-available", "--id-name-only"])
            .await?;
        let mut devices = Vec::new();
        // Parse output lines: "- <id>: <name>" or "<id> <name>"
        for line in output.lines() {
            let trimmed = line.trim().trim_start_matches('-').trim();
            if trimmed.is_empty() {
                continue;
            }
            let (id, name) = if let Some((i, n)) = trimmed.split_once(':') {
                (i.trim(), n.trim())
            } else if let Some((i, n)) = trimmed.split_once(' ') {
                (i.trim(), n.trim())
            } else {
                (trimmed, trimmed)
            };

            if is_valid_device_id(id) {
                devices.push(DeviceInfo {
                    id: id.to_string(),
                    name: name.to_string(),
                    origin: DeviceOrigin::KdeConnect,
                    paired: true,
                    reachable: true,
                    address: None,
                });
            }
        }
        Ok(devices)
    }

    async fn pair_device(&self, device_id: &str) -> Result<String, DomainError> {
        if !is_valid_device_id(device_id) {
            return Err(DomainError::new(
                ErrorCode::InvalidDeviceId,
                "Invalid KDE device ID",
            ));
        }
        self.run_cli(&["--pair", "--device", device_id]).await
    }

    async fn ping_device(&self, device_id: &str) -> Result<String, DomainError> {
        if !is_valid_device_id(device_id) {
            return Err(DomainError::new(
                ErrorCode::InvalidDeviceId,
                "Invalid KDE device ID",
            ));
        }
        self.run_cli(&["--ping", "--device", device_id]).await
    }

    async fn share_file(&self, device_id: &str, file_or_url: &str) -> Result<String, DomainError> {
        if !is_valid_device_id(device_id) {
            return Err(DomainError::new(
                ErrorCode::InvalidDeviceId,
                "Invalid KDE device ID",
            ));
        }
        let target = file_or_url.trim();
        if target.is_empty() {
            return Err(DomainError::new(
                ErrorCode::InvalidRequest,
                "Path or URL cannot be empty",
            ));
        }
        self.run_cli(&["--share", target, "--device", device_id])
            .await
    }
}
