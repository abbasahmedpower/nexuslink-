# Changelog

All notable changes to NexusLink are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-10-30

### Architecture & Security Hardening
- **Hexagonal Architecture Refactor**: Inverted dependency graph. Core domain and application layers no longer depend on concrete adapter crates. Introduced `KdePort` and `ScrcpyPort` interfaces implemented by `nexus-adapter-kde` and `nexus-adapter-scrcpy`.
- **Eliminated UDP 1716 Port Collision**: Removed port hijacking and bogus KDE Connect identity broadcasts that advertised non-existent TCP ports. LAN discovery is now strictly passive and non-blocking, operating without conflict alongside official KDE Connect daemons.
- **Race-Free Process Supervision**: Replaced fragile notification primitives with `tokio::sync::watch` state channels. Introduced `SessionState::TerminationUnconfirmed` to truthfully reflect timeout events while preserving session metadata (PID, serial, start time).
- **Continuous Bounded Output Drain**: Implemented bounded memory capture (`MAX_OUTPUT_CAPTURE_SIZE`, `MAX_CHILD_OUTPUT`) combined with asynchronous sink draining (`tokio::io::copy(&mut pipe, &mut tokio::io::sink())`) to prevent pipe buffer deadlocks and memory exhaustion.
- **Child Process Lifecycle Protection**: Added `kill_on_drop(true)` to child processes and integrated graceful shutdown (`ShutdownCoordinator` on `ctrl_c`) ensuring all managed processes are terminated cleanly on daemon exit.
- **IPC Hardening & Deadlines**:
  - Added strict 5-second handshake deadline on accepted IPC connections.
  - Added 60-second idle connection timeout.
  - Added constant-time XOR token verification to eliminate timing attacks.
  - Bounded line reader strictly enforces `MAX_IPC_FRAME_SIZE` (64 KiB) before allocating memory.
  - Enforced outbound frame size limits.
  - Refused insecure shared temporary directory fallbacks.
  - Added `ResyncRequired` event notification when client broadcast subscriber lags.
- **Flutter UI Hardening**: Added connection synchronization guards (`_connectingFuture`) to prevent concurrent connection race conditions, implemented startup session discovery (`getSession`), and integrated `ResyncRequired` event triggers.
- **Automated CI & Verification**: Added GitHub Actions workflow (`.github/workflows/ci.yml`) covering Rust formatting, clippy (`-D warnings`), workspace tests, and architectural constraint verification.
