# NexusLink Architecture & Design Specification (v1.0.0)

## 1. Architectural Style: Hexagonal (Ports & Adapters)

NexusLink adopts a clean Hexagonal / Ports & Adapters architectural model designed to strictly isolate business logic from transport protocols, external binaries, and UI frameworks.

```
+-------------------------------------------------------------------------+
|                               Flutter UI                                |
+-------------------------------------------------------------------------+
                                    │
                                    │ Authenticated Loopback TCP IPC
                                    ▼
+-------------------------------------------------------------------------+
|                               nexus-ipc                                 |
|   Handshake deadline (5s) | CSPRNG token | Constant-time verify | Bounded |
+-------------------------------------------------------------------------+
                                    │
                                    ▼
+-------------------------------------------------------------------------+
|                           nexus-application                             |
|   HubApplication Orchestration                                          |
|   Ports: KdePort, ScrcpyPort, DiscoveryProvider                         |
+-------------------------------------------------------------------------+
               │                                            │
               │                                            │
               ▼                                            ▼
+------------------------------+             +------------------------------+
|       nexus-adapter-kde      |             |     nexus-adapter-scrcpy     |
|   Implements KdePort         |             |   Implements ScrcpyPort      |
|   Spawns kdeconnect-cli      |             |   Supervises scrcpy process  |
|   Continuous bounded drain   |             |   watch channel & sink drain |
+------------------------------+             +------------------------------+
               │                                            │
               ▼                                            ▼
       [kdeconnect-cli]                                  [scrcpy]
```

## 2. Layer Responsibilities

### `nexus-domain` (Core Domain Layer)
- Pure Rust models: `DeviceInfo`, `StreamSession`, `SessionState`, `StreamKind`, `DeviceOrigin`.
- Strict validation: `is_valid_device_id` enforces `1..=128` alphanumeric characters (`^[A-Za-z0-9_.:-]+$`), explicitly disallowing leading dashes to prevent command/switch injection.
- Domain error handling: `DomainError` and `ErrorCode` taxonomy.
- Constants: Protocol versions, IPC frame limits (`MAX_IPC_FRAME_SIZE = 64 KiB`), and child capture bounds (`MAX_CHILD_OUTPUT = 64 KiB`, `MAX_OUTPUT_CAPTURE_SIZE = 12 KB`).
- Zero external dependencies beyond standard serialization (`serde`) and `thiserror`.

### `nexus-application` (Application & Port Layer)
- Defines primary ports:
  - `KdePort`: Interface for device listing, pairing, pings, and file transfers.
  - `ScrcpyPort`: Interface for streaming session lifecycles and event subscriptions.
  - `DiscoveryProvider`: Interface for passive LAN peer queries.
- `HubApplication`: Encapsulates use cases without knowledge of external CLI tooling or network sockets.

### `nexus-adapter-kde` (Infrastructure Adapter)
- Implements `KdePort` using `tokio::process::Command` targeting `kdeconnect-cli`.
- Direct execution without shell wrappers (`sh -c` / `cmd.exe /c`).
- Asynchronous bounded stdout/stderr consumption with sink draining prevents pipe deadlock.

### `nexus-adapter-scrcpy` (Infrastructure Adapter)
- Implements `ScrcpyPort` providing race-free process supervision over `scrcpy`.
- Child handle ownership is isolated to a background monitoring task (`kill_on_drop(true)` enabled).
- Utilizes `tokio::sync::watch` to notify callers of termination without lost wakeups.
- Introduces `SessionState::TerminationUnconfirmed` on timeout while preserving session metadata (PID, serial, start time).

### `nexus-ipc` (Transport Layer)
- Loopback TCP transport listening on `127.0.0.1:8788`.
- Token-based authentication using per-user CSPRNG tokens with filesystem permissions (`0600` / user-only ACL).
- Bounded line reader strictly enforces frame limits before allocation.
- 5-second handshake deadline and 60-second idle connection timeout.
- Real-time event streaming (`SessionStarted`, `SessionExited`, `SessionFailed`, `ResyncRequired`).

### `nexus-core` (Composition & Runtime)
- Initializes adapters, configures per-user security, and wires dependencies.
- Runs non-colliding passive LAN discovery (receive-only on UDP 1716, non-blocking on failure).
- Graceful shutdown coordinator: Captures Ctrl+C signals and cleanly terminates active child processes.

### `ui` (Presentation Layer)
- Built with Flutter 3.x for cross-platform desktop execution.
- Typed IPC client (`NexusIpcClient`) with concurrency protection (`_connectingFuture`).
- Reactive dashboard reflecting real-time supervisor events.
