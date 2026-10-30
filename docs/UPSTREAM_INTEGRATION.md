# Upstream Integration Architecture & Policy (v1.0.0)

## 1. Integration Boundary Philosophy

NexusLink interfaces with mature open-source engines as **managed, external child processes** rather than maintaining fragile in-tree forks or directly linking against GPL codebases:

1. **`scrcpy` (Apache-2.0)**:
   - High-performance, low-latency H.264/H.265 Android screen mirroring and camera streaming.
   - Managed via `nexus-adapter-scrcpy` with strict argument validation, bounded output draining, `tokio::sync::watch` status reporting, and automatic termination (`kill_on_drop`).
   - Runs in a native high-refresh window, eliminating latency overhead from intermediate frame copy.

2. **`KDE Connect` (GPLv2+ / GPLv3+)**:
   - Device discovery, TLS mutual authentication, file/URL transfer, and ping functionality.
   - Invoked via `kdeconnect-cli` by `nexus-adapter-kde`.
   - Interacts strictly across OS process boundaries via standard I/O streams without linking against GPL libraries.

## 2. Process Supervision Contract

| Subsystem | Managed Binary | Supervised By | Error Handling & Guarantees |
| :--- | :--- | :--- | :--- |
| **Screen Mirror** | `scrcpy` | `ProcessScrcpySupervisor` | Full lifecycle tracking (`Running`, `Stopped`, `Failed`, `TerminationUnconfirmed`). Output drained to `tokio::io::sink()`. |
| **Camera Stream** | `scrcpy` | `ProcessScrcpySupervisor` | Passes `--video-source=camera` with bounded buffer drains. |
| **Device Listing** | `kdeconnect-cli` | `CliKdeAdapter` | 12-second timeout, JSON/text output parsing. |
| **Pairing Request**| `kdeconnect-cli` | `CliKdeAdapter` | Validates device ID before invocation. |
| **File Sharing**   | `kdeconnect-cli` | `CliKdeAdapter` | Passes path safely as separate argument. |

## 3. License Compliance
- **Apache-2.0 (`scrcpy`)**: Compatible with NexusLink's Apache-2.0 license.
- **GPL (`kdeconnect`)**: NexusLink executes `kdeconnect-cli` purely as an external process without dynamic or static linking, maintaining strict license separation.
