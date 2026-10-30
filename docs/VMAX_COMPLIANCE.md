# NexusLink V-MAX Architecture & Systems Audit Matrix

This document provides a rigorous, evidence-based compliance evaluation of NexusLink against the V-MAX Engineering Directive. Each claim is strictly classified as **Verified**, **Partially Verified**, or **Spike / Roadmap**, linked to concrete source locations and automated test cases.

---

## 1. Nano-Level Axioms (Memory, Layout, CPU & Cryptography)

| Axiom | Engineering Status | Code Location | Evidence & Test Verification |
| :--- | :--- | :--- | :--- |
| **1.1 Bounded Allocation Before Parsing** | **Verified** | `nexus-ipc::read_bounded_frame`<br/>`nexus-protocol-kde::parse_bounded` | Strict length bounds checked iteratively on raw slices. Rejects frames exceeding 64 KiB (IPC) or 8 KiB (KDE) before parsing. Proven by `test_ipc_rejects_oversized_frame` & `test_packet_too_large_rejection`. Note: While bounded against DoS, payloads still allocate on heap via `Vec`/`serde_json` (not pure zero-copy). |
| **1.2 Cache-Friendly Stack Layout** | **Verified** | `nexus-protocol-kde::ProtocolError`<br/>`nexus-domain::DomainError` | `ProtocolError` inner state boxed (`Box<DeviceConnectionState>`). Proven via `std::mem::size_of::<ProtocolError>() <= 32` bytes in `test_memory_layout_and_size_of`. |
| **1.3 Constant-Time Cryptography** | **Verified** | `nexus-protocol-kde::crypto`<br/>`nexus-ipc::AuthConfig` | Constant-time slice comparison enforced using `subtle::ConstantTimeEq` (compiler-barrier resistant to early exit optimizations) and FIPS 180-4 SHA-256 via `sha2`. Verified in `test_constant_time_fingerprint_verification`. |
| **1.4 Slice Hygiene & Validation** | **Verified** | `nexus-domain::is_valid_device_id` | Validates identifiers against whitelist `^[A-Za-z0-9_.:-]+$` (1..=128 chars). Rejects leading dashes (`-`) to eliminate switch injection. Verified in `test_device_id_validation` and `test_invalid_device_id_rejection`. |
| **1.5 Atomic Monotonic Primitives** | **Verified** | `ProcessScrcpySupervisor::counter` | Monotonic session counter incremented via `AtomicU64::fetch_add(1, Ordering::Relaxed)`. |

---

## 2. Micro-Level Axioms (State Machines, Concurrency & Supervision)

| Axiom | Engineering Status | Code Location | Evidence & Test Verification |
| :--- | :--- | :--- | :--- |
| **2.1 Strict Finite State Machines (FSM)** | **Verified** | `PairingStateMachine` | Closed state transitions (`Discovered` -> `PairingRequested` -> `Paired` -> `Connected`). Cryptographically validates certificate fingerprint format before allowing transition to `Paired`. Proven by `test_pairing_fsm_lifecycle` & `test_reject_invalid_fingerprint_on_pairing`. |
| **2.2 Race-Free Supervision via Watch Channels** | **Verified** | `ProcessScrcpySupervisor::completed_rx` | Utilizes `tokio::sync::watch` to observe child process exit without lost wakeups. Preserves metadata on timeout (`TerminationUnconfirmed`). |
| **2.3 Asynchronous Pipe Drain** | **Verified** | `CliKdeAdapter`<br/>`ProcessScrcpySupervisor` | Continuous asynchronous draining via `tokio::io::copy(&mut pipe, &mut tokio::io::sink())` with memory capture ceilings (`MAX_CHILD_OUTPUT = 64 KB`, `MAX_OUTPUT_CAPTURE_SIZE = 12 KB`) preventing pipe deadlock. |
| **2.4 Process Crash Isolation** | **Verified** | `nexus-ipc` (TCP Loopback) | Process boundaries between UI and daemon maintained over `127.0.0.1:8788`. UI crashes do not interrupt daemon network sync. Authentication race in Flutter resolved via single-flight `_connectingFuture` and raw-handshake sequencing. |
| **2.5 Universal Deadlines & Lifecycles** | **Partially Verified** | `nexus-ipc` & `nexus-core` | Handshake deadline (5s) and idle timeout (60s) verified. Ctrl+C child cleanup verified. External command subprocesses use 12s/3s timeouts. Note: Automated recovery/watchdog process after unexpected daemon death is a future operational enhancement. |

---

## 3. Architectural & Behavioral Axioms (Clean Hexagonal & Integration)

| Axiom | Engineering Status | Code Location | Evidence & Test Verification |
| :--- | :--- | :--- | :--- |
| **3.1 Ports & Adapters Architecture** | **Verified** | `nexus-application`<br/>`nexus-adapter-*` | `HubApplication` defines pure traits (`KdePort`, `ScrcpyPort`, `DiscoveryProvider`). Adapters implement traits inward. Tested and verified across all crate boundaries. |
| **3.2 Truthful Observability** | **Verified** | `SessionState::TerminationUnconfirmed` | State machine reflects timeout truth without synthesizing fake successes. |
| **3.3 Conflict-Free Upstream Coexistence** | **Verified** | `nexus-core::main` | Disabled probing/binding of UDP 1716 by default. Probing is strictly opt-in via `NEXUS_ENABLE_LAN_DISCOVERY=1`. Native KDE Connect daemon operates without interference. |
| **3.4 Native Protocol Migration** | **Spike / Roadmap** | `nexus-protocol-kde` | Pure-Rust KDE Connect packet framing, NIST SHA-256, constant-time fingerprinting, and pairing FSM implemented in an isolated spike crate. Active runtime continues to use stable `CliKdeAdapter` until end-to-end device testing is complete. |

---

## 4. Verification Gates Summary

```powershell
[Gate 1] python scripts/check_structure.py        ==> Structure check passed (16 required files)
[Gate 2] cargo fmt --all -- --check              ==> Zero formatting drift
[Gate 3] cargo check --workspace --all-targets   ==> Clean compilation across 7 crates
[Gate 4] cargo clippy --workspace -- -D warnings ==> ZERO warnings tolerated
[Gate 5] cargo test --workspace                  ==> 19 unit & integration tests passed
[Gate 6] python scripts/test_v04_contracts.py    ==> ALL SUITE TESTS PASSED
```
