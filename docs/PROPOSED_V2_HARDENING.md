# NexusLink V2 Hardening & Architectural Evaluation (ADR-002)

## Context & Audit Summary

An initial set of high-level optimization requests was audited against the production codebase of NexusLink (`nexuslink-`). The findings confirm that several proposed changes would undermine critical architectural guarantees, while others reflect misconceptions about the existing runtime:

1. **Process Isolation vs. In-Process FFI (`flutter_rust_bridge`)**:
   - **Decision**: **Reject FRB for core daemon**. Retain OS Process Isolation between Flutter UI and Rust Core via authenticated loopback IPC (`127.0.0.1:8788`).
   - **Rationale**: Merging Flutter's Dart VM runtime into the same OS process as the Rust device coordination daemon eliminates **Crash Isolation**. A UI crash (e.g., rendering glitch, GC stall, platform channel unhandled exception) would terminate the network engine, drop active phone mirrors, and tear down background synchronizations. Process isolation guarantees that the daemon survives independently.

2. **Scrcpy Video Streaming Pipeline**:
   - **Finding**: Video frames do **not** traverse the IPC socket. Scrcpy runs as a supervised, managed child process rendering to its own hardware-accelerated native surface at 60 FPS.
   - **Decision**: Do not build in-tree SHM/DMA-BUF zero-copy pipelines until an in-window embedded video player is actively developed. Prematurely replacing managed subprocess execution introduces video stutter and sync bugs.

3. **Framing & Buffer Exhaustion Protection**:
   - **Status**: The IPC framing in `nexus-ipc` is already memory-bounded (`read_bounded_frame`). Sockets enforce a strict 64 KiB ceiling before allocating memory, constant-time XOR token authentication, a 5-second handshake deadline, and a 60-second idle timeout.

4. **KDE Connect Native Protocol Migration**:
   - **Strategy**: Phased evolution. Rather than breaking the working `kdeconnect-cli` wrapper, develop `nexus-protocol-kde` as an isolated pure-Rust protocol spike. Once verified (discovery, packet serialization, self-signed TLS pairing, fingerprint pinning), promote it to a native adapter.

## Incremental Evolution Roadmap

- **Phase 1 (Completed in v1.0.0)**: Hardened Hexagonal Core, race-free scrcpy supervisor, bounded pipe draining, secure IPC deadlines, CI workflow.
- **Phase 2 (Current Spike)**: `nexus-protocol-kde` pure-Rust protocol engine with zero external CLI dependencies.
- **Phase 3**: Optional high-throughput binary IPC framing alongside JSON wire protocol.
- **Phase 4**: In-window video rendering via direct `libscrcpy` / SDL texture bridge.
