# NexusLink Threat Model & Security Audit (v1.0.0)

## 1. Trust Boundaries & Attack Surfaces

```
[ Untrusted Local Network (LAN) ]
              │ (Untrusted UDP packets)
              ▼
[ Passive LAN Receiver ] ──(Bounded memory & TTL)──> [ PeerStore ]
                                                          │
──────────────────────────────────────────────────────────┼───────────────
[ OS Local User Security Boundary ]                       │
                                                          ▼
[ Host Applications ] ──(Requires Bearer Token)──> [ nexus-ipc (127.0.0.1:8788) ]
                                                          │
                                                          ▼
                                                  [ HubApplication ]
                                                          │
                                         ┌────────────────┴────────────────┐
                                         ▼                                 ▼
                              [ kdeconnect-cli ]                       [ scrcpy ]
```

## 2. Threat Analysis & Mitigations

| Threat | Vector | Severity | Mitigation in v1.0.0 |
| :--- | :--- | :--- | :--- |
| **Command Injection** | Malicious device ID or serial via IPC | **Critical** | Direct argument vectors (`Command::args`), strictly bypassing OS shells. Strict whitelist validation: `^[A-Za-z0-9_.:-]+$` (max 128 chars). Rejects leading dashes (`-`) to prevent switch confusion. |
| **IPC Authentication Bypass** | Rogue local process accessing IPC port | **High** | Loopback binding only (`127.0.0.1`). 32-byte CSPRNG token (`getrandom`) saved with strict user ACLs (`0600` on Unix; explicit user-only ACE on Windows). Refuses shared temp directory fallbacks. Constant-time XOR token verification prevents timing side-channels. |
| **Denial of Service (Memory)** | Client sending unclosed lines or huge frames | **High** | `read_bounded_frame` checks line lengths iteratively during buffer filling. Strictly rejects frames exceeding `MAX_IPC_FRAME_SIZE` (64 KiB) before allocating target memory. |
| **Connection Starvation** | Unauthenticated client holding TCP socket | **Medium** | Strict 5-second handshake deadline: unauthenticated sockets are terminated immediately. 60-second idle timeout drops abandoned client sockets. |
| **Process Pipe Deadlock** | Verbose child stderr/stdout filling OS buffer | **High** | Asynchronous background draining: `tokio::io::copy(&mut pipe, &mut tokio::io::sink())` continuously drains uncaptured child output to `/dev/null`. Memory capture bounded to `MAX_OUTPUT_CAPTURE_SIZE` (12 KB) and `MAX_CHILD_OUTPUT` (64 KB). |
| **Orphaned Child Processes** | Daemon crash or abrupt process exit | **High** | Child spawned with `kill_on_drop(true)`. Unified `ShutdownCoordinator` traps `Ctrl+C` and cleanly stops active sessions within 3 seconds. |
| **KDE Port Collision** | Port conflict on UDP 1716 with kdeconnectd | **High** | Removed port collision: passive UDP bind fails gracefully and defers to `kdeconnect-cli`. Eliminated fake identity broadcast with nonexistent TCP port. |
| **Timing Attack on Token** | Statistical timing variance on string equality | **Low** | Bitwise XOR accumulator compare ensures constant-time verification regardless of match position. |

## 3. Residual Risks & Roadmap Mitigations
1. **Plaintext Local Loopback IPC**: While bound to `127.0.0.1` and secured by per-user Bearer tokens, future revisions (v1.1) will provide OS-native Unix domain sockets (on Linux/macOS) and Named Pipes with security descriptors (on Windows).
2. **Third-Party Upstream Binaries**: NexusLink relies on the integrity of the installed `scrcpy` and `kdeconnect-cli` executables. Future packaging will verify digital signatures / SHA256 hashes of bundled binaries.
