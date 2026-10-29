# Nano/Micro Threat Model and Defect Audit

| Layer | Failure | Existing protection | Remaining required controls |
|---|---|---|---|
| Nano | Forged UDP identity | Untrusted indicator; parser validation | Device pairing TLS identity verification, explicit user confirmation |
| Nano | Oversized packet | Fixed 8 KiB UDP buffer, parser size check | Fuzzing and protocol compatibility matrix |
| Nano | Peer-table memory growth | MAX_PEERS 128, 90s TTL | Per-source throttles and interface filters |
| Nano | Concurrent API resource pressure | 32 concurrent API workers; 2s IO deadline | Authenticated IPC; explicit request size and parsing bounds |
| Nano | Malformed HTTP method | Endpoint exact prefix and 404 fallback | Replace ad hoc server with audited IPC/HTTP library |
| Micro | Stale connected status | TTL pruning and untrusted display | Authenticated heartbeat-driven state machine |
| Micro | Media stalls | Not implemented | PTS timestamps, backpressure, bounded queues, GPU fallback |
| Micro | Reconnect storm | Not implemented | Jitter, exponential backoff, generation IDs |
| Micro | Privilege escalation | No control commands implemented | Scope permissions by feature and peer, opt-in toggles |
| Micro | Device ID persistence | Ephemeral by default | Persistent cryptographic identity on secure storage |
| Micro | Local API privacy | Loopback-only, read-only, no CORS header | OS credential-restricted socket or named pipe, no port binding |
| System | Wayland / Windows API differences | Not implemented | OS-specific adapters and integration CI |
| System | Battery drain on Android | Not implemented | Foreground service policy and adaptive scheduling |
| System | License failure | No third-party code vendored | License inventory and distribution compliance |

## Important open risks
This prototype's loopback API is still unauthenticated: a local program can read the discovered peer names and IPs. It must not be enabled outside a trusted development session. Discovery can be spoofed on LAN, and the advertised TCP port is NOT served as a compatible KDE TLS endpoint. Do not claim the tool is fully interoperable.

## Acceptance gates before 1.0
1. Pairing identity challenge and pinned TLS certificates across desktop/Android, tested with hostile peers.
2. Fuzz packet and frame parsers; timeouts, reconnect, corruption, and restart tests.
3. No unbounded queues or lingering child processes after cancellation.
4. P50/P95/P99 latency, peak RSS, idle CPU and battery measured on each supported OS and device.
5. Clipboard, SMS, camera, mic require individual user-granted permissions.
6. SBOM, vulnerability checking, signed builds and reproducible release procedure.
