# Release roadmap (dependency-ordered, no artificial dates)

| Gate | Outcome | Exit evidence |
|---|---|---|
| 0 | Prototype discovery and status UI | Parser/unit tests, bounded state, manual LAN smoke test |
| 1 | Secure local identity and IPC | Persistent keys, restricted OS IPC, no untrusted commands |
| 2 | KDE Connect interoperability | Real phone pair/verify/unpair tests; capability negotiation |
| 3 | Core productivity | Battery, ping, notifications, clipboard with permission tests |
| 4 | Media | Existing scrcpy subprocess first, live preview and recording with measured latency |
| 5 | Android companion | Kotlin permissions, lifecycle, notification listener; test OEM restrictions |
| 6 | File transfer and camera | Stream integrity, resume, camera permission UI, GPU pipeline |
| 7 | Calls and advanced audio | Bluetooth/telephony compatibility documented per OS |
| 8 | Release readiness | Multi-OS CI, fuzz/security audit, signed packages, legal notices |

## Optional advanced features (not committed)
Multi-device sessions; Android-Wear OS companion; system-tray controls; picture-in-picture; audio routing; virtual webcam; file drag-and-drop; E2EE nearby remote access; diagnostics exports; plug-in API; accessibility keyboard controls; QoS auto-tuning.

## Roadmap decisions
Reuse scrcpy as a managed subprocess before embedding codec/video transport. Avoid a second custom protocol until KDE interoperability has strong contract tests. Use flutter_rust_bridge only when typed bidirectional IPC has a clear benefit over the simpler API. On macOS/iOS, OS permissions and platform limits may block some feature parity.
