# Roadmap and acceptance gates

- [x] P0: Repo skeleton, discovery-only Rust daemon, read-only local status endpoint, Flutter shell, parser tests authored.
- [ ] P1: Stable machine identity, authenticated KDE Connect TLS handshake/pairing, interoperability tests with official Android peer.
- [ ] P2: Typed IPC / FRB, device state store, proper event stream, battery, ping, encrypted notifications.
- [ ] P3: Files, clipboard, secure transfer, resume, content hashes, explicit permission UX.
- [ ] P4: Android Kotlin app, Android permission flows, notification replies, scoped media, Doze/OEM tests.
- [ ] P5: ADB/scrcpy integration, hardware video, adaptive bitrate, input-control consent, Wi-Fi/USB switch.
- [ ] P6: Phone camera/mic with visible consent; virtual camera adapters (Linux/Windows/macOS separately).
- [ ] P7: SMS/calls when permitted by OS, accessibility, licensing, and app-distribution policies.
- [ ] P8: Reproducible builds, fuzz/security audits, signed installers, nightly OS/device compatibility.

Each gate requires at least one hardware end-to-end acceptance test, not just green unit tests.
