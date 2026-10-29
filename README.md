# Nexus Hub — verified scope: discovery-only starter

A local-first desktop concept inspired by KDE Connect, scrcpy and Phone Link. This repo DOES NOT clone their functionality yet. It does not implement TLS, pairing, capture, SMS, notifications, or virtual camera. All peers are untrusted.

## Launch

1. Install Rust stable and Flutter stable on your OS.
2. From repository root: `cargo test --workspace` and `cargo run -p nexus-core`.
3. In a second terminal: `cd ui && flutter create --platforms=windows,linux,macos . && flutter pub get && flutter run -d windows` (replace windows with linux/macos).
4. Phone and desktop must be on same LAN. Allow UDP 1716 for discovery; TCP 8787 is bound to loopback only. Stop KDE Connect on the desktop if UDP port is occupied.
5. Check `http://127.0.0.1:8787/status` and UI. Note: protocol version and exact interop need verification against official peers. Discovery is best-effort, not guaranteed.

## Security warnings

- The HTTP status API uses loopback, no authentication. It is not suitable for remote exposure; do not port-forward 8787.
- UDP identities are spoofable. The current discovery list proves no ownership or encryption.
- The development ID is static: set a persistent unique `NEXUS_DEVICE_ID` for local tests; production must generate, persist, and protect identity keys and use authenticated pairing.
- Do NOT advertise real KDE Connect capabilities until the complete TLS/pairing/channel implementation is ready.

## Design documents

See `docs/ARCHITECTURE.md`, `docs/NANO_MICRO_AUDIT.md`, `docs/ROADMAP.md`, `docs/REA_WORKFLOW.md`, and `docs/QUALITY_GATES.md`.
