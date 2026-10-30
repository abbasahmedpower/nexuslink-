# Nexus Hub v0.4 - Hardened IPC & Managed Integrations

A robust, minimal **desktop GUI integration** of the existing `scrcpy` and KDE Connect executables. It combines process supervision, bounded local IPC authentication, and unified device integration without rewriting mature protocols.

## What works
- List paired/reachable KDE Connect devices via `kdeconnect-cli`
- Request pairing and send ping via KDE Connect
- Share a file/URL securely through KDE Connect
- Launch/stop official `scrcpy` screen streaming with full state-machine process supervision
- Launch/stop official `scrcpy` camera streaming
- Authenticated, bounded local TCP IPC (Bearer token auth, CSPRNG, constant-time compare, 64 KiB frame limits)
- UDP discovery daemon with bounded untrusted peer caching (MAX_PEERS=128 + TTL 90s)
- Discovery query exposed via IPC (`get_discovery_peers`) with unauthenticated HTTP 8787 disabled by default

These calls are wired into the Flutter UI, not demo buttons. **Not implemented:** embedded video in the window, real native TLS pairing, notifications reading, SMS, virtual camera on Windows, Android companion app, releases, or benchmarked performance.

## Installation
Install current official `scrcpy` and `kdeconnect` packages compatible with your OS, plus Flutter 3.x and Rust. Ensure commands `scrcpy` and `kdeconnect-cli` are on PATH. In the `ui` directory:

```
flutter create --platforms=windows,linux,macos .
flutter pub get
flutter run -d linux  # on Windows: -d windows
```

Set up Android USB debugging and authorized ADB on your own device. Open official KDE Connect on phone and desktop for discovery/pairing; accept requests on both ends. `kdeconnect-cli` depends on the KDE Connect daemon and its environment. Only launch one Rust UDP discovery process if you need that separate view:

```
cargo test --workspace
cargo run -p nexus-core
```

See `docs/UPSTREAM_INTEGRATION.md` for license review, threat modeling, and next steps. upstream: https://github.com/Genymobile/scrcpy and https://github.com/KDE/kdeconnect-kde
