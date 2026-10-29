# Nexus Hub v0.3 - upstream integration prototype

A real, minimal **desktop GUI integration** of the existing `scrcpy` and KDE Connect executables. It adds useful behavior to the previous Rust discovery-only project without importing vendor code or rewriting well-maintained core protocols.

## What works (assuming installed tools)
- List paired/reachable KDE Connect devices using `kdeconnect-cli --list-available --id-name-only`
- Request pairing or send ping via official KDE Connect
- Share a file/URL through KDE Connect
- Launch/stop official scrcpy screen streaming as an owned child
- Launch/stop supported scrcpy camera streaming
- Optional separate Rust discovery daemon with untrusted peer listing

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
