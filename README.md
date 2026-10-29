# Nexus Hub v0.2 — source prototype + engineering audit

**Truthful status:** Rust discovery daemon + Flutter status GUI; it does NOT yet pair with phones, mirror screens, control Android, offer live notifications, messages, calls, mic or camera. No compiled installer included.

## Development setup
Requires current stable Rust/Cargo and Flutter SDK. Confirm your OS desktop build prerequisites via Flutter's official install guide. This environment did not contain Rust/Flutter, so compilation was NOT verified here.

```bash
python scripts/check_structure.py
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p nexus-core
```

Run GUI in another terminal:
```bash
cd ui
flutter create --platforms=windows,linux,macos .
flutter pub get
flutter run -d windows  # or linux / macos
```

Port UDP 1716 is for discovery, not a completed implementation of KDE Connect. Stop another desktop KDE Connect process if there is a port conflict. `127.0.0.1:8787/status` is a read-only, **unauthenticated developer-only** endpoint — do not port-forward. All LAN identities are spoofable, never a trusted connection.

## Changes since v0.1
- Split protocol/peers/daemon logic and added peer-store unit tests.
- Bounded peer store to 128 and TTL to 90 seconds.
- Bounded concurrent local status requests to 32 with IO deadlines.
- Removed broad `Access-Control-Allow-Origin: *` response header.
- Sorted device snapshots for deterministic UI behavior.
- Ephemeral device ID by default, instead of a shared fixed development identifier.

## Documents
- `docs/ARCHITECTURE.md` clean layering and future components
- `docs/THREAT_MODEL.md` risk register at nano/micro/system levels
- `docs/ROADMAP.md` evidence-driven release gates
- `docs/REA_WORKFLOW.md` how to use REA as analysis tooling

## Licensing
Nexus Hub starter code authored for this task. No upstream KDE Connect, scrcpy or REA source copied into this archive. Check obligations before redistributing integrations.
