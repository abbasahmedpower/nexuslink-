# Quality gates

Rust: `cargo fmt --all -- --check`, `cargo clippy --workspace -- -D warnings`, `cargo test --workspace`.
Flutter: `flutter analyze`, `flutter test`, widget snapshots + accessibility tests.
CI: Windows / Fedora / macOS; Android physical device matrix; dependency/SBOM and license audit.
Performance: record CPU %, RSS/PSS, idle battery, discovery latency, media glass-to-glass p50/p95/p99, dropped frames, cold/warm starts. Define target hardware and scene each run.
Safety: parser fuzz, TLS/pairing negative cases, replay, downgrade, invalid certificates, spoofed identities, path traversal, malformed filename, cancellation at every stage, missing camera permissions.
Release: no simulated state, no insecure default network API, no unreviewed third-party license obligations, signed artifacts and rollback plan.
