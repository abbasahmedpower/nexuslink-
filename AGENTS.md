# Nexus Hub — engineering rules for Codex and Antigravity

## Mission and baseline
- Baseline is Nexus Hub v0.3. Inspect the actual repository and perform discovery before changes. Do not fabricate implementation, compatibility, benchmarks or test results.
- User-facing target: cross-platform desktop hub for trusted Android devices, using upstream scrcpy and KDE Connect. Target platforms Windows, Linux (including Fedora KDE/Wayland) and macOS with platform-specific capability negotiation.
- Develop in order: tasks/V0.4.md then V0.5.md then V0.6.md then V0.7.md. ONE phase per branch/PR. Do not attempt all phases in one change.
- Existing Flutter `ui/lib/backend.dart` launches processes and `ui/lib/main.dart` polls an unauthenticated 127.0.0.1:8787 endpoint. Treat these as migration targets, not permanent interfaces.

## Architecture invariants
- Hexagonal / ports-and-adapters: domain models independent from platform integrations; use typed request/response and error types; adapter implementations behind small interfaces.
- Rust owns subprocess lifecycle; UI must not invoke scrcpy, kdeconnect-cli, adb or the OS shell. Never interpolate untrusted arguments into a shell.
- Use local IPC with OS-controlled per-user access; do not expose unrestricted HTTP on TCP localhost. All commands typed, bounded, validated and permission checked. Prefer Unix domain sockets with filesystem ACLs on Unix, named pipes with proper security descriptors on Windows, or another justified local IPC mechanism; analyze transport-specific threat models. Avoid bearer tokens in logs. No remote IPC listener.
- Do not conflate KDE Connect IDs with ADB device serials. Cross-device mapping requires a verified, explicit user-approved binding.
- Never label devices as paired/trusted based only on UDP discovery. Preserve verified identity from KDE Connect pairing.
- No rooting, hidden recording, unattended camera/mic activation, or bypassing OS permissions. Foreground indicator and explicit user action for camera/microphone/capture.
- Prefer scrcpy maintained upstream executable initially; embedded media requires an API/sidecar architecture and measured validation, not scraping scrcpy windows or inventing a stable video IPC protocol.
- Memory/resource bounds (message frames, child stdout/stderr, queues, file sizes when relevant), cancellation deadlines, graceful shutdown, reconnection backoff and observability with sensitive data redaction are required.
- Follow upstream licenses and notices: scrcpy Apache-2.0; KDE Connect source contains GPL licensed files. Do not copy KDE code into proprietary modules without a license decision.

## Workflow per phase
1. Inventory files, actual build status, runtime/platform constraints and upstream interfaces; list evidenced facts and unknowns.
2. Write a short plan with interfaces, failure modes, non-goals and tests. Make incremental coherent changes; avoid massive rewrites.
3. Add unit, integration and failure-path tests, including malformed input, connection failures, child exits, timeouts, race conditions, cancellation and resource cleanup where applicable.
4. Run available checks (`cargo fmt --all -- --check`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `flutter analyze`, `flutter test`, platform build). Report exactly what ran; explicitly label blocked tests.
5. Document behavior and OS-specific support matrix; keep UI feature flags disabled unless backed by working functionality.
6. End with files changed, implemented / not implemented, tests, risks, and one Conventional Commit subject. Do not auto-push or release without approval.

## Quality gates
- Default-deny trust, principle of least privilege, no silent fallback to unencrypted paths.
- Keep process ownership and cleanup robust across crashes, cancelled start/stop, and daemon restarts. PID alone is not sufficient to assert process ownership.
- Metrics: FPS, frame time P50/P95/P99, end-to-end control latency, CPU, memory, reconnect time, per-feature error rates. Do not assert numerical targets met without recordings and platform/device methodology.
- Every feature must degrade gracefully for unsupported APIs / Android permissions / Linux Wayland / macOS restrictions.
- Keep changes readable, testable, with no unused abstractions. English code/docs/commits; concise Egyptian Arabic progress report if requested.
