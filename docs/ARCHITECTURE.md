# Architecture decision record

## Goals
Local-first; minimum privilege; low idle resource use; predictable process ownership; multi-platform UI; independently replaceable transports; no false connection status.

## Layers
1. `domain`: device identities, capabilities, states and errors; no platform APIs.
2. `application`: discovery, pairing orchestration, transfer/capture use cases.
3. `adapters`: UDP/TCP/TLS, KDE compatibility, WebRTC, codec, USB, OS services.
4. `presentation`: Flutter state/UI; use local IPC/FRB after authentication design.
5. `android`: Kotlin foreground/user-consented integration and MediaProjection.

## States
UNKNOWN -> DISCOVERED_UNTRUSTED -> PAIRING_PENDING -> PAIRED -> CONNECTED -> DEGRADED/OFFLINE -> RECONNECTING. Revoke trust -> BLOCKED. Never treat discovery as pairing.

## Contracts
Versioned, size-bounded, explicitly typed command/event messages. Correlation IDs, cancellation, deadlines, backpressure. Separate control channel and media channel. Never pass arbitrary file paths into privileged services.

## ADRs
ADR-001: Rust for system core, Flutter for UI, Kotlin for Android APIs.
ADR-002: KDE Connect standard interoperability is opt-in and maintained as an isolated adapter, not a fork of core logic.
ADR-003: Separate scrcpy integration adapter from the product core, respecting its licensing and server/ADB constraints.
ADR-004: REA is a development-time analysis tool, not a runtime dependency.
ADR-005: No cloud or telemetry by default. Explain OS permissions to the user at point of use.
