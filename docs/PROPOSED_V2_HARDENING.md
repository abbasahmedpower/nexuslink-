# Proposed v2 Hardening — Nano/Micro Roadmap (Evidence-Based Review)

Status: **proposal only**. Nothing in this document has been implemented.
Each item is classified against the actual code on `main` (v1.0.0, hexagonal
architecture: `nexus-domain`, `nexus-application`, `nexus-adapter-kde`,
`nexus-adapter-scrcpy`, `nexus-ipc`, `nexus-core`).

The external review request contains several claims that do not match this
codebase (e.g., "notification JSON hot path", "Scrcpy frames over IPC",
"PeerRegistry behind RwLock contention"). Where a claim is already mitigated
or factually wrong for this repo, it is marked accordingly. No speculative
performance numbers ("5ms → 0.001ms", "1% CPU at 4K/120FPS") are adopted as
targets; all would require platform-specific benchmarks before being stated.

---

## 1. Replacing JSON IPC with `flutter_rust_bridge` (in-process FFI)

**Claim:** JSON-over-socket costs ~5ms per call; FRB reduces latency to
~0.001ms by passing memory pointers directly.

**Facts about the current code (`crates/nexus-ipc/src/lib.rs`):**
- The UI process and daemon are **separate processes**. This is deliberate:
  a Flutter crash must not take down the network engine (see README /
  ARCHITECTURE.md).
- Framing is line-delimited with a hard bound of `MAX_IPC_FRAME_SIZE = 64 KiB`
  enforced *before* parsing (`read_bounded_frame`), plus constant-time token
  auth, handshake deadline (5 s), idle timeout (60 s), max 8 clients, and
  outbound frame-size checks. Lagging event subscribers receive an explicit
  `ResyncRequired` notification instead of silent drops.
- Payloads are small control messages (device lists, session state). There is
  no notification flood or video traffic on this channel today.

**Verdict:** Partially valid, but the quoted numbers are unsubstantiated and
the move conflicts with the crash-isolation requirement.

**Proposal if pursued (spike first):**
1. Keep the socket IPC as the default transport.
2. Prototype FRB behind a feature flag **only** if measured end-to-end control
   latency exceeds budget (measure with real harness, not assumed baselines).
3. Accept explicitly that embedding Rust in the Flutter process removes crash
   isolation; mitigate with a watchdog-supervised daemon restart story.
4. Do not claim latency improvements without before/after p50/p95/p99 data.

## 2. Zero-copy video path (SHM / DMA-BUF / hardware decode)

**Claim:** Passing Scrcpy 1080p@60–120fps frames over Unix sockets destroys
CPU performance; build a shared-memory texture pipeline.

**Facts:** The current `nexus-adapter-scrcpy` supervises the Scrcpy *binary*
as a child process (race-free kill/reap design); **no frame bytes ever pass
through the IPC layer**. The premise of a "video IPC bottleneck" does not
apply to this architecture today.

**Verdict:** Not a current defect. It becomes relevant only if we implement
our own decoder/renderer instead of launching Scrcpy.

**Proposal if pursued:**
1. Design doc first: external-texture contract between a future native
   renderer and Flutter (`Texture` API), never frames over control IPC.
2. Linux: DMA-BUF or memfd-backed ring buffer; Windows: DXGI shared surface.
   Pin the frame-ring size and measure copies eliminated.
3. Hardware decode (VA-API/DXVA2/VideoToolbox via FFmpeg) gated behind
   capability detection with a software fallback.
4. Success criteria defined as measured p95 frame time and CPU %, not
   marketing targets.

## 3. Actor-model refactor of `PeerStore` (remove lock contention)

**Claim:** `Arc<RwLock<T>>` around peer tables blocks cores during concurrent
connect/message events.

**Facts:** `PeerStore` (`crates/nexus-core/src/peers.rs`) is guarded by a
std `RwLock` used only from the single UDP discovery task; reads happen once
per `GetDiscoveryPeers` request. Tables are bounded (`MAX_PEERS = 128`,
90 s TTL). There is no multi-core write contention pattern in the code.

**Verdict:** Premature optimization. A full actor rewrite adds complexity
with no demonstrated win.

**Proposal if profiling ever shows a hotspot:** migrate that store to an
`mpsc` command-channel actor (state owned by one task, message passing,
no locks). Requires tests proving identical prune/snapshot semantics. Until
then: keep as-is.

## 4. Length-prefixed framing in IPC

**Claim:** Stream sockets have no message boundaries; without length prefixes
parsers merge packets (coalescing) and are vulnerable to buffer overflow.

**Facts:** Line-delimited JSON **is** a complete framing scheme here:
newline-bounded, UTF-8 validated, and strictly size-bounded before parse.
Coalescing cannot corrupt a frame because readers stop at `\n`; oversized
frames are rejected with `PayloadTooLarge`.

**Verdict:** Already mitigated. Binary length-prefix framing (u32 LE + body)
is a reasonable *future* step bundled with a binary codec migration, not a
security fix. If done, keep the same hard size cap and add golden wire tests.

## 5. Delta sync / event sourcing instead of full snapshots

**Claim:** Sending `NexusStateSnapshot` on every reconnect wastes bandwidth.

**Facts:** No such snapshot exists in the codebase. Event delivery is already
incremental (`SessionEvent` broadcasts); staleness is handled by an explicit
`ResyncRequired` notification telling the client to re-fetch — which is the
correct, simple reconciliation primitive at this payload scale.

**Verdict:** Invalid premise today. Revisit only if device/notification lists
grow large enough that full refetch measurably hurts; then add sequence
numbers + delta events with resync-on-gap semantics.

## 6. TLS 1.3 + certificate pinning in `nexus-adapter-kde`

**Claim:** KDE Connect handshake is MITM-vulnerable; enforce strict TLS 1.3
with pinned self-signed certificates via `rustls`.

**Facts:** Our KDE adapter currently shells out to `kdeconnect-cli`
(`CliKdeAdapter`). All transport security is delegated to the upstream KDE
Connect daemon; our code performs no TLS itself and stores no keys. We also
validate device IDs strictly (`is_valid_device_id`) and treat LAN-discovered
peers as untrusted until paired.

**Verdict:** Valid long-term direction, but it means implementing our own
KDE Connect protocol stack (identity, pair, packet channels) — a major
project, not a drop-in patch. Also note: real KDE Connect peers negotiate
TLS 1.x with pinned self-signed certs; forcing TLS 1.3-only can break
interoperability with older phones, so version policy must be tested against
real devices.

**Proposal if pursued:**
1. Spike crate `nexus-adapter-kde-native` using `rustls` + `rcgen`:
   identity exchange, pairing confirmation UI flow, SHA-256 fingerprint
   pinning store, reject unknown fingerprints (MITM negative tests required).
2. Threat model update (`docs/THREAT_MODEL.md`) covering key storage,
   pairing DoS, and downgrade policy.
3. Keep `CliKdeAdapter` as fallback until native stack passes device matrix.

## 7. Pairing handshake (fingerprint exchange & persistence)

Depends on item 6 (there is nothing to pin until we own the TLS layer).
Design outline when implemented: temporary restricted channel → identity →
pair request → user confirmation with fingerprint shown → persist pin →
reload verifier. Storage choice (SQLite/Sled) must be decided with backup,
migration, and file-permission requirements documented.

---

## Explicit non-goals / anti-hype notes

- No adoption of unverified benchmark claims (latency figures, CPU %, FPS).
  Every performance statement needs a reproducible measurement plan.
- No `lazy_static` global mutable state in an FFI bridge; state ownership
  belongs to the daemon/actor tasks.
- The sample codes circulated with the review reference APIs that do not
  exist here (`send_sms`, `AppNotification`, `NexusStateSnapshot`,
  `db::save_paired_device`); they cannot be merged as-is.
- Crash isolation (UI separate from daemon) remains a hard architectural
  requirement unless explicitly revoked with a supervision design.

## Suggested execution order (if approved)

1. Measurement harness for current IPC (baseline p50/p95/p99). *(cheap)*
2. Native KDE TLS spike (item 6) — highest security value.
3. Video-path design doc (item 2) — only if we commit to custom rendering.
4. FRB spike (item 1) — only if baseline shows the socket is the bottleneck.
5. Everything else: deferred until profiling justifies it.
