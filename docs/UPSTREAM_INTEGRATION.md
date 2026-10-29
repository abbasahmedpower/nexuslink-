# Upstream integration decision (2026-10-29)

## Two integration boundaries
1. `scrcpy`: launch the independently installed Apache-2.0 executable as an owned subprocess, with fixed, vetted arguments. Supports screen and camera mode. Do not fork/synchronize its native server until a profiler demonstrates a requirement.
2. `KDE Connect`: invoke `kdeconnect-cli` installed from KDE using documented command switches. Pairing and TLS are performed **by KDE Connect**, not Nexus's discovery-only Rust daemon. Do not assume device discovery establishes identity.

These integrations rely on executable discovery through PATH and upstream installs. They are not source-level linkages, and the code is not yet merged into one media engine. Commands use `Process.run` / `Process.start`, with `runInShell:false` to avoid shell command injection. Process startup reports errors, and UI shows results; `scrcpy` video currently appears in its own native window. Existing initial Rust daemon is not hardened enough to be a production security boundary.

## License gates
- scrcpy: Apache License 2.0; retain copyright/NOTICE obligations for redistribution.
- KDE Connect: files may be GPL-2.0-only OR GPL-3.0-only or KDE-approved GPL; audit specific file SPDX expressions when embedding code. Running an installed CLI as a separate process is intentionally different from copying and linking its GPL code.
- No '10% change' exemption exists.
- For packaged releases audit licenses, trademarks, copied assets, dependencies and distribution model with counsel.

## Performance, behavior and nano issues
| Failure | Mitigation | Follow-up verification |
|---|---|---|
| executable missing | visible ProcessException | clean install on Fedora and Windows |
| shell injection | typed argument list; no shell | invalid path and device ID tests |
| stale video process | track handle, graceful close, timeout | exit while streaming |
| stuck CLI | 12s timeout | slow dbus backend and hung process |
| invalid pairing identity | leave TLS and trust store to KDE | pair + fingerprint verification on two physical devices |
| USB/ADB device ambiguity | explicit serial optional | several attached Android phones |
| dropped link | report error; avoid fake state | unplug/replug, airplane mode |
| buffer use / frame queues | upstream scrcpy process | profile P95/P99 input-to-photon and peak RSS |
| permission-sensitive camera | explicit user action, app-supported APIs | Android 12+ tests |
| dependency drift | specify verified versions for release | CI version matrix |

## Critical limitations
- `Process.run().timeout()` is not guaranteed to terminate a stuck child; production adapter needs explicit PID ownership and termination on timeout.
- The desktop UI currently requests KDE devices and controls scrcpy directly, while Rust discovery remains independent. Future step: move command supervision to a dedicated Rust IPC daemon using per-user named pipes/Unix sockets, authenticated IPC and structured events.
- Windows KDE CLI executable packaging and PATH need real deployment validation.
- Do not claim 120 FPS, zero latency, zero-copy, peer authentication, or production readiness absent measured evidence.
