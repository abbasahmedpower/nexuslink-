# Nexus Hub Architecture Decision Record (ADR-001)

## Scope and verified boundary
This revision is a read-only, discovery-only *prototype*. Its UDP identity packets are untrusted. No KDE Connect authenticated sessions, phone control or media pipeline exist.

## Production target
```
Flutter/Desktop GUI    <---versioned IPC ---> Rust application coordinator
                                              |-- Device registry / state machine
                                              |-- Identity + trust store
                                              |-- KDE-compatible transport adapter
                                              |-- Nexus-native transport adapter
                                              |-- scrcpy integration adapter (isolated process)
                                              |-- Notification/clipboard/file services
                                              |-- Platform adapters (Windows/Linux/macOS)
Android Kotlin app <--- authenticated sessions ---> core services
REA (developer-only) ---> evidence collection, analysis, regression investigation
```

### Dependency rule
Domain models have no network/Flutter/Android dependencies. Use cases depend on domain and interfaces; transport/media/platform adapters depend inward. `main` wires implementations together. GUI must not read protocol packets directly.

### Where the starter differs
`main.rs` still coordinates several concerns and the Flutter UI is intentionally one file. Before feature growth: split UI into features, replace ad hoc HTTP with OS-specific authenticated IPC, move transports to adapters, and add persisted keys under OS secure storage.

### Principles
- Fail closed: no command executes without explicit authenticated pairing and capability authorization.
- Explicit states: Discovered != Paired != Connected != FeatureReady.
- Bounded allocations and queues; no infinite retry or uncontrolled subprocesses.
- One owner per resource: connection, decoder, camera, clipboard and subprocess.
- Clean-room interoperability; avoid copying protected UI artwork or proprietary binaries.
- Track evidence quality separately: observed, source-derived, inferred, unknown.
