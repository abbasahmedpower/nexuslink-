# Nano/Micro audit — initial engineering review

Status: risk assessment, NOT confirmed defects in a deployed product.

| Layer | Failure mode | Risk | Design safeguard | Verification |
|---|---|---|---|---|
| Nano / parsing | Malformed or oversized UDP datagram | DoS | bounded datagram and typed parser | fuzz parser + property tests |
| Nano / network | UDP identity spoofing | false peer | treat discovered as untrusted | spoofed sender test |
| Nano / transport | Partial TCP frames | decode corruption | length/framing limits, loop until complete | fragmented packet tests |
| Nano / crypto | Auto-trust certificates | impersonation | explicit pairing + pinning + key rotation | MITM negative tests |
| Nano / lifecycle | Orphaned ADB/media process | memory/process leaks | supervised children, cancellation & reaping | kill midstream |
| Nano / media | Out-of-order video frames | latency | bounded jitter buffer, drop stale frames | loss/jitter benchmark |
| Nano / display | Copies through Flutter UI thread | stutter | external texture and hardware decoder | p95 frame time benchmark |
| Micro / feature | Real-time UI reports fake success | trust erosion | typed connected-state machine | integration tests |
| Micro / permission | Background Android restrictions | missed notifications | consent + foreground service where permitted | Android OEM matrix |
| Micro / files | Path traversal and overwrite | data loss | canonical safe destination, confirmation | adversarial filenames |
| Micro / privacy | Cached messages kept forever | exposure | explicit retention TTL and encryption policy | lifecycle audit |
| Micro / UX | No offline/reconnect feedback | confusion | clear status, manual refresh, retry strategy | usability tests |
| Micro / updates | OS plugin API changes | broken features | adapter/feature flags, compatibility matrix | cross-platform CI |
| Micro / legal | Bundling GPL components improperly | licensing exposure | license inventory and distribution review | SBOM + NOTICE review |

## Non-promises
No fixed 150 MB memory target, zero latency, universal 120 FPS, or 100% device compatibility is credible without platform-specific measurement.
