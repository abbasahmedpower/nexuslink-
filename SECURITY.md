# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0.0 | :x:                |

## Security Architecture & Threat Model

NexusLink adopts a zero-trust local isolation architecture:

1. **Per-User Authenticated IPC**:
   - Loopback TCP listener bound exclusively to `127.0.0.1`.
   - Per-user session authentication token generated via OS CSPRNG (`getrandom`).
   - Token stored in user-scoped runtime directories (`XDG_RUNTIME_DIR` / `%LOCALAPPDATA%` / `~/.nexus_hub/run`) with explicit access controls (`0600` on Unix; `icacls /inheritance:r /grant:r %USERNAME%:F` on Windows).
   - Insecure shared fallback to world-writable temporary directories (`/tmp`, `C:\Windows\Temp`) is strictly refused.
   - Constant-time XOR token verification prevents timing side-channel attacks.
   - Strict 5-second handshake deadline closes unauthenticated connections before resource starvation.
   - 60-second idle timeout prevents leaked socket connections.

2. **Managed Process Supervision**:
   - Upstream binaries (`kdeconnect-cli`, `scrcpy`) are executed with direct argument vectors (`std::process::Command`), strictly avoiding shell interpretation (`sh -c` or `cmd.exe /c`) to prevent command injection.
   - All input identifiers (device IDs, serial numbers) are validated against strict alphanumeric whitelists (`^[A-Za-z0-9_.:-]+$`). Leading dashes are rejected to eliminate argument-injection / switch confusion attacks.
   - Process standard output and standard error pipes are bounded (`MAX_OUTPUT_CAPTURE_SIZE = 12 KB`, `MAX_CHILD_OUTPUT = 64 KB`) and continuously drained to `/dev/null` (`tokio::io::sink()`) to eliminate unbounded buffer growth and pipe deadlock.
   - Processes are configured with `kill_on_drop(true)` to guarantee child termination even during unexpected runtime shutdown.

3. **Untrusted LAN Protocol Isolation**:
   - NexusLink does not expose open TCP servers to the local network.
   - LAN discovery is passive (receive-only) and non-blocking. It never broadcasts invalid identity records advertising non-existent TCP listeners, avoiding collision with official KDE Connect daemons.

## Reporting a Vulnerability

If you discover a security vulnerability in NexusLink:
- **Do not open a public issue.**
- Email the maintainers or report via GitHub Security Advisories.
- Include detailed reproduction steps and affected components.
- Maintainers will acknowledge receipt within 48 hours and work on a coordinated fix.
