# Contributing to NexusLink

We welcome contributions to NexusLink! Follow these guidelines to maintain code quality, security, and architectural boundaries.

## Architecture Guidelines

NexusLink enforces a clean Hexagonal / Ports & Adapters architecture:
- `nexus-domain`: Pure domain contracts, error codes, and validation. Zero framework dependencies.
- `nexus-application`: Port definitions (`KdePort`, `ScrcpyPort`, `DiscoveryProvider`) and application orchestration.
- `nexus-adapter-kde`: Implementation of `KdePort` managing `kdeconnect-cli`.
- `nexus-adapter-scrcpy`: Implementation of `ScrcpyPort` providing race-free process supervision.
- `nexus-ipc`: Secure authenticated loopback transport and wire protocols.
- `nexus-core`: Daemon composition and graceful runtime lifecycle.
- `ui`: Flutter cross-platform user interface communicating via IPC.

Adapters MUST depend on `nexus-application` and `nexus-domain`; `nexus-application` MUST NEVER depend on concrete adapters.

## Development Workflow

1. Fork and clone the repository.
2. Ensure you have the required prerequisites:
   - Rust 1.75+ (stable)
   - Python 3.10+
   - Flutter 3.19+ (for UI development)
   - `kdeconnect-cli` & `scrcpy` (for end-to-end integration)
3. Create a feature branch:
   ```bash
   git checkout -b feat/your-feature-name
   ```
4. Run verification gates before submitting a PR:
   ```bash
   # Code formatting
   cargo fmt --all -- --check

   # Architectural boundaries
   python scripts/check_structure.py

   # Compiler and clippy checks
   cargo check --workspace --all-targets
   cargo clippy --workspace --all-targets -- -D warnings

   # Test suites
   cargo test --workspace
   python scripts/test_v04_contracts.py
   ```
5. Submit a pull request with a descriptive title adhering to Conventional Commits.
