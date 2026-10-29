from pathlib import Path
root = Path(__file__).resolve().parents[1]
required = [
    'Cargo.toml',
    'crates/nexus-core/Cargo.toml',
    'crates/nexus-core/src/main.rs',
    'crates/nexus-core/src/protocol.rs',
    'crates/nexus-core/src/peers.rs',
    'crates/nexus-domain/src/lib.rs',
    'crates/nexus-adapter-kde/src/lib.rs',
    'crates/nexus-adapter-scrcpy/src/lib.rs',
    'crates/nexus-application/src/lib.rs',
    'crates/nexus-ipc/src/lib.rs',
    'crates/nexus-ipc/src/contract.rs',
    'ui/lib/main.dart',
    'ui/lib/backend.dart',
    'docs/ARCHITECTURE.md',
    'docs/THREAT_MODEL.md',
    'docs/ROADMAP.md',
]
missing = [p for p in required if not (root / p).is_file()]
assert not missing, f'Missing: {missing}'
print('Structure check passed:', len(required), 'required files')
