from pathlib import Path
root = Path(__file__).resolve().parents[1]
required = ['Cargo.toml','crates/nexus-core/Cargo.toml','crates/nexus-core/src/main.rs','crates/nexus-core/src/protocol.rs','crates/nexus-core/src/peers.rs','ui/lib/main.dart','docs/ARCHITECTURE.md','docs/THREAT_MODEL.md','docs/ROADMAP.md']
missing = [p for p in required if not (root / p).is_file()]
assert not missing, f'Missing: {missing}'
print('Structure check passed:', len(required), 'required files')
