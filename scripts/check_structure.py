from pathlib import Path
r=Path(__file__).resolve().parents[1]
required=['Cargo.toml','crates/nexus-core/src/main.rs','crates/nexus-core/src/protocol.rs','ui/lib/main.dart','docs/ARCHITECTURE.md','docs/NANO_MICRO_AUDIT.md','docs/ROADMAP.md','docs/REA_WORKFLOW.md','docs/QUALITY_GATES.md']
for path in required:
    assert (r/path).is_file(), path
print(f'Structure PASS ({len(required)} required files)')
