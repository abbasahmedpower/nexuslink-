# REA — research-only integration

Official source: https://github.com/morluto/rea ; published npm package: rea-agents ; MIT license (verify when updating).

Install separately (from a trusted directory) with Node version compatible with that release: `npx rea-agents setup`. Review setup changes before approving. REA is not packaged in this starter.

Use REA to collect evidence from executables you own or are authorized to inspect. Never confuse observed evidence with hypothesis. Respect product licenses and platform restrictions; do not copy closed-source UI assets or bypass access controls.

Investigation template:
- Question: which user-visible behavior or interoperability gap?
- Target/version/OS/hash and scope of permission.
- Static evidence: code references, strings, packet captures (sanitized).
- Dynamic evidence: trace under controlled testing (if authorized).
- Finding confidence: observed / inferred / unknown.
- Clean-room specification: inputs, outputs, error cases, side effects.
- Minimal reproduction and regression test.
- Implementation in isolated adapter; verify against official API/protocol.

Example authorized target tasks: inspect YOUR app's Electron IPC handlers; determine how YOUR APK handles media lifecycle; investigate crashing binary YOU are permitted to analyze. REA does not itself build Nexus Hub.
