# Using Nexus Hub with Codex or Antigravity

This workspace is the **existing v0.3 prototype** plus agent instructions and four implementation specifications. It **does not contain completed v0.4–v0.7 implementation**. Read `README.md` and `AGENTS.md` before coding.

## Start with version control

```bash
git status
git switch -c feat/v0.4-secure-ipc
```

Only initialize the repository if it was not already initialized; preserve existing Git history.

## Codex
Run `codex` in this project directory, or use Codex IDE with this workspace. Paste this prompt:

> Read AGENTS.md, README.md and tasks/V0.4.md. Audit the current code first, propose your small implementation plan, and implement ONLY v0.4. Run real tests, be explicit about platform limitations, and finish with the Conventional Commit and a list of changes. Do not push to GitHub.

Do not use full-access override flags. With CLI, select workspace-write and on-request approvals as supported by your Codex version.

## Antigravity
Open the folder as a workspace, then paste the identical task prompt. Antigravity recognizes root-level `AGENTS.md` as workspace rules. Use task review before approving irreversible changes.

## After each milestone

- Check `git diff` and validate the artifacts.
- Run real build, lint, tests and manual hardware checks. Only report tests that actually ran.
- Commit the scoped change on its branch and create PR. Merge only after review and checks.
- Create new branch for next milestone. Prompt the agent with corresponding `tasks/V0.x.md`.

## Baseline facts
- Current `ui/lib/backend.dart` calls executables directly from Flutter.
- Current `ui/lib/main.dart` queries an unauthenticated localhost status API and runs a GUI shell.
- scrcpy upstream license Apache-2.0; KDE Connect source files include GPL terms.
- macOS/Windows/Linux hardware and permission behavior must be verified separately.
