# Repository agent guidance

This file applies to every coding agent working in this repository, regardless of vendor or model.

## Environment

- Run all Rust, build, test, lint, formatting, dependency, audit, and fuzz commands in WSL 2. Do not execute them with Windows-native Rust tooling.
- From Windows, invoke commands with `wsl.exe bash -lc 'cd /mnt/d/dev/wasi_pg_client && <command>'`.
- Keep source edits portable across Windows and Linux. Do not introduce machine-specific absolute paths into committed files.

## Dependency updates

- Read and follow `.agents/skills/update-rust-dependencies/SKILL.md` before auditing or changing dependencies.
- Preserve the workspace MSRV declared by `workspace.package.rust-version` unless the user explicitly approves changing it.
- Treat WASI Preview 2 (`wasm32-wasip2`) compatibility as a release requirement.
- Prefer workspace-managed dependency declarations when a dependency is shared by multiple members.
- Update `Cargo.lock` with the manifests and never edit the lockfile manually.
- Separate routine compatible upgrades from breaking major-version or backend replacements; explain migrations and compatibility tradeoffs.
- Do not overwrite unrelated user changes. The working tree may already be dirty.

## Required validation

Run the checks selected by the dependency-update skill in WSL. At minimum, validate formatting, workspace compilation, library tests with all features, the WASI target, and dependency policy. Report commands that could not run and why.

