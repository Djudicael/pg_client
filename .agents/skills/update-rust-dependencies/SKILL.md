---
name: update-rust-dependencies
description: Audit and safely update dependencies in Rust crates and Cargo workspaces while preserving MSRV, targets, feature behavior, lockfile integrity, and security policy. Use for Cargo.toml or Cargo.lock upgrades, replacing deprecated crates, resolving advisories, checking outdated dependencies, or validating dependency-related pull requests.
---

# Update Rust Dependencies

Update dependencies as a compatibility exercise, not a version-number sweep. Use the repository's own instructions and tools; this skill assumes no particular agent, model, shell, editor, or hosting service.

## 1. Establish constraints

1. Read repository guidance and inspect every workspace manifest, lockfile, toolchain file, CI workflow, deny/audit policy, and release note relevant to dependencies.
2. Check the working tree and preserve unrelated changes.
3. Record the MSRV, compilation targets, default and optional features, excluded sub-workspaces, and required platform or runtime backends.
4. Use the execution environment required by repository guidance. If none is specified, use the project's documented environment.
5. Obtain current version and compatibility information from authoritative sources such as the package registry metadata, upstream release notes, and official documentation. Do not guess based on memory.

## 2. Build an update plan

Classify candidates before editing:

- **Lockfile-only:** a compatible release already allowed by the manifest.
- **Manifest-compatible:** a newer release within the same intended API line.
- **Breaking:** a new major version, changed MSRV, feature change, API migration, or target-support change.
- **Replacement:** a deprecated, unmaintained, incompatible, or security-sensitive crate should be replaced.
- **Blocked:** the update conflicts with MSRV, target support, licensing, policy, or another required dependency.

Prefer small, attributable groups. Keep security fixes and direct dependencies ahead of cosmetic transitive churn. Ask for direction before a replacement that materially changes public behavior, cryptographic/TLS backends, licensing, or supported targets unless the user already authorized it.

## 3. Apply changes

1. Centralize shared versions under `[workspace.dependencies]` when appropriate.
2. Preserve feature flags and `default-features` choices unless upstream migration requires a deliberate change.
3. Use Cargo commands to update the lockfile; never hand-edit `Cargo.lock`.
4. Update source code, examples, and documentation for API migrations.
5. Review the resolved graph for duplicate major versions, unexpected default features, target-specific dependencies, and MSRV regressions.
6. Inspect the final diff before validation and remove accidental formatting or unrelated churn.

## 4. Validate proportionally

Run commands in the repository-required environment. Derive the exact matrix from CI, then cover at least:

```text
cargo fmt --all --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets --no-run
cargo test <primary-library> --lib --all-features
cargo build --workspace --target <required-non-host-target>
cargo deny check advisories bans licenses sources
```

Also validate independent manifests such as fuzz workspaces. Test the declared MSRV when feasible; testing only with stable does not prove MSRV compatibility. Run live service or container tests only when their prerequisites are available, and distinguish compile-only checks from executed integration tests.

When a command fails, determine whether the cause is the update, existing repository state, the environment, or unavailable infrastructure. Fix in-scope regressions and report genuine blockers with the exact command and concise error.

## 5. Report the result

Summarize:

- direct manifest changes and important transitive changes;
- source migrations or intentional feature changes;
- candidates left unchanged and the compatibility reason;
- validation commands and outcomes;
- remaining risk, especially MSRV, WASI/cross-target, native-library, TLS/crypto, and integration-test coverage.

Do not claim that everything is current merely because the lockfile updated or the host build passed.
