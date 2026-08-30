# Prerequisites & Environment Setup

This document covers everything you need to install and configure before contributing to `wasi-pg-client`.

---

## 1. Rust Toolchain

### Minimum Version
- **Rust 1.91.1+** (the workspace MSRV; `wasip3` itself requires Rust 1.90+)
- The `rust-toolchain.toml` in the repo root specifies `stable`, so ensure your stable toolchain is up to date:

```bash
rustup update stable
rustc --version   # should be >= 1.91.1
```

### Required Targets
Install the `wasm32-wasip2` target (WASI Preview 2):

```bash
rustup target add wasm32-wasip2
```

This stable target is also the project's primary WASI 0.3 build route. Rust `std` continues to import P2, while the client transport and timers import native WASI 0.3 interfaces without a feature flag:

```bash
cargo build -p wasi-pg-client --target wasm32-wasip2
```

The resulting component requires a runtime that supports its P2 and P3 imports (directly or through adapters).

Rust's separate `wasm32-wasip3` compiler target is currently Tier 3, so `rustup target add` is not available for it. To run the repository's additional target check, install nightly Rust sources:

```bash
rustup toolchain install nightly --profile minimal --component rust-src
cargo +nightly check -Z build-std=std,panic_abort \
  -p wasi-pg-client --target wasm32-wasip3
```

A fully linked command using the Tier-3 target additionally needs a compatible `wasi-sdk` and P3-capable runtime. This target is optional; the stable development and release requirement remains `wasm32-wasip2`.

The runtime must implement the same final WASI 0.3 async ABI as `wasip3` 0.8. Wasmtime 48.0.1 is verified with this workspace. Wasmtime 43 and 45 recognize P3 components but fail to link the current `wasi:clocks/monotonic-clock.wait-for` signature.

### Required Components
```bash
rustup component add rustfmt clippy --toolchain stable
```

---

## 2. wasmtime CLI

`wasmtime` is the primary runtime for testing the workspace's P2-targeted components with P3 imports. Use Wasmtime 48.0.1 or a compatible newer release.

### Installation
```bash
curl https://wasmtime.dev/install.sh -sSf | bash
```

Or via package managers:
- **Homebrew (macOS)**: `brew install wasmtime`
- **Cargo**: `cargo install wasmtime-cli --version 48.0.1 --locked` (building this release requires Rust 1.95.0)

The official prebuilt Wasmtime 48.0.1 archive is preferable when the host only
has the workspace MSRV toolchain or when avoiding a full local Wasmtime build.
`wasmtime-cli` is a host validation tool, not a dependency to add to this
workspace's `Cargo.toml`.

### Verify
```bash
wasmtime --version   # verified: wasmtime 48.0.1
```

### Network Permissions
When running these components, enable component-model async, WASI P3, and inherited networking:

```bash
wasmtime run -W component-model-async=y -S p3=y \
  -S inherit-network=y -S allow-ip-name-lookup=y -S tcp=y \
  -S inherit-env=y your-component.wasm
```

For Wasmtime 48.0.1 P3 components, pass `allow-ip-name-lookup=y` and `tcp=y`
explicitly. `inherit-network=y` alone grants the inherited Preview 2 network,
but did not enable P3 name lookup in the workspace runtime smoke test.

An asynchronous P3 command must be built as a `cdylib` and export `wasi:cli/run` with `wasip3::cli::command::export!`. Calling `wasip3::wit_bindgen::block_on` from a synchronous Rust `bin` entry point traps because Wasmtime cannot block that synchronous task before it returns.

---

## 3. WSL (Windows Users)

All WASI builds and tests **must** be done in WSL because:
- `wasmtime` for Windows has different path handling
- Native Windows paths cause issues with WASI filesystem mappings
- The CI runs on Linux

### Setup
```powershell
wsl --install   # if not already installed
```

Then inside WSL:
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Install targets
rustup target add wasm32-wasip2

# Install wasmtime
curl https://wasmtime.dev/install.sh -sSf | bash
source ~/.bashrc   # or restart your shell
```

### Running builds from Windows
If your project is on the Windows D: drive, access it via `/mnt/d/` in WSL:

```bash
cd /mnt/d/dev/wasi_pg_client
cargo build --workspace --target wasm32-wasip2
```

---

## 4. PostgreSQL (for Integration Tests)

Integration tests need a real PostgreSQL server. The easiest way is Docker:

```bash
docker run -d \
  --name pg-test \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=test \
  -p 5432:5432 \
  postgres:16 \
  -c ssl=on \
  -c ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem \
  -c ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key
```

The ignored `e2e_tls` and `e2e_pool` test targets can instead start PostgreSQL
16 containers through `testcontainers`; they require Docker or a compatible
Podman setup.

### Test Environment Variable
```bash
export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test"
```

---

## 5. Project-Specific Cargo Configuration

The `.cargo/config.toml` is already committed. **Do not** add:

```toml
[build]
target = "wasm32-wasip2"   # ❌ DON'T DO THIS
```

Setting a default build target breaks native test compilation because dev-dependencies (`proptest`, `wait-timeout`) don't compile for WASM. Always pass `--target wasm32-wasip2` explicitly when building for WASI.

---

## 6. IDE / Editor Setup

### VS Code
Recommended extensions:
- **rust-analyzer** — Rust language support
- **Even Better TOML** — Cargo.toml editing
- **CodeLLDB** — Debugging native tests

Settings (`.vscode/settings.json`):
```json
{
  "rust-analyzer.cargo.target": null,
  "rust-analyzer.check.command": "clippy",
  "rust-analyzer.check.extraArgs": ["--all-targets"]
}
```

> Note: Do **not** set `rust-analyzer.cargo.target` to `wasm32-wasip2` — it will break IDE support for native tests and dev-dependencies.

### Zed / Vim / Emacs
Ensure your LSP runs `cargo check` without `--target wasm32-wasip2` for the best experience. Use `cargo check --target wasm32-wasip2` only in CI or manual verification.

---

## 7. Quick Verification

After setup, run these commands to verify everything works:

```bash
# 1. Native check + tests (matches CI)
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets --no-run
cargo test -p wasi-pg-client --lib --all-features
cargo fmt --all --check

# 2. WASI build
cargo build --workspace --target wasm32-wasip2

# 3. Smoke test in wasmtime
cargo build --target wasm32-wasip2 -p smoke-test
wasmtime run -W component-model-async=y -S p3=y \
  -S inherit-network=y -S allow-ip-name-lookup=y -S tcp=y \
  -S inherit-env=y \
  target/wasm32-wasip2/debug/smoke_test.wasm
```

All commands should complete without errors. Do not combine `--all-features`
with the WASI build: that enables the native-only `tokio-transport` feature.

---

## 8. Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| `wait-timeout` fails to compile | Default target set to `wasm32-wasip2` | Remove `[build] target` from `.cargo/config.toml` |
| Runtime reports missing imports | The component combines Rust `std` P2 imports with client P3 imports | Use a runtime supporting both interface sets or configure the required adapters |
| `PermanentResolverFailure` for a valid hostname | P3 name lookup was not explicitly enabled | Add `-S allow-ip-name-lookup=y`; also add `-S tcp=y` for connections |
| `cannot block a synchronous task before returning` | A P3 future was driven with `block_on` from a Rust `bin` | Build a `cdylib` and export the async `wasi:cli/run` guest interface |
| `getrandom` panic at runtime | Misconfigured random source | Ensure `getrandom` v0.4+; call `ensure_random_available()` early |
| `wasmtime: command not found` | Not installed or not in PATH | Re-run install script; source shell profile |
| `rustc` version < 1.91.1 | Outdated toolchain | `rustup update stable` |
| Tests hang in WSL | File locking on Windows mount | Close other Cargo processes; check `cargo` isn't running in Windows terminal |
