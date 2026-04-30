# Prerequisites & Environment Setup

This document covers everything you need to install and configure before contributing to `wasi-pg-client`.

---

## 1. Rust Toolchain

### Minimum Version
- **Rust 1.87+** (required by `wstd` 0.5.6)
- The `rust-toolchain.toml` in the repo root specifies `stable`, so ensure your stable toolchain is up to date:

```bash
rustup update stable
rustc --version   # should be >= 1.87.0
```

### Required Targets
Install the `wasm32-wasip2` target (WASI Preview 2):

```bash
rustup target add wasm32-wasip2
```

### Required Components
```bash
rustup component add rustfmt clippy --toolchain stable
```

---

## 2. wasmtime CLI

`wasmtime` is the primary WASI P2 runtime for testing compiled components.

### Installation
```bash
curl https://wasmtime.dev/install.sh -sSf | bash
```

Or via package managers:
- **Homebrew (macOS)**: `brew install wasmtime`
- **Cargo**: `cargo install wasmtime-cli`

### Verify
```bash
wasmtime --version   # e.g., wasmtime-cli 24.0.0
```

### Network Permissions
When running WASI components that use TCP, always pass `--wasi inherit-network`:

```bash
wasmtime run --wasi inherit-network --wasi inherit-env your-component.wasm
```

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
cargo build --target wasm32-wasip2 --all-features
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

Or use the GitHub Actions services definition from `.github/workflows/ci.yml`.

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
  "rust-analyzer.check.extraArgs": ["--all-targets", "--all-features"]
}
```

> Note: Do **not** set `rust-analyzer.cargo.target` to `wasm32-wasip2` — it will break IDE support for native tests and dev-dependencies.

### Zed / Vim / Emacs
Ensure your LSP runs `cargo check` without `--target wasm32-wasip2` for the best experience. Use `cargo check --target wasm32-wasip2` only in CI or manual verification.

---

## 7. Quick Verification

After setup, run these commands to verify everything works:

```bash
# 1. Native check + tests
cargo check --all-targets --all-features
cargo test -p pg-protocol -p pg-types --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check

# 2. WASI build
cargo build --target wasm32-wasip2 --all-features

# 3. Smoke test in wasmtime
cargo build --target wasm32-wasip2 -p smoke-test
wasmtime run --wasi inherit-network --wasi inherit-env \
  target/wasm32-wasip2/debug/smoke-test.wasm
```

All four should complete without errors.

---

## 8. Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| `wait-timeout` fails to compile | Default target set to `wasm32-wasip2` | Remove `[build] target` from `.cargo/config.toml` |
| `wstd::net::TcpStream::connect` not found | wstd 0.5.x has no client connect | Use raw `wasip2::sockets::tcp` (see `examples/smoke-test`) |
| `getrandom` panic at runtime | Misconfigured random source | Ensure `getrandom` v0.4+; call `ensure_random_available()` early |
| `wasmtime: command not found` | Not installed or not in PATH | Re-run install script; source shell profile |
| `rustc` version < 1.87 | Outdated toolchain | `rustup update stable` |
| Tests hang in WSL | File locking on Windows mount | Close other Cargo processes; check `cargo` isn't running in Windows terminal |
