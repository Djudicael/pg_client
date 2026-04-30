# Step 01 - Project Setup & WASI P2 Foundation

## Goal
Bootstrap the Rust project targeting `wasm32-wasip2`, set up the workspace structure, async runtime, CI, and validate that a minimal async WASI P2 component compiles and runs.

## Context
WASI Preview 2 (wasip2) is based on the Component Model. Rust supports it via the `wasm32-wasip2` target (stable since Rust 1.78+). Unlike wasip1, wasip2 uses typed interfaces (WIT - WebAssembly Interface Types) for host imports like sockets and I/O.

**Async in WASI P2**: Rust's `async`/`await` compiles to state machines and does not require `tokio`. WASI P2 provides `wasi:io/poll` which can drive futures natively. The `wstd` crate provides a complete async standard library for WASI 0.2 including:
- `wstd::net::TcpListener` - async TCP server socket
- `wstd::io` - async I/O primitives (`AsyncRead`, `AsyncWrite`, `AsyncInputStream`, `AsyncOutputStream`)
- `wstd::runtime` - async executor (`#[wstd::main]`, `AsyncPollable`)
- `wstd::time` - async timers (`Duration`, `Instant`)
- `wstd::wasip2` - re-exported raw WASI P2 bindings (needed for `TcpStream::connect` — see below)
- `wstd::task::sleep` - async sleep

**Important**: `wstd::net::TcpStream` exists but does **not** expose a `connect()` constructor in wstd 0.5.x. Only `TcpListener::bind` (server-side) is available. Client-side TCP connections must use raw `wasip2::sockets::tcp` bindings (`create_tcp_socket`, `start_connect`, `finish_connect`) wrapped in `AsyncInputStream` / `AsyncOutputStream`. This is implemented in the smoke test example and will be used by the transport layer in step 02.

This means our library will be **fully async** from the start.

**Critical WASI P2 gotcha — `getrandom`**: Many crates (`rustls`, SCRAM auth, TLS) need a CSPRNG. The `getrandom` crate is the standard interface, but it must be configured to use `wasi:random/random` on the `wasm32-wasip2` target. Without this, any crypto operation will panic at runtime with "unsupported target" or similar. We use `getrandom` v0.4 which has WASI P2 auto-detection, and add a runtime sanity check in `Connection::connect` to fail fast with a clear message.

## Tasks

### 1.1 - Initialize workspace
```
cargo init --lib wasi-pg-client
```
Use a Cargo workspace to separate concerns:
```
wasi_pg_client/
├── Cargo.toml              (workspace root)
├── crates/
│   ├── pg-protocol/        (wire protocol codec, no I/O, sync)
│   ├── pg-types/           (type OID mapping, serialization, sync)
│   ├── pg-client/          (main async client, transport, connection)
│   └── pg-pool/            (async connection pooling)
├── tests/                  (integration tests)
├── examples/               (WASI component examples)
├── fuzz/                   (fuzz targets for protocol decoder)
├── plan/                   (these plan files)
├── .cargo/
│   └── config.toml         (target, runner, getrandom config)
├── rust-toolchain.toml
├── .github/
│   └── workflows/
│       └── ci.yml
└── README.md
```

### 1.2 - Configure `wasm32-wasip2` target

`.cargo/config.toml`:
```toml
[build]
target = "wasm32-wasip2"

[target.wasm32-wasip2]
runner = "wasmtime --wasi inherit-network --wasi inherit-env"
```

> **Important**: Do NOT add `[build] target = "wasm32-wasip2"` to `.cargo/config.toml`. Setting a default build target causes `cargo clippy --all-targets --all-features` and `cargo test` to compile dev-dependencies (`proptest`, `wait-timeout`) for WASM, which will fail. Always pass `--target wasm32-wasip2` explicitly to build commands.

> **Note**: The `[target.<triple>.dependencies]` table in `.cargo/config.toml` is not a standard Cargo feature. Instead, we configure `getrandom` in each crate's `Cargo.toml` with `features = ["wasi"]` when targeting WASI. The config above documents the intent; the actual feature flag goes in the dependency declarations.

### 1.3 - Rust toolchain

`rust-toolchain.toml`:
```toml
[toolchain]
channel = "stable"
targets = ["wasm32-wasip2"]
components = ["rustfmt", "clippy"]
```

Minimum Rust version: **1.78** (first stable release with `wasm32-wasip2` target). We recommend 1.82+ for the best `async fn` in traits support.

### 1.4 - Workspace `Cargo.toml`

```toml
[workspace]
resolver = "2"
members = [
    "crates/pg-protocol",
    "crates/pg-types",
    "crates/pg-client",
    "crates/pg-pool",
    "examples/*",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
rust-version = "1.78"
license = "MIT OR Apache-2.0"
repository = "https://github.com/your-org/wasi-pg-client"

[workspace.dependencies]
# ── Async runtime (WASI P2) ──
wstd = "0.5"           # Async std for WASI 0.2 — pin to known-good version
# NOTE: wstd API is evolving. Pin the version and test upgrades carefully.
# If wstd breaks, our fallback is raw wasi:sockets bindings (see step-02).

# ── WASI bindings ──
wasi = "0.13"          # Low-level WASI P2 bindings (used by wstd internally)

# ── Byte handling ──
bytes = "1"            # Efficient byte buffer manipulation (zero-copy slices)

# ── Crypto (pure Rust, WASI-compatible) ──
sha2 = "0.10"          # SHA-256 for SCRAM auth
hmac = "0.12"          # HMAC for SCRAM auth
pbkdf2 = "0.12"        # PBKDF2 key derivation for SCRAM
md-5 = "0.10"          # MD5 for legacy PG auth (optional feature)
base64 = "0.22"        # Base64 for SCRAM encoding

# ── TLS (pure Rust) ──
rustls = { version = "0.23", default-features = false, features = ["std", "tls12"] }
rustls-rustcrypto = "0.0.2"   # CryptoProvider using pure-Rust RustCrypto
# WARNING: rustls-rustcrypto is immature (v0.0.x). It may lack cipher suites
# or have performance issues. Mitigation strategy:
#   1. Try rustls-ring first on WASI (ring may compile for wasm32-wasip2 on recent versions)
#   2. Fall back to rustls-rustcrypto if ring fails
#   3. If neither works, support plaintext-only mode with a clear compile-time warning
webpki-roots = "0.26"         # Mozilla CA roots (embedded, no filesystem needed)

# ── Random ──
getrandom = "0.4"      # CSPRNG interface — v0.4+ has WASI P2 auto-detection

# ── Error handling ──
thiserror = "2"        # Derive Error trait

# ── Logging ──
tracing = "0.1"        # Structured logging / instrumentation

# ── Async utilities ──
futures-concurrency = "7"  # Async combinators (join, race, etc.) — no runtime dep

# ── Optional type integrations ──
uuid = { version = "1", optional = true }
serde_json = { version = "1", optional = true }
chrono = { version = "0.4", optional = true }

# ── Dev dependencies ──
pretty_assertions = "1"
proptest = "1"         # Property-based testing for protocol/types
```

### 1.5 - Sub-crate `Cargo.toml` files

#### `crates/pg-protocol/Cargo.toml`
```toml
[package]
name = "pg-protocol"
description = "PostgreSQL wire protocol codec (I/O-free, sync)"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
pretty_assertions = { workspace = true }
```

#### `crates/pg-types/Cargo.toml`
```toml
[package]
name = "pg-types"
description = "PostgreSQL type system: ToSql/FromSql, OID mapping, encoding"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true, optional = true }
serde_json = { workspace = true, optional = true }
chrono = { workspace = true, optional = true }

[features]
default = []
uuid = ["dep:uuid"]
serde-json = ["dep:serde_json"]
chrono = ["dep:chrono"]
```

#### `crates/pg-client/Cargo.toml`
```toml
[package]
name = "wasi-pg-client"
description = "PostgreSQL client library for WASI Preview 2"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
pg-protocol = { path = "../pg-protocol" }
pg-types = { path = "../pg-types" }
wstd = { workspace = true }
bytes = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true, optional = true }
futures-concurrency = { workspace = true }

# Crypto for auth
sha2 = { workspace = true, optional = true }
hmac = { workspace = true, optional = true }
pbkdf2 = { workspace = true, optional = true }
md-5 = { workspace = true, optional = true }
base64 = { workspace = true, optional = true }
getrandom = { workspace = true }

# TLS
rustls = { workspace = true, optional = true }
rustls-rustcrypto = { workspace = true, optional = true }
webpki-roots = { workspace = true, optional = true }

[features]
default = ["tls", "scram", "tracing"]
tls = ["dep:rustls", "dep:rustls-rustcrypto", "dep:webpki-roots"]
scram = ["dep:sha2", "dep:hmac", "dep:pbkdf2", "dep:base64"]
md5-auth = ["dep:md-5"]
tracing = ["dep:tracing"]
test-native = []  # Enable native (blocking) transport for non-WASI testing

[dev-dependencies]
proptest = { workspace = true }
pretty_assertions = { workspace = true }
```

#### `crates/pg-pool/Cargo.toml`
```toml
[package]
name = "wasi-pg-pool"
description = "Connection pooling for wasi-pg-client"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
pg-client = { path = "../pg-client" }
pg-protocol = { path = "../pg-protocol" }
thiserror = { workspace = true }
tracing = { workspace = true, optional = true }

[features]
default = ["tracing"]
tracing = ["dep:tracing", "pg-client/tracing"]
```

### 1.6 - Async model
Since WASI P2 is single-threaded, no `Send`/`Sync` bounds are required on futures. This simplifies the async design significantly:
- No `Arc<Mutex<...>>` needed for single-threaded use
- Async traits can use `async fn` directly (no `Pin<Box<dyn Future + Send>>`)
- Use `futures-concurrency` for concurrent operations (e.g., timeout races)

**Important: `async fn` in traits limitations**:
- Rust 1.75+ stabilized `async fn` in traits, but **dynamic dispatch (`dyn Trait`) with `async fn` is not yet stable**
- Our `AsyncTransport` trait uses `async fn` — this means we use **generic dispatch only** (`impl AsyncTransport`), never `dyn AsyncTransport`
- For testing, mock transports are passed as generic parameters: `fn do_thing<T: AsyncTransport>(transport: &mut T)`
- This is a deliberate design choice; it avoids `Pin<Box<dyn Future>>` overhead and works on WASI

```rust
// Example: entry point for a WASI component using the library
#[wstd::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::connect("postgresql://user:pass@localhost/db").await?;
    let rows = conn.query("SELECT 1").await?;
    conn.close().await?;
    Ok(())
}
```

### 1.7 - `getrandom` configuration (CRITICAL)

The `getrandom` crate is used by `rustls`, SCRAM auth, and TLS for cryptographic randomness. On `wasm32-wasip2`, it must use `wasi:random/random`.

**Configuration**:
1. Pin `getrandom = "0.3"` (v0.3+ auto-detects `wasm32-wasip2` and uses `wasi:random/random`)
2. For `getrandom = "0.2"`, you must enable the `"wasi"` feature explicitly
3. Add a runtime sanity check in the library init:

```rust
// In pg-client/src/lib.rs
fn ensure_random_available() {
    let mut buf = [0u8; 1];
    if getrandom::fill(&mut buf).is_err() {
        panic!(
            "wasi-pg-client: getrandom failed. \
            Ensure 'getrandom' is compiled with features=[\"wasi\"] \
            when targeting wasm32-wasip2. \
            In your Cargo.toml: getrandom = {{ version = \"0.3\", features = [\"wasi\"] }}"
        );
    }
}
```

4. Document this in the crate-level doc comment and README

**Why this matters**: If `getrandom` is misconfigured, you get a runtime panic only when the first crypto operation is attempted (e.g., during SCRAM auth or TLS handshake). This is confusing and hard to debug. The early check makes the failure immediate and actionable.

### 1.8 - Validate minimal async build
Create a smoke test that:
1. Compiles to `wasm32-wasip2`
2. Uses `#[wstd::main]` as async entry point
3. Opens a TCP connection via `wstd::net::TcpStream`
4. Exercises `getrandom` to verify CSPRNG availability
5. Runs in wasmtime with `--wasi inherit-network`

`examples/smoke_test/src/main.rs`:
```rust
//! Minimal WASI P2 smoke test: verify TCP + random + async runtime work.

#[wstd::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Verify getrandom works
    let mut rand_buf = [0u8; 4];
    getrandom::fill(&mut rand_buf)?;
    eprintln!("Random bytes: {:02x?}", rand_buf);

    // 2. Verify TCP connect works (to a well-known host)
    //    This will fail if wasmtime doesn't have --wasi inherit-network
    match wstd::net::TcpStream::connect("example.com:80").await {
        Ok(_) => eprintln!("TCP connect: OK"),
        Err(e) => eprintln!("TCP connect failed: {} (expected if no network)", e),
    }

    // 3. Verify async timer works
    wstd::task::sleep(std::time::Duration::from_millis(10)).await;
    eprintln!("Async sleep: OK");

    eprintln!("All smoke tests passed!");
    Ok(())
}
```

`examples/smoke_test/Cargo.toml`:
```toml
[package]
name = "smoke-test"
version.workspace = true
edition.workspace = true

[dependencies]
wstd = { workspace = true }
getrandom = { workspace = true }
```

The smoke test uses raw `wasip2::sockets::tcp` to create a client TCP connection because `wstd::net::TcpStream` does not expose `connect()` in wstd 0.5.x:

```rust
use wstd::io::{AsyncInputStream, AsyncOutputStream, AsyncWrite};
use wstd::runtime::{AsyncPollable, WaitFor};
use wstd::wasip2::sockets::{
    instance_network::instance_network,
    network::{Ipv4SocketAddress, Ipv6SocketAddress},
    tcp::{IpAddressFamily, IpSocketAddress},
    tcp_create_socket::create_tcp_socket,
};

async fn try_tcp_connect(addr: &str) -> wstd::io::Result<()> {
    let std_addr: std::net::SocketAddr = addr.parse()
        .map_err(|_| wstd::io::Error::other("bad addr"))?;
    let family = match std_addr {
        std::net::SocketAddr::V4(_) => IpAddressFamily::Ipv4,
        std::net::SocketAddr::V6(_) => IpAddressFamily::Ipv6,
    };
    let socket = create_tcp_socket(family)
        .map_err(|e| wstd::io::Error::other(format!("{:?}", e)))?;
    let network = instance_network();

    let wasi_addr = match std_addr {
        std::net::SocketAddr::V4(addr) => {
            let ip = addr.ip().octets();
            IpSocketAddress::Ipv4(Ipv4SocketAddress {
                address: (ip[0], ip[1], ip[2], ip[3]),
                port: addr.port(),
            })
        }
        std::net::SocketAddr::V6(addr) => {
            let ip = addr.ip().segments();
            IpSocketAddress::Ipv6(Ipv6SocketAddress {
                address: (ip[0], ip[1], ip[2], ip[3], ip[4], ip[5], ip[6], ip[7]),
                port: addr.port(),
                flow_info: addr.flowinfo(),
                scope_id: addr.scope_id(),
            })
        }
    };

    socket.start_connect(&network, wasi_addr)
        .map_err(|e| wstd::io::Error::other(format!("{:?}", e)))?;
    AsyncPollable::new(socket.subscribe()).wait_for().await;

    let (input, output) = socket.finish_connect()
        .map_err(|e| wstd::io::Error::other(format!("{:?}", e)))?;

    let mut input = AsyncInputStream::new(input);
    let mut output = AsyncOutputStream::new(output);

    // ... use input.read() / output.write_all() ...
    Ok(())
}
```

### 1.9 - Feature flags design
```toml
[features]
default = ["tls", "scram", "tracing"]

# TLS support via rustls (pure Rust, WASI-compatible)
tls = ["dep:rustls", "dep:rustls-rustcrypto", "dep:webpki-roots"]

# SCRAM-SHA-256 authentication (recommended, PG 10+ default)
scram = ["dep:sha2", "dep:hmac", "dep:pbkdf2", "dep:base64"]

# MD5 authentication (legacy, less secure)
md5-auth = ["dep:md-5"]

# Connection pooling
pool = ["dep:wasi-pg-pool"]

# Structured logging via tracing crate
tracing = ["dep:tracing"]

# UUID type support via uuid crate
uuid = ["pg-types/uuid", "dep:uuid"]

# JSON type support via serde_json
serde-json = ["pg-types/serde-json", "dep:serde_json"]

# chrono integration for date/time types
chrono = ["pg-types/chrono", "dep:chrono"]

# Native test support (blocking I/O transport for non-WASI testing)
test-native = []
```

### 1.10 - CI pipeline (GitHub Actions)

`.github/workflows/ci.yml`:
```yaml
name: CI
on: [push, pull_request]

env:
  CARGO_TERM_COLOR: always

jobs:
  # ── Lint ──
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - run: cargo fmt --all -- --check
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo clippy --all-targets --all-features -- -D warnings

  # ── Unit tests (native, no WASI needed) ──
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo test -p pg-protocol -p pg-types
      - run: cargo test -p pg-protocol -p pg-types --all-features

  # ── WASI build check ──
  wasi-build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: wasm32-wasip2
      - name: Build all crates for WASI P2
        run: cargo build --target wasm32-wasip2 --all-features
      - name: Build smoke test example
        run: cargo build --target wasm32-wasip2 --example smoke-test
      - name: Check no unnecessary deps
        run: cargo tree --target wasm32-wasip2 --duplicates

  # ── Integration tests (native + real PostgreSQL) ──
  integration-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: Run integration tests
        run: cargo test --test integration --features test-native
        env:
          TEST_DATABASE_URL: postgresql://postgres:postgres@localhost:5432/test

  # ── E2E WASI test ──
  e2e-wasi:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: wasm32-wasip2
      - uses: bytecodealliance/actions/wasmtime/setup@v1
      - name: Build E2E test component
        run: cargo build --target wasm32-wasip2 --example e2e-test
      - name: Run E2E test in wasmtime
        run: |
          wasmtime run \
            --wasi inherit-network \
            --wasi inherit-env \
            --env TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/test \
            target/wasm32-wasip2/debug/examples/e2e_test.wasm

  # ── Security audit ──
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: rustsec/audit-check@v2
        with:
          targets: wasm32-wasip2
```

### 1.11 - `rustls` crypto provider strategy

The TLS stack needs a `CryptoProvider` for `rustls`. On native targets, `ring` is the default and well-tested. On `wasm32-wasip2`, `ring` may not compile (it uses platform-specific assembly). Our strategy:

```
┌─────────────────────────────┐
│ Build time: feature flag    │
│ tls = ["dep:rustls", ...]   │
├─────────────────────────────┤
│ Runtime: try providers      │
│ 1. rustls-ring (if compiled)│
│ 2. rustls-rustcrypto        │
│ 3. Panic with clear message │
├─────────────────────────────┤
│ Fallback: no TLS            │
│ Compile without "tls" feat  │
│ Only plaintext connections  │
└─────────────────────────────┘
```

Implementation in `pg-client/src/transport/tls.rs`:
```rust
fn default_crypto_provider() -> Result<Arc<rustls::crypto::CryptoProvider>, TransportError> {
    // Try rustls-rustcrypto (pure Rust, always available with "tls" feature)
    let provider = rustls_rustcrypto::provider();
    Ok(Arc::new(provider))
}
```

We default to `rustls-rustcrypto` because it's guaranteed to compile on `wasm32-wasip2`. If users want `ring` (faster, more audited), they can configure it manually via the `ClientConfig` API.

### 1.12 - Native test transport scaffolding

For integration tests that run natively (not via WASI), we need a blocking I/O transport. This is behind the `test-native` feature flag.

```rust
// pg-client/src/transport/native.rs (only compiled with test-native feature)
#[cfg(feature = "test-native")]
pub struct NativeTcpTransport {
    stream: std::net::TcpStream,
}

#[cfg(feature = "test-native")]
impl NativeTcpTransport {
    pub fn connect(addr: &str) -> Result<Self, TransportError> {
        let stream = std::net::TcpStream::connect(addr)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        stream.set_nonblocking(false)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        Ok(Self { stream })
    }
}

// Note: AsyncTransport impl uses blocking I/O inside async fn bodies.
// This works because the futures are polled to completion synchronously
// in the test executor. Not suitable for production use.
```

## Acceptance Criteria
- [ ] `cargo build --target wasm32-wasip2` succeeds for all workspace crates
- [ ] Workspace structure is in place with all sub-crates
- [ ] `Cargo.toml` files have correct dependencies and feature flags
- [ ] `getrandom` is properly configured for WASI P2 (v0.4 with auto-detection)
- [ ] Runtime sanity check for `getrandom` is in place
- [ ] Async smoke test component compiles and runs in wasmtime
- [ ] Raw `wasip2::sockets::tcp` connect works from WASI component (wstd lacks `TcpStream::connect`)
- [ ] CI pipeline configured (lint, unit, WASI build, integration, E2E)
- [ ] `rustls` crypto provider strategy documented and implemented
- [ ] Native test transport scaffolding compiles (behind feature flag)
- [ ] `cargo clippy` passes with no warnings
- [ ] `cargo fmt` passes

## WASI P2 Interfaces Used (via wstd)
- `wasi:sockets/tcp` - TCP connections (wrapped by `wstd::net`)
- `wasi:sockets/ip-name-lookup` - DNS resolution
- `wasi:io/streams` - async read/write streams
- `wasi:io/poll` - pollable resources (drives the async executor)
- `wasi:clocks/monotonic-clock` - timers, timeouts
- `wasi:random/random` - cryptographic randomness (via `getrandom`)
- `wasi:cli/environment` - reading env vars

## Key Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| `wstd` API changes between versions | Pin `wstd = "0.5"` in workspace. Test upgrades manually. Maintain thin abstraction so swapping is localised to `transport/tcp.rs`. |
| `rustls-rustcrypto` immaturity (v0.0.x) | Default to it for WASI (guaranteed compile). Allow user-provided `CryptoProvider`. Support `no-tls` mode. |
| `getrandom` misconfiguration | Pin v0.3+. Add runtime sanity check. Document in README and crate docs. |
| `ring` not compiling for WASI | Don't depend on `ring` by default. Use `rustls-rustcrypto`. |
| `async fn` in traits — no dyn dispatch | Use generic parameters everywhere. Mock transports are generic. No `dyn AsyncTransport`. |

## Notes
- No `Send`/`Sync` bounds needed — WASI P2 is single-threaded
- All crypto must be **pure Rust** — no C dependencies
- `pg-protocol` and `pg-types` remain **sync** (pure data transformation, no I/O)
- Only `pg-client` and `pg-pool` are async (they do I/O)
- The `test-native` feature enables running integration tests without wasmtime
- This design positions well for WASI P3 which will have native async in the Component Model itself
- `tracing` is optional but recommended — it provides observability without runtime overhead when disabled
