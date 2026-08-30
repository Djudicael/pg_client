# wasi-pg-client

A PostgreSQL client library using native WASI 0.3 interfaces on Rust's stable `wasm32-wasip2` target, written in Rust. The native and WASI compile matrix is green, the library unit suite is exercised with all features, and the Wasmtime smoke component validates P3 clocks, DNS, and TCP. Running against PostgreSQL still requires the separately documented integration-test environment.

## Features

- ✅ Full PostgreSQL wire protocol v3 support
- ✅ Parameterized queries (SQL injection prevention)
- ✅ Prepared statements with automatic LRU caching
- ✅ Streaming results (O(1) memory for large queries)
- ✅ Parameterized streaming (`query_params_stream`)
- ✅ Transactions with RAII guards and savepoints
- ✅ COPY protocol for bulk import/export (CSV + binary)
- ✅ LISTEN/NOTIFY for pub/sub with timeout support
- ✅ TLS via rustls (pure Rust, WASI-compatible)
- ✅ SCRAM-SHA-256 and SCRAM-SHA-256-PLUS channel binding support when TLS channel-binding data is available
- ✅ MD5 authentication (legacy, opt-in)
- ✅ Connection pooling behind `pool` feature flag
- ✅ Automatic reconnection with session state rebuild
- ✅ Retry policies for transient errors (serialization failures, deadlocks)
- ✅ Query cancellation via `CancelToken` (with TLS support)
- ✅ Runtime parameter setting (`set_param`) with reconnect re-application
- ✅ Structured logging via `tracing`
- ✅ Compiles to `wasm32-wasip2` and native targets
- ✅ Native WASI 0.3 asynchronous sockets, DNS, streams, and clocks on the stable `wasm32-wasip2` compiler target

## Quick Start

Add to your `Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
wasi-pg-client = "0.2"
wasip3 = "0.8"
```

Write the component in `src/lib.rs`:

```rust
use wasi_pg_client::{Config, Connection};

wasip3::cli::command::export!(App);

struct App;

impl wasip3::exports::cli::run::Guest for App {
    async fn run() -> Result<(), ()> {
        run().await.map_err(|error| eprintln!("application failed: {error}"))
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_uri("postgresql://user:pass@localhost/mydb")?;
    let mut conn = Connection::connect(&config).await?;

    let result = conn.query("SELECT id, name FROM users").await?;
    for row in result.iter() {
        let id: i32 = row.get(0)?;
        let name: String = row.get(1)?;
        println!("{}: {}", id, name);
    }

    conn.close().await?;
    Ok(())
}
```

Build and run with wasmtime:

```bash
cargo build --target wasm32-wasip2
wasmtime run -W component-model-async=y -S p3=y \
  -S inherit-network=y -S allow-ip-name-lookup=y -S tcp=y \
  -S inherit-env=y \
  target/wasm32-wasip2/debug/your_app.wasm
```

## WASI target requirements

- **Rust**: 1.91.1 or newer (the minimum supported Rust version)
- **Target**: `wasm32-wasip2` (stable since Rust 1.78)
- **Runtime**: Wasmtime 48.0.1 or a compatible P3 runtime, with component-model async, P3, DNS lookup, and TCP permissions enabled
- **getrandom**: Version 0.4 or newer, which detects WASI Preview 2 automatically

### `Send` on WASI

`Connection` is `Send` on `wasm32-wasip2`.

### WASI 0.3 interfaces

The Rust target name and the interfaces imported by a component are separate choices. Following the `wasip3` crate's supported setup, the library uses Rust's stable `wasm32-wasip2` target (and its P2-based `std`) while its transport imports WASI 0.3 interfaces. No client feature flag is required.

```bash
cargo build --target wasm32-wasip2
```

The transport uses native asynchronous DNS, TCP stream, and monotonic-clock interfaces from `wasip3`; reconnect, retry, and pool sleeps consequently call `wasip3::clocks::monotonic_clock::wait_for` directly. The client library no longer depends on `wasip2` or `wstd`. A P3 command is a `cdylib` component that exports the asynchronous `wasi:cli/run` interface; it must not call `block_on` from a synchronous Rust binary entry point. The component still has P2 imports from Rust `std` and P3 imports from this client, so its runtime must support both interface sets or provide adapters.

The runtime's P3 ABI must match the `wasip3` crate version. Wasmtime 48.0.1 is runtime-validated with the workspace's `wasip3` 0.8 smoke component. Wasmtime 43 and 45 recognize P3 components but cannot link its current `wait-for` import. This smoke validation is not a PostgreSQL integration test.

Rust also has an experimental Tier-3 `wasm32-wasip3` target. The CI compile-checks it with nightly and `-Z build-std`, but that compiler target is not required for the supported stable-target path.

## Usage Examples

### Parameterized Queries

```rust
let result = conn.query_params(
    "SELECT * FROM users WHERE age > $1 AND city = $2",
    &[&18i32, &"Paris"],
).await?;
```

### Streaming Large Results

```rust
// Stream rows one at a time (O(1) memory)
let mut stream = conn.query_stream("SELECT * FROM large_table").await?;
while let Some(row) = stream.next().await? {
    let id: i32 = row.get(0)?;
    // Process each row as it arrives
}

// Parameterized streaming — bind parameters and stream results
let mut stream = conn.query_params_stream(
    "SELECT * FROM users WHERE age > $1",
    &[&18i32],
).await?;
while let Some(row) = stream.next().await? {
    let name: String = row.get(1)?;
}

// Cursor-based streaming with fetch size
let mut cursor = conn.query_cursor_stream(
    "SELECT * FROM huge_table WHERE category = $1",
    &[&"electronics"],
    1000, // fetch 1000 rows per round-trip
).await?;
```

### Transactions

```rust
// Automatic rollback on error, commit on success
conn.with_transaction(|txn| async {
    txn.execute_params(
        "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
        &[&amount, &from],
    ).await?;
    txn.execute_params(
        "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        &[&amount, &to],
    ).await?;
    Ok(())
}).await?;
```

### Runtime Parameters

```rust
// Set a session-level parameter (tracked for reconnection)
conn.set_param("timezone", "UTC").await?;
conn.set_param("application_name", "my_app").await?;
```

### LISTEN/NOTIFY with Timeout

```rust
// Listen for events
conn.listen("events").await?;

// Wait with timeout
if let Some(n) = conn.wait_for_notification_with_timeout(
    std::time::Duration::from_secs(5)
).await? {
    println!("Got notification on {}: {}", n.channel, n.payload);
}
```

### Connection Pool

```rust
use wasi_pg_client::{Config, Connection};
use wasi_pg_client::pool::{Pool, PoolConfig};

let pool_config = PoolConfig::default()
    .connection(config)
    .max_size(5);

let pool = Pool::new(pool_config).await?;
let mut guard = pool.acquire().await?;
guard.query("SELECT 1").await?;
guard.release().await;  // preferred over Drop for proper cleanup
```

### Error Handling

```rust
use wasi_pg_client::{PgError, ErrorClass};

match conn.execute_params("INSERT INTO users (email) VALUES ($1)", &[&"user@example.com"]).await {
    Ok(result) => println!("Inserted {} rows", result.rows_affected().unwrap_or(0)),
    Err(PgError::Server(e)) if e.is_unique_violation() => {
        println!("Email already exists");
    }
    Err(e) => {
        match wasi_pg_client::classify_error(&e) {
            ErrorClass::Broken => println!("Connection broken — need to reconnect"),
            ErrorClass::Transient => println!("Transient error — can retry"),
            ErrorClass::Permanent => println!("Permanent error — cannot retry"),
        }
        return Err(e);
    }
}
```

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tls` | ✅ | TLS support via rustls |
| `scram` | ✅ | SCRAM-SHA-256 authentication, including SCRAM-SHA-256-PLUS when channel binding is available |
| `md5-auth` | ❌ | MD5 authentication (legacy) |
| `pool` | ❌ | Connection pooling |
| `tracing` | ✅ | Structured logging via tracing |
| `uuid` | ❌ | UUID type support via uuid crate |
| `serde-json` | ❌ | JSON type support via serde_json |
| `chrono` | ❌ | chrono integration for date/time |
| `test-native` | ❌ | Native transport for testing |
| `tokio-transport` | ❌ | Tokio async TCP transport for native builds |

## Project Structure

The library is a single crate with two internal protocol layers:

| Module | Purpose | I/O | Async |
|--------|---------|-----|-------|
| `protocol` | Wire protocol encoding/decoding (thin wrapper around `postgres-protocol`) | ❌ | ❌ |
| `types`    | Type system, OID mapping, `ToSql`/`FromSql` (thin wrapper around `postgres-types`) | ❌ | ❌ |
| `pool`     | Connection pooling (behind `pool` feature flag) | ✅ | ✅ |

All modules live inside the `wasi-pg-client` package. Default features enable
TLS, SCRAM, and tracing; enable pool and type-integration features explicitly
when needed.

## Security and deployment posture

Current defaults are intentionally conservative:

- TLS defaults to `sslmode=verify-full` when the `tls` feature is enabled.
- Plaintext fallback requires an explicit insecure mode such as `sslmode=prefer` or `sslmode=disable`.
- Cleartext-password and MD5 authentication over plaintext transports are rejected unless you explicitly opt in with insecure configuration.
- `sslmode=require` is supported for PostgreSQL compatibility, but it intentionally skips certificate verification and should not be treated as a production-grade verification mode.
- `sslmode=verify-ca` verifies the CA chain but intentionally skips hostname verification.
- `accept_invalid_certs(true)` disables certificate verification entirely and is for development/testing only.

For production use, prefer:

- `SslMode::VerifyFull`
- normal hostname verification
- default certificate validation
- SCRAM-based authentication instead of MD5

## API Stability

This is v0.2 — the public API may change between minor versions (SemVer pre-1.0).

- `#[non_exhaustive]` on all public enums and structs ensures adding new variants/fields isn't breaking
- Internal `pub(crate)` items can change freely
- The `AsyncTransport` trait is public for custom transports/testing but may still evolve as the transport surface is refined

## Testing

```bash
# Workspace validation
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check

# WASI 0.3 interfaces on the stable wasm32-wasip2 compiler target
cargo build -p wasi-pg-client --target wasm32-wasip2

# Optional Tier-3 compiler-target check (nightly + rust-src required)
cargo +nightly check -Z build-std=std,panic_abort \
  -p wasi-pg-client --target wasm32-wasip3

# Library tests
cargo test -p wasi-pg-client --lib --all-features

# E2E tests (requires podman or docker, with tokio-transport)
cargo test -p wasi-pg-client \
  --features "pool,tokio-transport,tls" \
  --test e2e_tls --test e2e_pool \
  -- --ignored --test-threads=1

# Build the workspace on the stable WASI target
cargo build --workspace --target wasm32-wasip2
```

## Fuzzing

The repository includes a dedicated `fuzz/` crate with targets for:

- whole-buffer backend message decoding
- incremental/chunked backend framing
- bounded-buffer stress cases
- type-system decode paths across text and binary formats

Typical local commands:

```bash
cargo install cargo-fuzz
cargo check --manifest-path fuzz/Cargo.toml
cargo fuzz run decode_message
cargo fuzz run decode_message_persistent
cargo fuzz run decode_message_bounded
cargo fuzz run decode_pg_types
```

`cargo fuzz run` uses nightly Rust through `cargo-fuzz`. Fuzzing is currently a
manual/pre-release hardening tool rather than a default CI step.

## Thread Safety

- **Supported WASI command path**: single-threaded execution — `Connection` is `Send` but not `Sync`
- **Native (`tokio-transport`)**: multi-thread-friendly — `Pool` is `Send + Sync` via `std::sync::Mutex`

## Limitations (WASI)

- **Single-threaded execution model** – the currently supported component execution model is single-threaded even though key types such as `Connection` are `Send`
- **No background tasks** – pool maintenance is lazy (on acquire)
- **Connection pooling** – the library ships with a built-in pool (`pool` feature), but without a supported background-task spawning API, an external pool manager such as [PgBouncer](https://www.pgbouncer.org/) is preferred for production WASI deployments. The in-process pool remains useful for native builds, testing, and forward compatibility.
- **No certificate-file loading** – the TLS configuration uses embedded roots from `webpki-roots`; this client does not load custom certificate files
- **No process spawning** – cannot run `pg_dump` or external tools
- **Notification timeout** – native tokio builds have a real timeout race; WASI currently keeps a best-effort fallback path
- **Runtime DNS / sockets behavior depends on the host runtime** – the WASI transport now uses `wasi:sockets/ip-name-lookup`, so behavior follows the runtime's implementation rather than the host standard library

## License

Dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.
