# wasi-pg-client

A PostgreSQL client library for [WASI Preview 2](https://github.com/WebAssembly/wasi-preview2), written in Rust. The workspace is currently in a hardening phase: the main native and WASI build/test matrix is green, secure defaults have been tightened, and the remaining work is mostly around documentation polish, broader fuzzing process integration, and long-tail API refinement rather than known workspace-breaking issues.

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

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
wasi-pg-client = "0.1"
wstd = "0.6"
wasip2 = "1.0"
```

Write your application:

```rust
use wasi_pg_client::{Config, Connection};

#[wstd::main]
async fn main() -> Result<(), wasi_pg_client::PgError> {
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
wasmtime run --wasi inherit-network --wasi inherit-env target/wasm32-wasip2/debug/your_app.wasm
```

## WASI P2 Requirements

- **Target**: `wasm32-wasip2` (stable since Rust 1.78)
- **Runtime**: wasmtime with `--wasi inherit-network`
- **getrandom**: Must use `features = ["wasi"]` for cryptographic randomness

### `Send` on WASI

`Connection` is `Send` on `wasm32-wasip2` (enabled by `wstd 0.6+`, which uses `Arc` internally).

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

All modules live inside `wasi-pg-client` — a single `cargo add wasi-pg-client` pulls everything in.

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

This is v0.1 — the public API may change between minor versions (semver pre-1.0).

- `#[non_exhaustive]` on all public enums and structs ensures adding new variants/fields isn't breaking
- Internal `pub(crate)` items can change freely
- The `AsyncTransport` trait is public for custom transports/testing but may still evolve as the transport surface is refined

## Testing

```bash
# Workspace validation
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check

# Library tests
cargo test -p wasi-pg-client --lib --all-features

# E2E tests (requires podman or docker, with tokio-transport)
cargo test -p wasi-pg-client \
  --features "pool,tokio-transport,tls" \
  --test e2e_tls --test e2e_pool \
  -- --ignored --test-threads=1

# Build for WASI P2
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
cargo check --manifest-path fuzz/Cargo.toml
cargo fuzz run decode_message
cargo fuzz run decode_message_persistent
cargo fuzz run decode_message_bounded
cargo fuzz run decode_pg_types
```

Fuzzing is currently a manual/pre-release hardening tool rather than a default CI step.

## Thread Safety

- **WASI P2**: single-threaded runtime — `Connection` is `Send` but not `Sync`
- **Native (`tokio-transport`)**: multi-thread-friendly — `Pool` is `Send + Sync` via `std::sync::Mutex`

## Limitations (WASI Preview 2)

- **Single-threaded execution model** – the typical WASI Preview 2 runtime model is still single-threaded even though key types such as `Connection` are `Send`
- **No background tasks** – pool maintenance is lazy (on acquire)
- **Connection pooling** – the library ships with a built-in pool (`pool` feature), but under WASI's single-threaded execution model (no `spawn`), a deported pool manager like [PgBouncer](https://www.pgbouncer.org/) is the preferred architecture for production deployments. The in-process pool is included for convenience and native builds but is intentionally not the recommended path under wasmtime or similar runtimes. This is expected to change with WASI Preview 3, which introduces multithreading — at that point the in-process pool becomes a first-class option for WASI runtimes. Until then, the pool is included for native builds, testing, and forward compatibility.
- **No file system access** – SSL certificates must be embedded (via `webpki-roots`)
- **No process spawning** – cannot run `pg_dump` or external tools
- **Notification timeout** – native tokio builds have a real timeout race; WASI currently keeps a best-effort fallback path
- **Runtime DNS / sockets behavior depends on the host runtime** – the WASI transport now uses `wasi:sockets/ip-name-lookup`, so behavior follows the runtime's implementation rather than the host standard library

## License

Dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.
