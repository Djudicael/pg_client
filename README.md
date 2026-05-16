# wasi-pg-client

A production-grade PostgreSQL client library for [WASI Preview 2](https://github.com/WebAssembly/wasi-preview2), written in Rust. Compile to WebAssembly and run on platforms like wasmtime, WasmEdge, and Spin.

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
- ✅ SCRAM-SHA-256 and MD5 authentication
- ✅ Connection pooling with `Mutex`-based thread safety
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
use wasi_pg_client::{Connection, Config};

#[wstd::main]
async fn main() -> Result<(), wasi_pg_client::PgError> {
    let config = Config::from_uri("postgresql://user:pass@localhost/mydb")?;
    let mut conn = Connection::connect(config).await?;

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

`Connection` is `Send` on `wasm32-wasip2`, so it works with `async-trait` and frameworks like Axum that require `Send` futures. This is enabled by `wstd 0.6+`, which uses `Arc` (instead of `Rc`) internally.

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
use wasi_pg_pool::{Pool, PoolConfig};

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
| `scram` | ✅ | SCRAM-SHA-256 authentication |
| `md5-auth` | ❌ | MD5 authentication (legacy) |
| `pool` | ❌ | Connection pooling (use `wasi-pg-pool` crate) |
| `tracing` | ✅ | Structured logging via tracing |
| `uuid` | ❌ | UUID type support via uuid crate |
| `serde-json` | ❌ | JSON type support via serde_json |
| `chrono` | ❌ | chrono integration for date/time |
| `test-native` | ❌ | Native transport for testing |
| `tokio-transport` | ❌ | Tokio async TCP transport for native builds |

## Project Structure

The library is split into four crates:

| Crate | Purpose | I/O | Async |
|-------|---------|-----|-------|
| `pg-protocol` | Wire protocol encoding/decoding | ❌ | ❌ |
| `pg-types`    | Type system, OID mapping, `ToSql`/`FromSql` | ❌ | ❌ |
| `wasi-pg-client` | Main async client, transport, auth, queries | ✅ | ✅ |
| `wasi-pg-pool`  | Connection pooling | ✅ | ✅ |

## API Stability

This is v0.1 — the public API may change between minor versions (semver pre-1.0).

- `#[non_exhaustive]` on all public enums and structs ensures adding new variants/fields isn't breaking
- Internal `pub(crate)` items can change freely
- The `AsyncTransport` trait is public for custom transports/testing but may evolve

## Testing

```bash
# Workspace validation baseline
cargo check --workspace --all-targets
cargo test --workspace --all-targets --no-run

# Library tests with feature coverage
cargo test -p wasi-pg-client --lib --all-features
cargo test -p wasi-pg-pool --lib --all-features

# E2E harnesses compile but ignored tests are not run by default
cargo test -p wasi-pg-client --features tokio-transport,tls --test e2e_tls --no-run
cargo test -p wasi-pg-pool --features tokio-transport --test e2e_pool --no-run

# Build for WASI P2
cargo build --workspace --target wasm32-wasip2
```

On Windows, run the validation commands from **WSL** so they execute in the same Linux-style environment used by the project's native verification flow.

## CI/CD (Google Cloud Build)

This repository is set up to use **Google Cloud Build** rather than GitHub Actions.
The root `cloudbuild.yaml` runs the Linux-native verification pipeline that mirrors the local WSL checks above:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo check --workspace --all-targets`
- `cargo test --workspace --all-targets --no-run`
- `cargo test -p wasi-pg-client --lib --all-features`
- `cargo test -p wasi-pg-pool --lib --all-features`
- `cargo test -p wasi-pg-client --features tokio-transport,tls --test e2e_tls --no-run`
- `cargo test -p wasi-pg-pool --features tokio-transport --test e2e_pool --no-run`
- `cargo build --workspace --target wasm32-wasip2`

Run it manually with:

```bash
gcloud builds submit --config cloudbuild.yaml .
```

The ignored container-backed E2E tests are compiled in CI to keep the harnesses healthy, but they are not run in the default Cloud Build pipeline.

## Thread Safety

- **WASI P2**: Single-threaded — `RefCell`-based interior mutability in the pool
- **Native (tokio-transport)**: Multi-threaded — `std::sync::Mutex`-based interior mutability in the pool. `Pool` is `Send + Sync`.

## Limitations (WASI Preview 2)

- **Single-threaded only** – no `Send`/`Sync` required, but no parallel queries
- **No background tasks** – pool maintenance is lazy (on acquire)
- **No file system access** – SSL certificates must be embedded (via `webpki-roots`)
- **No process spawning** – cannot run `pg_dump` or external tools
- **Notification timeout** – best-effort on WASI (no true async race with timeout)

## License

Dual-licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.
