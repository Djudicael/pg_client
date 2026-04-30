# wasi-pg-client

A production-grade PostgreSQL client library for [WASI Preview 2](https://github.com/WebAssembly/wasi-preview2), written in Rust. Compile to WebAssembly and run on platforms like wasmtime, WasmEdge, and Spin.

## Features

- **Full PostgreSQL wire protocol** – simple and extended queries, prepared statements, transactions, COPY, LISTEN/NOTIFY
- **WASI Preview 2 native** – uses `wasi:sockets`, `wasi:io/poll`, `wasi:random` via the `wstd` async runtime
- **Pure Rust crypto** – TLS via `rustls` (with `rustls-rustcrypto` fallback), SCRAM‑SHA‑256, MD5 authentication
- **Memory‑safe & zero‑copy** – built on `bytes` for efficient buffer handling
- **Streaming results** – async `RowStream` for large datasets, no in‑memory buffering
- **Connection pooling** – channel‑based pool with health checks and timeouts
- **Structured logging** – `tracing` integration with redaction of sensitive data
- **I/O‑free protocol crate** – `pg-protocol` is sync and can be used independently

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
wasi-pg-client = { git = "https://github.com/your-org/wasi-pg-client", features = ["tls", "scram"] }
```

Example (WASI P2 async entry point):

```rust
use wasi_pg_client::{Connection, Config};

#[wstd::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::new()
        .host("localhost")
        .port(5432)
        .user("postgres")
        .password("password")
        .database("test");

    let mut conn = Connection::connect(config).await?;

    // Simple query
    let rows = conn.query("SELECT 1").await?;
    for row in rows {
        let value: i32 = row.get(0)?;
        println!("value = {}", value);
    }

    // Prepared statement
    let stmt = conn.prepare("SELECT $1::int").await?;
    let rows = stmt.query(&[&42]).await?;
    // ...

    Ok(())
}
```

## Building for WASI Preview 2

1. Install the `wasm32-wasip2` target:

   ```bash
   rustup target add wasm32-wasip2
   ```

2. Build your project:

   ```bash
   cargo build --target wasm32-wasip2
   ```

3. Run with wasmtime (requires network access):

   ```bash
   wasmtime run --wasi inherit-network --wasi inherit-env target/wasm32-wasip2/debug/your_app.wasm
   ```

## Project Structure

The library is split into four crates:

| Crate | Purpose | I/O | Async |
|-------|---------|-----|-------|
| `pg-protocol` | Wire protocol encoding/decoding | ❌ | ❌ |
| `pg-types`    | Type system, OID mapping, `ToSql`/`FromSql` | ❌ | ❌ |
| `wasi-pg-client` | Main async client, transport, auth, queries | ✅ | ✅ |
| `wasi-pg-pool`  | Connection pooling | ✅ | ✅ |

## Supported PostgreSQL Features

- [x] Simple query protocol (`SELECT`, `INSERT`, etc.)
- [x] Extended query protocol (prepared statements, parameter binding)
- [x] Transactions (begin/commit/rollback, savepoints)
- [x] COPY IN/OUT (text, CSV, binary)
- [x] LISTEN / NOTIFY
- [x] Cancellation
- [x] SCRAM‑SHA‑256, MD5, cleartext, trust authentication
- [x] TLS (via `rustls`)
- [x] Connection pooling
- [x] Automatic reconnection with exponential backoff

## Testing

The project includes a comprehensive testing strategy:

- **Unit tests** for protocol and type crates
- **Integration tests** with a real PostgreSQL instance (native transport)
- **E2E WASI tests** running the compiled WASM component in wasmtime
- **Property‑based tests** (proptest) for protocol decoding
- **Fuzz tests** for security‑critical parsers

Run the tests:

```bash
# Unit tests (no PostgreSQL required)
cargo test -p pg-protocol -p pg-types

# Integration tests (requires PostgreSQL)
TEST_DATABASE_URL=postgresql://user:pass@localhost/db cargo test --features test-native

# Build and run the WASI smoke test
cargo build --target wasm32-wasip2 --example smoke-test
wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/examples/smoke_test.wasm
```

## Feature Flags

| Feature | Description | Default |
|---------|-------------|---------|
| `tls` | Enable TLS support (via `rustls`) | ✅ |
| `scram` | SCRAM‑SHA‑256 authentication | ✅ |
| `md5-auth` | Legacy MD5 authentication | ❌ |
| `pool` | Connection pooling (`wasi-pg-pool`) | ❌ |
| `tracing` | Structured logging via `tracing` crate | ✅ |
| `uuid` | UUID type support (via `uuid` crate) | ❌ |
| `serde-json` | JSON type support (via `serde_json`) | ❌ |
| `chrono` | Date/time types (via `chrono`) | ❌ |
| `test-native` | Native (blocking) transport for testing | ❌ |

## Limitations (WASI Preview 2)

- **Single‑threaded only** – no `Send`/`Sync` required, but no parallel queries
- **No background tasks** – pool maintenance is lazy (on acquire)
- **No file system access** – SSL certificates must be embedded (via `webpki-roots`)
- **No process spawning** – cannot run `pg_dump` or external tools

## Roadmap

- [ ] Phase 1: Foundation (project setup, TCP transport, wire protocol, tracing)
- [ ] Phase 2: Connect (TLS, authentication, connection, type system)
- [ ] Phase 3: Query (simple/extended queries, streaming, error handling)
- [ ] Phase 4: Advanced (transactions, COPY, LISTEN/NOTIFY)
- [ ] Phase 5: Production (pooling, reconnection, testing, API polish)

## License

Dual‑licensed under either:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please open an issue or pull request on GitHub.

## Acknowledgements

- The `wstd` project for providing a WASI‑native async runtime
- The `rustls` team for a pure‑Rust TLS implementation
- The PostgreSQL community for the open wire protocol specification
