# wasi-pg-client - Implementation Plan

A production-grade PostgreSQL client library for WASI Preview 2, written in Rust. Native SQL queries (no ORM), with full transaction support, prepared statements, COPY, notifications, connection pooling, and streaming results.

> **Prerequisites**: See [PREREQUISITES.md](PREREQUISITES.md) for full environment setup. In brief: Rust 1.87+, `wasm32-wasip2` target, and `wasmtime` CLI.

## Architecture Overview

```
┌───────────────────────────────────────────────────────────────┐
│                      Public API (pg-client)                    │
│  Connection, Query, Transaction, COPY, Pool, Stream, Config   │
├──────────────┬──────────────┬──────────────┬──────────────────┤
│   pg-types   │ pg-protocol  │   pg-pool    │  pg-client       │
│  ToSql/      │ Encode/      │  Pool with   │  Transport       │
│  FromSql     │ Decode       │  channels    │  Auth/Conn       │
│  OID mapping │ Wire format  │  Health      │  Query/Stream    │
├──────────────┴──────────────┴──────────────┴──────────────────┤
│                    Transport Layer                              │
│  TCP (wasi:sockets) + TLS (rustls) + Buffered I/O             │
│  AsyncTransport trait (no dyn, generic only)                   │
├───────────────────────────────────────────────────────────────┤
│                    Async Runtime                                │
│  wstd (WASI 0.2 native) OR tokio (native test fallback)       │
├───────────────────────────────────────────────────────────────┤
│                    WASI Preview 2                               │
│  wasi:sockets/tcp | wasi:io/streams | wasi:io/poll            │
│  wasi:random      | wasi:clocks     | wasi:cli/env            │
└───────────────────────────────────────────────────────────────┘
```

## Implementation Steps

| Step | File | Description | Dependencies |
|------|------|-------------|-------------|
| **01** | [step-01-project-setup.md](step-01-project-setup.md) | Project setup, workspace, WASI P2 target, CI, getrandom | None |
| **02** | [step-02-tcp-transport.md](step-02-tcp-transport.md) | TCP transport via `wasi:sockets`, Transport trait, DNS, timeouts | Step 01 |
| **03** | [step-03-tls-support.md](step-03-tls-support.md) | TLS via `rustls`, PG SSL negotiation, certificate handling | Step 02 |
| **04** | [step-04-wire-protocol.md](step-04-wire-protocol.md) | PG v3 wire protocol codec (encode/decode), I/O-free | Step 01 |
| **05** | [step-05-authentication.md](step-05-authentication.md) | Auth: Trust, Cleartext, MD5, SCRAM-SHA-256 | Step 02, 04 |
| **06** | [step-06-connection.md](step-06-connection.md) | Connection struct, config parsing, connection string, lifecycle | Step 02, 03, 04, 05 |
| **07** | [step-07-simple-query.md](step-07-simple-query.md) | Simple query protocol, Row, QueryResult, streaming | Step 06 |
| **08** | [step-08-extended-query.md](step-08-extended-query.md) | Extended query, prepared statements, pipelines, cursors | Step 06, 07 |
| **09** | [step-09-type-system.md](step-09-type-system.md) | Type system: ToSql/FromSql, all PG types, binary/text | Step 04 |
| **10** | [step-10-transactions.md](step-10-transactions.md) | Transactions, savepoints, RAII guards, isolation levels | Step 07 |
| **11** | [step-11-copy-protocol.md](step-11-copy-protocol.md) | COPY IN/OUT, text/CSV/binary format, streaming | Step 06 |
| **12** | [step-12-listen-notify-cancel.md](step-12-listen-notify-cancel.md) | LISTEN/NOTIFY, async messages, query cancellation | Step 06 |
| **13** | [step-13-error-handling.md](step-13-error-handling.md) | Error types, SQLSTATE, health checks, retry helpers | Step 06 |
| **14** | [step-14-streaming-api.md](step-14-streaming-api.md) | Async stream/iterator for query results, backpressure | Step 07, 08 |
| **15** | [step-15-connection-pooling.md](step-15-connection-pooling.md) | Connection pool, channels, lifecycle, health, RAII guard | Step 06 |
| **16** | [step-16-reconnection.md](step-16-reconnection.md) | Automatic reconnection, connection resilience, retry policies | Step 06, 13 |
| **17** | [step-17-logging-tracing.md](step-17-logging-tracing.md) | Structured logging via `tracing`, configurable levels, redaction | Step 06 |
| **18** | [step-18-testing-strategy.md](step-18-testing-strategy.md) | Testing: unit, mock, integration, E2E WASI, fuzz, CI | All |
| **19** | [step-19-api-design.md](step-19-api-design.md) | Public API, docs, examples, feature flags, versioning | All |

## Recommended Build Order

**Phase 1 - Foundation** (Steps 01, 02, 04, 17)
- Set up the project and build system
- Implement TCP transport and wire protocol codec
- Add logging/tracing infrastructure early (so all subsequent steps can emit diagnostics)
- Steps 02 and 04 are independent and can be developed in parallel

> **Note on Step 02**: `wstd::net::TcpStream` does **not** expose `connect()` in wstd 0.5.x. The actual TCP client connect uses raw `wasip2::sockets::tcp` bindings (via `wstd::wasip2`) and is implemented in `pg-client/src/transport/tcp.rs`.

**Phase 2 - Connect** (Steps 03, 05, 06, 09)
- TLS, authentication, and connection establishment
- Type system (independent, can be parallel with auth)
- Step 09 can start in parallel with Steps 03/05 since it only depends on Step 04

**Phase 3 - Query** (Steps 07, 08, 14, 13)
- Simple query protocol first (simpler, validates the stack)
- Extended query protocol (parameterized queries, prepared statements)
- Streaming API (memory-efficient result processing)
- Error handling refinement

**Phase 4 - Advanced** (Steps 10, 11, 12)
- Transactions and savepoints
- COPY protocol
- LISTEN/NOTIFY and cancellation

**Phase 5 - Production** (Steps 15, 16, 18, 19)
- Connection pooling (channel-based design)
- Automatic reconnection and resilience
- Comprehensive testing
- API polish and documentation

## Key Design Decisions

1. **Fully async I/O via `wstd`**: The library is async from the ground up, using the `wstd` crate which provides a native WASI P2 async runtime powered by `wasi:io/poll`. No `tokio` or `async-std` needed on WASI. Since WASI P2 is single-threaded, no `Send`/`Sync` bounds are required on futures, which simplifies the async design significantly.

   > **Minimum Rust version**: 1.87 (required by `wstd` 0.5.6). The `rust-toolchain.toml` uses `stable`, so keep your toolchain up to date.

2. **I/O-free protocol crate**: `pg-protocol` has zero I/O dependencies. It operates on byte buffers only (sync). This makes it testable, portable, and reusable. Only `pg-client` and `pg-pool` are async (they do I/O).

3. **Pure Rust crypto**: No C dependencies. Uses `rustls` (with `rustls-ring` when available, `rustls-rustcrypto` as WASI fallback) for TLS, `RustCrypto` crates for SCRAM/MD5. Everything compiles to `wasm32-wasip2`.

4. **No ORM**: Raw SQL with parameterized queries. The library provides type-safe parameter binding and result decoding, but no query builders or schema abstraction.

5. **RAII safety with explicit async cleanup**: Transactions and pool connections use guard types that clean up on Drop (best-effort). Since `Drop` cannot be async, cleanup in Drop is limited to marking the guard as "needs cleanup" — the actual rollback/release happens lazily on next interaction or explicitly via `.commit().await` / `.rollback().await`. Users **must** prefer explicit async cleanup.

6. **Workspace crates**: Separation into `pg-protocol`, `pg-types`, `pg-client`, `pg-pool` keeps concerns clean and allows independent testing/reuse.

7. **No `Send`/`Sync` bounds on WASI**: WASI P2 is single-threaded. Async traits use `async fn` directly (Rust 1.75+), no `Pin<Box<dyn Future + Send>>` needed. However, we use **generic** `async fn` in traits (not `dyn` dispatch) because `dyn` with `async fn` is not yet stable.

8. **Streaming results by default**: Large query results are not buffered entirely in memory. The primary query API returns a `RowStream` that yields rows asynchronously. Convenience methods like `.query()` that collect into `Vec<Row>` are built on top of the stream.

9. **Channel-based pool**: The connection pool uses an async channel (`async_channel` or a WASI-compatible equivalent) instead of `&mut Pool` borrowing. This allows the pool to be shared more flexibly even in single-threaded async contexts.

10. **Structured logging via `tracing`**: All internal operations emit `tracing` spans and events. Users can enable/disable logging at any granularity. Sensitive data (passwords, query parameters) is redacted by default.

## WASI P2 Interfaces Used

| Interface | Purpose | Notes |
|-----------|---------|-------|
| `wasi:sockets/tcp` | TCP connections | Via `wstd::net::TcpStream` |
| `wasi:sockets/ip-name-lookup` | DNS resolution | Via `wstd::net` |
| `wasi:io/streams` | Read/write streams | Via `wstd::io` |
| `wasi:io/poll` | Blocking/waiting, timeouts | Drives async executor |
| `wasi:random/random` | Cryptographic randomness | SCRAM nonce, TLS. Must configure `getrandom` crate |
| `wasi:clocks/monotonic-clock` | Timeouts, pool expiry | Via `wstd::time` |
| `wasi:cli/environment` | Read env vars | PGHOST, PGUSER, etc. Via `std::env` |
| `wasi:sockets/tcp_create_socket` | Client TCP connect | Raw socket creation when `wstd` lacks client connect |
| `wasi:sockets/network` | Network instance | Required for `start_connect` / `start_bind` |

### WASI P2 Compatibility Notes

- **`getrandom` configuration**: The `getrandom` crate (v0.4 in this project) auto-detects `wasm32-wasip2` and uses `wasi:random/random`. A runtime sanity check (`ensure_random_available()`) runs at connection startup to catch misconfiguration early. The `.cargo/config.toml` must **not** set a default `build.target = "wasm32-wasip2"` because that breaks native test compilation (dev-dependencies like `proptest` and `wait-timeout` don't compile for WASM).
- **`std::net` availability**: On `wasm32-wasip2`, `std::net` types are available but delegate to WASI interfaces. `wstd` provides the async wrappers we need.
- **`Instant` availability**: `std::time::Instant` works on `wasm32-wasip2` via `wasi:clocks/monotonic-clock`.
- **No `spawn`**: WASI P2 has no concept of spawning tasks. All async work is cooperative within a single `#[wstd::main]` entry point. The pool cannot run background maintenance tasks.
- **Component Model**: When compiled to `wasm32-wasip2`, the output is a WASI Component (not just a bare module). The component adapter handles the WIT interface translations automatically.

## Target Runtimes

| Runtime | WASI P2 Sockets | Status | Notes |
|---------|----------------|--------|-------|
| **wasmtime** | ✅ Full support | Primary target | Use `--wasi inherit-network` |
| **WasmEdge** | ✅ Supported | Verified | WASI P2 socket support available |
| **Spin** (Fermyon) | ⚠️ Limited | Needs adapter | Outbound networking via Spin SDK |
| **wazero** (Go) | ⚠️ Partial | Verify | WASI P2 support evolving |
| **Wasmer** | ⚠️ Partial | Verify | WASI P2 socket support in progress |

## Known Limitations (v0.1)

| Limitation | Reason | Workaround |
|------------|--------|------------|
| `wstd` has no `TcpStream::connect` | wstd 0.5.x only exposes `TcpListener` | Use raw `wasip2::sockets::tcp` in `pg-client/src/transport/tcp.rs` |
| No background `spawn` in WASI P2 | WASI P2 is single-threaded, no task spawning | Pool maintenance is lazy (on acquire). No background health checks. |
| `rustls-rustcrypto` is immature (v0.0.x) | Pure-Rust crypto, limited audit | Default for WASI. Allow user-provided `CryptoProvider` via `ClientConfig`. |
| `dyn AsyncTransport` not supported | `async fn` in traits + `dyn` is not yet stable | Use generic dispatch (`impl AsyncTransport`) everywhere. |
| `proptest` doesn't compile for WASI | Native-only dependencies | Native tests only. CI runs them on `ubuntu-latest`. |

## Non-Goals (v0.1)

- **Multi-threaded pool**: WASI P2 is single-threaded; no concurrent pool access from multiple threads
- **ORM / query builder**: Raw SQL only; no schema abstraction, migrations, or query DSL
- **Migration framework**: Out of scope for a client library
- **Connection multiplexing**: No PgBouncer-style multiplexing over a single TCP connection
- **`tokio` / `async-std` runtime**: Uses `wstd` native WASI async; a native test fallback uses blocking I/O
- **GSSAPI / SSPI authentication**: Enterprise auth mechanisms; not available in WASI
- **SCRAM-SHA-256-PLUS**: Channel binding variant; deferred to v0.2 (requires TLS channel binding support)
- **Large Object API**: PostgreSQL's `lo_*` functions; rarely used, deferred
- **Replication protocol**: Physical/logical replication; out of scope

## Future Enhancements (Post v0.1)

| Feature | Description | Estimated Complexity |
|---------|-------------|---------------------|
| **SCRAM-SHA-256-PLUS** | Channel-bound SCRAM (tls-unique / tls-exporter) | Medium — needs TLS exporter API from rustls |
| **Connection multiplexing** | PgBouncer-style multiplex over single TCP | High — significant protocol state redesign |
| **Pipeline / batch API** | Send multiple queries without waiting for each response | Medium — queue of in-flight requests |
| **Native `wstd::net::connect`** | Migrate to upstream `TcpStream::connect` when wstd adds it | Low — swap transport implementation |
| **WASI P3 threading** | Add `Send`/`Sync` bounds, multi-threaded pool | Medium — mostly adding trait bounds |
| ** Kerberos / LDAP auth** | Enterprise authentication mechanisms | High — needs external crates / host delegation |
| **JSONB binary format** | More efficient JSONB encoding than text | Low — add binary parser to pg-types |
| **Array / composite types** | Full support for PG arrays and row types | Medium — recursive type encoding |
| **COPY FROM / TO `AsyncRead`/`AsyncWrite`** | Stream COPY data from arbitrary async sources | Low — trait-based adapter |

## Key Risks and Mitigations

| Risk | Severity | Mitigation |
|------|----------|------------|
| `wstd` crate maturity | Medium | Fallback: use raw `wasi:sockets` bindings directly. Maintain a thin abstraction layer so swapping is localised. |
| `rustls-rustcrypto` immaturity | High | Fallback: try `rustls-ring` first (may work on recent wasmtime). If neither works, support plaintext-only mode with clear warning. |
| `getrandom` misconfiguration | High | Document clearly. Add a compile-time check or runtime panic with helpful message. Pin `getrandom` v0.3+ which has better WASI auto-detection. |
| No background tasks in WASI P2 | Medium | Pool maintenance is lazy (on acquire). Reconnection is explicit or on-next-use. No `spawn` needed. |
| `async fn` in traits limitations | Low | Use generic dispatch only (no `dyn AsyncTransport`). For test mocking, use generics with `MockTransport: AsyncTransport`. |
| Single-threaded bottleneck | Low | Accept this as a WASI P2 constraint. WASI P3 may add threading. Design pool to be upgrade-compatible. |

## Crate Dependency Graph

```
pg-types (sync, no I/O)
    └── (used by) pg-protocol (sync, no I/O)
            └── (used by) pg-client (async, I/O)
                    └── (used by) pg-pool (async, I/O)

pg-client depends on:
    pg-protocol, pg-types, wstd, rustls, RustCrypto crates, tracing

pg-pool depends on:
    pg-client, tracing
```

## Feature Flags

```toml
[features]
default = ["tls", "scram", "tracing"]

# TLS support via rustls
tls = ["dep:rustls", "dep:webpki-roots", "dep:rustls-rustcrypto"]

# SCRAM-SHA-256 authentication (recommended, PG 10+ default)
scram = ["dep:hmac", "dep:sha2", "dep:pbkdf2", "dep:base64"]

# MD5 authentication (legacy, less secure)
md5-auth = ["dep:md-5"]

# Connection pooling
pool = ["dep:wasi-pg-pool"]

# Structured logging via tracing crate
tracing = ["dep:tracing"]

# UUID type support via uuid crate
uuid = ["dep:uuid"]

# JSON type support via serde_json
serde-json = ["dep:serde_json"]

# chrono integration for date/time types
chrono = ["dep:chrono"]

# Native test support (blocking I/O transport for non-WASI testing)
test-native = []
```

## WASI P3 Migration Path

The library is designed for easy migration when WASI P3 arrives:
- **Async**: Already fully async; no changes needed to `.await` patterns
- **Threading**: Pool can gain `Send`/`Sync` + `Arc` wrappers; internal channel design is compatible
- **Runtime**: `wstd` can be swapped for WASI P3's native async Component Model support
- **Public API**: Stays identical; only internal runtime layer changes
</arg_value>
