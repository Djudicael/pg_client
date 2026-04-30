# Plan Changelog

This document tracks the evolution of the `wasi-pg-client` implementation plan, including design corrections, discovered limitations, and step-by-step implementation status.

---

## [Unreleased] - Plan Improvements

### Added
- **PREREQUISITES.md**: New document covering Rust 1.87+ requirement, WSL setup, wasmtime installation, PostgreSQL Docker setup, IDE configuration, and troubleshooting guide.
- **PLAN.md - Known Limitations table**: Documents real constraints discovered during step-01 implementation (`wstd` has no `TcpStream::connect`, no `spawn` in WASI P2, `rustls-rustcrypto` immaturity, `dyn AsyncTransport` not supported, `proptest` doesn't compile for WASM).
- **PLAN.md - Future Enhancements table**: Tracks post-v0.1 features (SCRAM-SHA-256-PLUS, connection multiplexing, pipeline API, native `wstd::net::connect`, WASI P3 threading, enterprise auth, JSONB binary, array/composite types, COPY stream adapters).
- **step-06-connection.md - Connection State Machine section**: Formal `ConnectionState` enum with 12 states and transition diagram. Prevents protocol violations (double query, query during auth, commit during stream).

### Changed
- **step-01-project-setup.md**: Updated `getrandom` version from `0.3` to `0.4` (actual workspace version with WASI P2 auto-detection).
- **step-01-project-setup.md**: Added explicit warning against `[build] target = "wasm32-wasip2"` in `.cargo/config.toml` because it breaks native test compilation (dev-dependencies like `proptest`, `wait-timeout` don't compile for WASM).
- **step-01-project-setup.md**: Rewrote smoke test example to use raw `wasip2::sockets::tcp` since `wstd::net::TcpStream::connect` does not exist in wstd 0.5.x.
- **step-01-project-setup.md**: Updated acceptance criteria to reflect that raw `wasip2::sockets::tcp` connect is used instead of `wstd::net::TcpStream::connect`.
- **step-02-tcp-transport.md**: Complete rewrite of Context and Tasks 2.1/2.2 to reflect that client TCP connections use raw `wasip2::sockets::tcp` bindings (`create_tcp_socket`, `start_connect`, `finish_connect`) wrapped in `AsyncInputStream`/`AsyncOutputStream`, not `wstd::net::TcpStream::connect`.
- **step-03-tls-support.md**: Updated `getrandom` version reference from `0.3` to `0.4`.
- **PLAN.md**: Added Rust 1.87+ minimum version note. Added `wasi:sockets/tcp_create_socket` and `wasi:sockets/network` to WASI P2 interfaces table. Added step-02 note about raw socket usage.

### Fixed
- **step-01-project-setup.md `.cargo/config.toml` example**: Removed non-standard `[target.wasm32-wasip2.dependencies]` table (not a real Cargo feature). Clarified that feature flags belong in individual crate `Cargo.toml` files.
- **PLAN.md Non-Goals**: Removed duplicated/merged markdown formatting issues that appeared during editing.

---

## Step-by-Step Implementation Status

### ✅ Step 01 — Project Setup & WASI P2 Foundation
**Status**: Implemented and passing all checks.

| Criterion | Status | Notes |
|-----------|--------|-------|
| Workspace structure | ✅ | `crates/pg-protocol`, `pg-types`, `pg-client`, `pg-pool` |
| `Cargo.toml` files | ✅ | All workspace deps and feature flags configured |
| `rust-toolchain.toml` | ✅ | `stable`, `wasm32-wasip2`, `rustfmt`, `clippy` |
| `.cargo/config.toml` | ✅ | Runner config only; no default `build.target` |
| `getrandom` configuration | ✅ | v0.4 with auto-detection; runtime sanity check added |
| CI pipeline (`ci.yml`) | ✅ | lint, unit-tests, wasi-build, integration-tests, e2e-wasi, audit |
| Async smoke test | ✅ | Compiles and runs in wasmtime; uses raw wasip2 sockets |
| `wstd` async runtime | ✅ | `#[wstd::main]`, `wstd::task::sleep`, `AsyncPollable` verified |
| `rustls` crypto provider strategy | ✅ | `rustls-rustcrypto` v0.0.2-alpha compiles for WASI |
| Native test transport | ✅ | `NativeTcpTransport` behind `test-native` feature |
| `cargo clippy` | ✅ | Passes with `-D warnings` on `--all-targets --all-features` |
| `cargo fmt` | ✅ | Passes `--check` |

**Build verification** (WSL):
```bash
cargo clippy --all-targets --all-features -- -D warnings   # ✅
cargo test -p pg-protocol -p pg-types --all-features       # ✅
cargo build --target wasm32-wasip2 --all-features          # ✅
cargo build --target wasm32-wasip2 -p smoke-test           # ✅
wasmtime run --wasi inherit-network target/wasm32-wasip2/debug/smoke-test.wasm  # ✅
```

### ⬜ Step 02 — Async TCP Transport Layer
**Status**: Scaffolding in place; implementation pending.

| Component | Status | Notes |
|-----------|--------|-------|
| `AsyncTransport` trait | ✅ | Defined in `crates/pg-client/src/transport.rs` |
| `BufferedTransport<T>` | ✅ | Stub implementation present |
| `WasiTcpTransport` | ⬜ | Needs raw wasip2 socket implementation (design documented) |
| Timeout support | ⬜ | `futures-concurrency::Race` or manual poll-based |
| DNS resolution | ⬜ | `std::net::ToSocketAddrs` on WASI P2 |
| `NativeTcpTransport` | ✅ | Implemented behind `test-native` feature |

**Blockers**: None. Ready to implement.

### ⬜ Step 03 — TLS Support (Async)
**Status**: Dependencies configured; implementation pending.

| Component | Status | Notes |
|-----------|--------|-------|
| `rustls` + `rustls-rustcrypto` | ✅ | Compiles for `wasm32-wasip2` |
| `TlsConfig` / `SslMode` | ⬜ | Not yet created |
| `TlsTransport<T>` | ⬜ | Async TLS wrapper around `AsyncTransport` |
| PostgreSQL SSLRequest negotiation | ⬜ | Single-byte `S`/`N` response handling |
| `PgTransport` enum (Plain/Tls) | ⬜ | Delegation wrapper |
| Certificate verification | ⬜ | `webpki-roots`, custom CA, `NoVerifier` for tests |

### ⬜ Step 04 — PostgreSQL Wire Protocol (Message Codec)
**Status**: Scaffolding in place; full codec pending.

| Component | Status | Notes |
|-----------|--------|-------|
| `ProtocolError` | ✅ | Defined with comprehensive variants |
| `MessageEncoder` / `MessageDecoder` | ✅ | Stub traits exist |
| `FrontendMessage` enum | ⬜ | Needs all variants |
| `BackendMessage` enum | ⬜ | Needs all variants |
| `ReadBuffer` | ⬜ | Partial message handling |
| Round-trip encode/decode tests | ⬜ | |
| Fuzz tests | ⬜ | `cargo fuzz` targets |

### ⬜ Step 05 — Authentication (Async)
**Status**: Not started.

| Component | Status | Notes |
|-----------|--------|-------|
| Trust auth | ⬜ | Trivial pass-through |
| Cleartext password | ⬜ | Send `PasswordMessage` |
| MD5 auth | ⬜ | `md-5` crate, salt handling |
| SCRAM-SHA-256 | ⬜ | `sha2`, `hmac`, `pbkdf2`, `base64` — most complex |

### ⬜ Step 06 — Connection Management (Async)
**Status**: Stub scaffolding; full lifecycle pending.

| Component | Status | Notes |
|-----------|--------|-------|
| `Config` struct | ✅ | Basic builder pattern |
| `Connection` struct | ✅ | Stub with `connect()`/`close()` |
| Connection state machine | ⬜ | Design added to plan; not yet implemented |
| Connection string parser | ⬜ | URI + key-value formats |
| `from_env()` support | ⬜ | `PGHOST`, `PGPORT`, etc. |
| `TargetSessionAttrs` | ⬜ | `Any`, `ReadWrite`, `ReadOnly` |

### ⬜ Step 07 — Simple Query Protocol
**Status**: Not started.

### ⬜ Step 08 — Extended Query Protocol
**Status**: Not started.

### ⬜ Step 09 — Type System
**Status**: Basic scaffolding.

| Component | Status | Notes |
|-----------|--------|-------|
| `ToSql` / `FromSql` traits | ✅ | Defined with `Format` parameter |
| `i32`, `String`, `Option<T>` impls | ✅ | Text + binary formats |
| OID constants | ✅ | Basic constants in `oid.rs` |
| All PG types | ⬜ | `bool`, `i16`, `i64`, `f32`, `f64`, `Vec<u8>`, `chrono`, `uuid`, etc. |

### ⬜ Step 10 — Transactions
**Status**: Not started.

### ⬜ Step 11 — COPY Protocol
**Status**: Not started.

### ⬜ Step 12 — Listen/Notify/Cancel
**Status**: Not started.

### ⬜ Step 13 — Error Handling & Resilience
**Status**: Partial scaffolding.

| Component | Status | Notes |
|-----------|--------|-------|
| `Error` enum in `pg-client` | ✅ | Comprehensive variants |
| `PgServerError` with SQLSTATE | ⬜ | Not yet implemented |
| `TransportError` | ⬜ | Defined in step-02 plan; not in code |
| Retry policies | ⬜ | |
| Connection health checks | ⬜ | |

### ⬜ Step 14 — Streaming API
**Status**: Design complete; not implemented.

**Key design decision**: Streaming is the **primary** API. Convenience methods (`query() -> Vec<Row>`) are built on top of `RowStream` by collecting.

### ⬜ Step 15 — Connection Pooling
**Status**: Stub scaffolding.

| Component | Status | Notes |
|-----------|--------|-------|
| `Pool` struct | ✅ | Stub exists |
| `PoolConfig` | ✅ | Stub exists |
| Channel-based acquire/release | ⬜ | |
| Health checks / idle timeout | ⬜ | Lazy (on-acquire) due to no `spawn` |
| `PoolGuard` RAII | ⬜ | |

### ⬜ Step 16 — Reconnection
**Status**: Not started.

### ⬜ Step 17 — Structured Logging & Tracing
**Status**: Dependency configured; not instrumented.

| Component | Status | Notes |
|-----------|--------|-------|
| `tracing` feature flag | ✅ | In `pg-client` and `pg-pool` |
| Instrumentation spans | ⬜ | Not yet added to code |
| Redaction of sensitive data | ⬜ | Design documented |

### ⬜ Step 18 — Testing Strategy
**Status**: CI configured; test suites minimal.

| Component | Status | Notes |
|-----------|--------|-------|
| Unit tests (pg-protocol, pg-types) | ✅ | Doc-tests pass; no dedicated unit tests yet |
| `MockTransport` | ⬜ | Design documented |
| Integration tests (native + PG) | ⬜ | CI job ready; no `tests/integration.rs` yet |
| E2E WASI tests | ⬜ | CI job ready; no `examples/e2e-test` yet |
| Fuzz tests | ⬜ | No `fuzz/` directory yet |
| Property-based tests | ⬜ | `proptest` configured; no tests written |

### ⬜ Step 19 — Public API Design & Documentation
**Status**: Partial.

| Component | Status | Notes |
|-----------|--------|-------|
| `#[non_exhaustive]` | ⬜ | Not yet applied |
| `#[must_use]` on `Result` methods | ⬜ | |
| Builder pattern for `Config` | ✅ | Basic builder exists |
| README quick-start | ⬜ | |
| API examples | ⬜ | Design documented |

---

## Design Corrections Log

| Date | Issue | Original Assumption | Corrected Understanding | Impact |
|------|-------|--------------------|------------------------|--------|
| — | `wstd::net::TcpStream::connect` | Assumed `wstd` 0.5.x has client TCP connect like `tokio::net::TcpStream` | `wstd` 0.5.x only exposes `TcpListener::bind` (server-side). Client connect requires raw `wasip2::sockets::tcp` bindings. | step-01 smoke test rewritten; step-02 transport design rewritten |
| — | `.cargo/config.toml` default target | Assumed setting `[build] target = "wasm32-wasip2"` was best practice | Breaks `cargo clippy --all-targets --all-features` and `cargo test` because dev-dependencies (`proptest`, `wait-timeout`) don't compile for WASM. | Removed default target; pass `--target wasm32-wasip2` explicitly |
| — | `getrandom` version | Plan specified `0.3` | Workspace uses `0.4` which has improved WASI P2 auto-detection | Updated all plan references to `0.4` |
| — | Rust minimum version | Assumed 1.78 (first stable with `wasm32-wasip2`) | `wstd` 0.5.6 requires Rust 1.87+ | Documented in PLAN.md and PREREQUISITES.md |
| — | Connection state management | Implicit state tracking inside `Connection` | Added explicit `ConnectionState` enum with 12 states and monotonic transition diagram | Added to step-06 plan; prevents protocol violations |

---

## How to Update This Changelog

When modifying the plan or implementing a step:

1. **Add a new section** under `[Unreleased]` describing what changed.
2. **Update the Step-by-Step table** for the relevant step.
3. **Add an entry to the Design Corrections Log** if a fundamental assumption was wrong.
4. **Move `[Unreleased]` to a dated release section** when a milestone is reached (e.g., "Phase 1 Complete").

```markdown
## [YYYY-MM-DD] - Phase X Complete

### Added
- ...

### Changed
- ...

### Fixed
- ...
```
