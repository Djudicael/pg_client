//! Integration tests for wasi-pg-client.
//!
//! This crate contains:
//! - **common**: Shared test helpers for configuration and assertions
//! - **protocol**: Layer 3 protocol tests (mock transport, full flows)
//! - **reconnect**: Reconnection, retry policy, session state tests
//! - **pool_safety**: Pool RefCell borrow safety, guard lifecycle tests
//! - **tracing**: Tracing event capture and assertion tests
//! - **streaming**: Streaming, cursor, early termination, recovery tests
//!
//! ## Running Tests
//!
//! Protocol tests (no PostgreSQL required):
//! ```bash
//! cargo test -p integration-tests
//! ```
//!
//! Integration tests with real PostgreSQL:
//! ```bash
//! TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/test \
//!   cargo test -p integration-tests --features tokio-transport
//! ```
//!
//! With tracing enabled:
//! ```bash
//! TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/test \
//!   RUST_LOG=wasi_pg_client=debug \
//!   cargo test -p integration-tests --features tokio-transport,tracing
//! ```

pub mod common;
pub mod pool_safety;
pub mod protocol;
pub mod reconnect;
pub mod streaming;
pub mod tracing;
