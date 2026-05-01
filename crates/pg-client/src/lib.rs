//! PostgreSQL client library for WASI Preview 2.
//!
//! This crate provides an asynchronous PostgreSQL client that runs on WASI Preview 2.
//! It uses the `wstd` crate for async I/O and supports the full PostgreSQL wire protocol,
//! including TLS, authentication, prepared statements, transactions, and streaming results.
//!
//! # Design
//!
//! The library is built around the following core abstractions:
//! - **Transport**: A trait for asynchronous I/O (TCP, TLS, buffering).
//! - **Connection**: A connection to a PostgreSQL server, managing authentication and protocol state.
//! - **Query**: Execution of simple and extended queries.
//! - **Transaction**: RAII guard for transactions and savepoints.
//! - **RowStream**: Async stream of rows for memory-efficient result processing.
//!
//! # Example
//! ```no_run
//! use wasi_pg_client::{Connection, Config};
//!
//! #[wstd::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config::new()
//!         .host("localhost")
//!         .port(5432)
//!         .user("postgres")
//!         .password("password")
//!         .database("test");
//!
//!     let mut conn = Connection::connect(config).await?;
//!
//!     // Simple query
//!     let rows = conn.query("SELECT 1").await?;
//!     for row in rows.iter() {
//!         let value: i32 = row.get(0)?;
//!         println!("value = {}", value);
//!     }
//!
//!     conn.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Note
//! This is a work in progress. The API will evolve.

// Re-export dependencies that are part of the public API.
pub use pg_protocol;
pub use pg_types;

// Internal modules.
mod auth;
mod cancel;
mod config;
mod connection;
pub mod copy;
pub mod error;
mod notification;
mod query;
mod transaction;

// Transport scaffolding — directory module for TCP, TLS, and test transports.
pub mod transport;

// Public API.
pub use cancel::CancelToken;
pub use config::{Config, ConfigError, TargetSessionAttrs};
pub use connection::{Connection, ConnectionState};
pub use copy::{BinaryCopyWriter, CopyFormat, CopyIn, CopyOut};
pub use error::retry;
pub use error::sqlstate;
pub use error::{Error, PgError, PgServerError, Result};
pub use notification::Notification;
pub use query::result::{CommandTag, ExecuteResult, QueryResult};
pub use query::row::{FieldDescription, Row};
pub use query::{Cursor, Pipeline, PipelineResult, PreparedStatement, StatementCache};
pub use query::{Notice, NoticeHandler};
pub use transaction::{IsolationLevel, Savepoint, Transaction, TransactionOptions};

// Prelude for convenient imports.
pub mod prelude {
    pub use super::error::PgServerError;
    pub use super::{Config, Connection, Error, PgError, Result};
    pub use pg_types::{FromSql, ToSql, Type};
}

/// Runtime sanity check that `getrandom` is properly configured for WASI P2.
///
/// Call this early (e.g. during `Connection::connect`) to get a clear panic
/// message instead of a cryptic runtime failure deep inside crypto code.
pub fn ensure_random_available() {
    let mut buf = [0u8; 1];
    if getrandom::fill(&mut buf).is_err() {
        panic!(
            "wasi-pg-client: getrandom failed. \
             Ensure 'getrandom' is compiled with features=[\"custom\"] \
             or a WASI-compatible backend when targeting wasm32-wasip2."
        );
    }
}
