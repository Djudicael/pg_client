//! # wasi-pg-client
//!
//! A production-grade PostgreSQL client library for WASI Preview 2.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use wasi_pg_client::{Connection, Config};
//!
//! #[wstd::main]
//! async fn main() -> Result<(), wasi_pg_client::PgError> {
//!     let config = Config::from_uri("postgresql://user:pass@localhost/mydb")?;
//!     let mut conn = Connection::connect(&config).await?;
//!
//!     let result = conn.query("SELECT id, name FROM users").await?;
//!     for row in result.iter() {
//!         let id: i32 = row.get(0)?;
//!         let name: String = row.get(1)?;
//!         println!("{}: {}", id, name);
//!     }
//!
//!     conn.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Feature Flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `tls` | ✅ | TLS support via rustls |
//! | `scram` | ✅ | SCRAM-SHA-256 authentication |
//! | `md5-auth` | ❌ | MD5 authentication (legacy) |
//! | `pool` | ❌ | Connection pooling |
//! | `tracing` | ✅ | Structured logging via tracing |
//! | `uuid` | ❌ | UUID type support via uuid crate |
//! | `serde-json` | ❌ | JSON type support via serde_json |
//! | `chrono` | ❌ | chrono integration for date/time |
//! | `test-native` | ❌ | Native transport for testing |
//! | `tokio-transport` | ❌ | Tokio async TCP transport for native builds |
//!
//! ## WASI P2 Requirements
//!
//! This library targets `wasm32-wasip2`. When running in wasmtime, use:
//! ```bash
//! wasmtime run --wasi inherit-network --wasi inherit-env component.wasm
//! ```
//!
//! The `getrandom` crate must be configured with `features = ["wasi"]` for
//! cryptographic randomness (required for SCRAM auth and TLS).
//!
//! ## Tracing
//!
//! When the `tracing` feature is enabled, the library emits structured events
//! at the following levels:
//!
//! | Level | What gets logged |
//! |-------|-----------------|
//! | ERROR | Fatal errors: auth failed, TLS handshake failed, reconnection failed |
//! | WARN  | Recoverable problems: connection broken, transaction rolled back |
//! | INFO  | Normal operations: connection established/closed, query completed |
//! | DEBUG | Detailed info: TCP connect, auth method, pool acquire/release |
//! | TRACE | Wire-level detail: every protocol message, full SQL |
//!
//! ⚠️ TRACE may expose sensitive data. Use only in development, never in production.

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

// Reconnection and retry support.
pub mod reconnect;

// Internal tracing helpers (target constants, redaction).
#[cfg(feature = "tracing")]
mod tracing_ext;

// ── Public API re-exports ──

// Core types
pub use cancel::CancelToken;
pub use config::{Config, ConfigError, TargetSessionAttrs};
pub use connection::{Connection, ConnectionState};
pub use copy::{BinaryCopyWriter, CopyFormat, CopyIn, CopyOut};
pub use notification::Notification;
pub use query::result::{CommandTag, ExecuteResult, QueryResult};
pub use query::row::{FieldDescription, Row};
pub use query::stream::RowStream;
pub use query::{
    Cursor, CursorStream, Pipeline, PipelineResult, PreparedStatement, StatementCache,
};
pub use query::{Notice, NoticeHandler};
pub use transaction::{IsolationLevel, Savepoint, Transaction, TransactionOptions};

// Error types
pub use error::sqlstate;
pub use error::{retry, Error, PgError, PgServerError, Result};

// Type system
#[cfg(feature = "serde-json")]
pub use pg_types::JsonB;
pub use pg_types::{FromSql, Oid, ToSql, Type};

// Transport (for custom transports / testing)
pub use transport::{AsyncTransport, TransportError};

// TLS (behind feature flag)
#[cfg(feature = "tls")]
pub use transport::tls::{SslMode, TlsConfig, TlsInfo};

// Reconnection
pub use reconnect::{classify_error, ErrorClass, ReconnectConfig, RetryPolicy, StaleConfig};
pub use reconnect::{ConnectionHealth, SessionState};

// Pool (behind feature flag)
// Note: wasi-pg-pool cannot be a dependency of wasi-pg-client due to
// circular dependency (wasi-pg-pool depends on wasi-pg-client).
// Use the wasi-pg-pool crate directly for connection pooling:
//   use wasi_pg_pool::{Pool, PoolConfig, PoolGuard, PoolStatus, PoolError};
// The `pool` feature flag in wasi-pg-client is a marker for downstream crates
// to detect pooling support availability.

// Protocol types (for advanced use)
pub use pg_protocol::types::{FormatCode, TransactionStatus};
pub use pg_protocol::BackendMessage;
pub use pg_protocol::FrontendMessage;

/// Builder type alias for [`Config`].
///
/// The `Config` type already uses the builder pattern via its `new()` method
/// and chainable setters. This alias is provided for discoverability.
pub type ConfigBuilder = Config;

// Prelude for convenient imports.
pub mod prelude {
    //! Common imports for working with wasi-pg-client.
    //!
    //! ```rust,no_run
    //! use wasi_pg_client::prelude::*;
    //! ```

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
