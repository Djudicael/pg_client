//! Connection pooling for wasi-pg-client.
//!
//! This crate provides an asynchronous connection pool for PostgreSQL connections
//! on WASI Preview 2. It is designed to work with the `wasi-pg-client` crate and
//! provides efficient reuse of connections, health checks, and configurable limits.
//!
//! # Design
//!
//! The pool is built around the following core abstractions:
//! - **Pool**: The main pool struct that manages a set of connections. Uses interior
//!   mutability (`Mutex` on native, `RefCell` on WASI) so `acquire()` takes `&self`, allowing multiple guards
//!   to coexist.
//! - **PoolConfig**: Configuration for the pool (size, timeouts, hooks, etc.).
//! - **PoolGuard**: RAII guard that returns a connection to the pool when dropped.
//!   Holds `&Pool` (not `&mut Pool`), so the pool remains usable while guards exist.
//! - **PoolStatus**: Metrics about the pool (idle, active, total_created, etc.).
//! - **PoolError**: Error type for pool-specific failures.
//!
//! On native targets, `std::sync::Mutex` provides thread-safe interior mutability.
//! On WASI (single-threaded), a `RefCell`-backed `Mutex` is used instead.
//!
//! # Example
//! ```rust,ignore
//! use wasi_pg_pool::{Pool, PoolConfig};
//! use wasi_pg_client::Config;
//!
//! #[wstd::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let connection_config = Config::new()
//!         .host("localhost")
//!         .port(5432)
//!         .user("postgres")
//!         .password("password")
//!         .database("test");
//!
//!     let pool_config = PoolConfig::default()
//!         .connection(connection_config)
//!         .max_size(10)
//!         .min_idle(2)
//!         .max_lifetime(Some(std::time::Duration::from_secs(30 * 60)));
//!
//!     let pool = Pool::new(pool_config).await?;
//!
//!     // Acquire a connection from the pool
//!     let mut guard = pool.acquire().await?;
//!     guard.query("SELECT 1").await?;
//!
//!     // Return the connection (preferred over Drop)
//!     guard.release().await;
//!
//!     Ok(())
//! }
//! ```

// Re-export dependencies that are part of the public API.
pub use pg_protocol;
pub use wasi_pg_client;

// Internal modules.
mod config;
mod error;
mod guard;
mod pool;
mod status;
mod sync;

/// Target for pool tracing events.
#[cfg(feature = "tracing")]
const TARGET_POOL: &str = "wasi_pg_client::pool";

// Public API.
pub use config::PoolConfig;
pub use error::{PoolError, Result};
pub use guard::PoolGuard;
pub use pool::Pool;
pub use status::PoolStatus;

// Prelude for convenient imports.
pub mod prelude {
    pub use super::{Pool, PoolConfig, PoolError, PoolGuard, PoolStatus};
}
