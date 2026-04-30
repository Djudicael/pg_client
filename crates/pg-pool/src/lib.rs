//! Connection pooling for wasi-pg-client.
//!
//! This crate provides an asynchronous connection pool for PostgreSQL connections
//! on WASI Preview 2. It is designed to work with the `wasi-pg-client` crate and
//! provides efficient reuse of connections, health checks, and configurable limits.
//!
//! # Design
//!
//! The pool is built around the following core abstractions:
//! - **Pool**: The main pool struct that manages a set of connections.
//! - **PoolConfig**: Configuration for the pool (size, timeouts, etc.).
//! - **PoolGuard**: RAII guard that returns a connection to the pool when dropped.
//!
//! The pool uses a channel-based design (with `async_channel` or a WASI-compatible
//! equivalent) to manage connections. Since WASI P2 is single-threaded, the pool
//! does not require `Send` or `Sync` bounds.
//!
//! # Example
//! ```no_run
//! use wasi_pg_pool::{Pool, PoolConfig};
//! use wasi_pg_client::Config;
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
//!     let pool_config = PoolConfig::default()
//!         .max_size(10)
//!         .min_idle(2)
//!         .max_lifetime(Some(std::time::Duration::from_secs(30 * 60)));
//!
//!     let pool = Pool::new(config, pool_config).await?;
//!
//!     // Acquire a connection from the pool
//!     let _conn = pool.acquire().await?;
//!     // Use the connection (query API coming in a later step)...
//!
//!     Ok(())
//! }
//! ```
//!
//! # Note
//! This is a work in progress. The API will evolve.

// Re-export dependencies that are part of the public API.
pub use wasi_pg_client;

// Internal modules.
mod config;
mod error;
mod pool;

// Public API.
pub use config::PoolConfig;
pub use error::{Error, Result};
pub use pool::Pool;

// Prelude for convenient imports.
pub mod prelude {
    pub use super::{Pool, PoolConfig};
}
