//! Connection pooling for wasi-pg-client.
//!
//! This module provides an asynchronous connection pool for PostgreSQL connections.
//! The pool uses interior mutability (`Mutex` on native, `RefCell` on WASI) so
//! `acquire()` takes `&self`, allowing multiple guards to coexist.

mod config;
mod error;
mod guard;
mod pool;
mod status;
mod sync;

pub use config::PoolConfig;
pub use error::PoolError;
pub use guard::PoolGuard;
pub use pool::Pool;
pub use status::PoolStatus;

pub use error::Result as PoolResult;

/// Target for pool tracing events.
#[cfg(feature = "tracing")]
pub(crate) const TARGET_POOL: &str = "wasi_pg_client::pool";
