//! Connection pooling for wasi-pg-client.
//!
//! This module is a placeholder for the connection pool implementation.
//! The actual implementation will be added in step-15 (connection-pooling).

use crate::config::PoolConfig;
use crate::error::{Error, Result};
use wasi_pg_client::Config;

/// A placeholder for the connection pool.
///
/// This struct will be replaced by a real connection pool in step-15.
pub struct Pool;

impl Pool {
    /// Creates a new placeholder pool.
    ///
    /// # Errors
    ///
    /// This placeholder always returns an error because the pool is not implemented yet.
    pub async fn new(_connection_config: Config, _pool_config: PoolConfig) -> Result<Self> {
        Err(Error::Unsupported(
            "connection pooling is not implemented yet".into(),
        ))
    }

    /// Acquires a connection from the pool (placeholder).
    ///
    /// # Errors
    ///
    /// This placeholder always returns an error.
    pub async fn acquire(&self) -> Result<wasi_pg_client::Connection> {
        Err(Error::Unsupported(
            "connection pooling is not implemented yet".into(),
        ))
    }
}
