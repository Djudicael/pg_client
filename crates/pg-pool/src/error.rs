//! Error types for the connection pool.
//!
//! This module defines the `Error` enum which represents all possible errors
//! that can occur when using the connection pool.

use std::time::Duration;

/// The main error type for the connection pool.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error from the underlying PostgreSQL client.
    #[error("client error: {0}")]
    Client(#[from] wasi_pg_client::Error),

    /// The pool is closed and cannot be used.
    #[error("pool closed")]
    PoolClosed,

    /// The pool has reached its maximum size and cannot create a new connection.
    #[error("pool exhausted (max size: {0})")]
    PoolExhausted(usize),

    /// A timeout occurred while waiting for a connection.
    #[error("timeout after {0:?}")]
    Timeout(Duration),

    /// The connection is invalid (e.g., closed by the server).
    #[error("invalid connection: {0}")]
    InvalidConnection(String),

    /// An error occurred while trying to spawn a background task (if applicable).
    #[error("background task error: {0}")]
    BackgroundTask(String),

    /// An error indicating that the operation is not supported by the pool.
    #[error("unsupported operation: {0}")]
    Unsupported(String),

    /// Any other error.
    #[error("{0}")]
    Other(String),
}

/// A specialized `Result` type for pool operations.
pub type Result<T> = std::result::Result<T, Error>;
