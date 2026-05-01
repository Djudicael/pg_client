//! Error types for the connection pool.
//!
//! This module defines the `PoolError` enum which represents all possible errors
//! that can occur when using the connection pool.

/// Errors specific to the connection pool.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PoolError {
    /// All connections are busy and `max_size` is reached.
    #[error("connection pool exhausted (max_size reached)")]
    Exhausted,

    /// The pool is closed and cannot acquire new connections.
    #[error("connection pool is closed")]
    Closed,

    /// Failed to create a new connection for the pool.
    #[error("failed to create pool connection: {0}")]
    CreateFailed(String),

    /// A connection was returned in a dirty state and could not be reset.
    #[error("connection reset failed: {0}")]
    ResetFailed(String),
}

/// A specialized `Result` type for pool operations.
pub type Result<T> = std::result::Result<T, PoolError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_error_display_messages() {
        assert_eq!(
            PoolError::Exhausted.to_string(),
            "connection pool exhausted (max_size reached)"
        );
        assert_eq!(PoolError::Closed.to_string(), "connection pool is closed");
        assert_eq!(
            PoolError::CreateFailed("timeout".to_string()).to_string(),
            "failed to create pool connection: timeout"
        );
        assert_eq!(
            PoolError::ResetFailed("rollback failed".to_string()).to_string(),
            "connection reset failed: rollback failed"
        );
    }
}
