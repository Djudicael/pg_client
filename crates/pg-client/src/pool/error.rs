#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum PoolError {
    #[error("connection pool exhausted (max_size reached)")]
    Exhausted,

    #[error("connection pool is closed")]
    Closed,

    #[error("failed to create pool connection: {0}")]
    CreateFailed(String),

    #[error("connection reset failed: {0}")]
    ResetFailed(String),
}

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
