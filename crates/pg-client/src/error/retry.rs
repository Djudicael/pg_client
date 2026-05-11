//! Retry helpers for transient errors.
//!
//! PostgreSQL can return transient errors like serialization failures
//! (SQLSTATE `40001`) or deadlocks (`40P01`) that are safe to retry.
//! This module provides a [`with_retry`] helper for common retry patterns.

use std::future::Future;

use crate::error::PgError;

// ---------------------------------------------------------------------------
// with_retry
// ---------------------------------------------------------------------------

/// Execute an async operation with automatic retry on serialization failures.
///
/// This is the most common retry pattern for PostgreSQL: when using
/// `SERIALIZABLE` isolation level, concurrent transactions may conflict
/// and the server returns a serialization failure (SQLSTATE `40001`).
/// The correct response is to roll back and retry the entire transaction.
///
/// # Example
///
/// ```ignore
/// use wasi_pg_client::error::retry::with_retry;
///
/// let result = with_retry(&mut conn, 3, |conn| async {
///     let tx = conn.transaction().await?;
///     tx.execute("UPDATE accounts SET balance = balance - 100").await?;
///     tx.execute("UPDATE accounts SET balance = balance + 100").await?;
///     tx.commit().await
/// }).await?;
/// ```
///
/// # Errors
///
/// Returns the last error if all retries are exhausted, or immediately
/// returns any non-retryable error.
pub async fn with_retry<T, F, Fut>(
    conn: &mut crate::Connection,
    max_retries: u32,
    f: F,
) -> Result<T, PgError>
where
    F: Fn(&mut crate::Connection) -> Fut,
    Fut: Future<Output = Result<T, PgError>>,
{
    let mut attempt = 0;
    loop {
        match f(conn).await {
            Ok(val) => return Ok(val),
            Err(PgError::Server(ref e))
                if e.is_serialization_failure() && attempt < max_retries =>
            {
                attempt += 1;
                // The query/execute methods already drain to ReadyForQuery on error,
                // so the connection is already idle. No need to call read_until_ready().
                continue;
            }
            Err(PgError::Server(ref e)) if e.is_deadlock_detected() && attempt < max_retries => {
                attempt += 1;
                // The query/execute methods already drain to ReadyForQuery on error,
                // so the connection is already idle. No need to call read_until_ready().
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Execute an async operation with retry on any retryable error.
///
/// Unlike [`with_retry`], which only retries serialization failures and
/// deadlocks, this function retries on any error where
/// [`PgError::is_retryable`] returns `true`, including transport timeouts.
pub async fn with_retry_any<T, F, Fut>(
    conn: &mut crate::Connection,
    max_retries: u32,
    f: F,
) -> Result<T, PgError>
where
    F: Fn(&mut crate::Connection) -> Fut,
    Fut: Future<Output = Result<T, PgError>>,
{
    let mut attempt = 0;
    loop {
        match f(conn).await {
            Ok(val) => return Ok(val),
            Err(e) if e.is_retryable() && attempt < max_retries => {
                attempt += 1;
                // For transport errors, the connection may be broken.
                // The next attempt will fail quickly if so.
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{PgServerError, TransportError};

    #[test]
    fn test_retry_on_serialization_failure() {
        // Unit test: verify the retry logic classification
        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "40001".to_string()),
            (b'M', "could not serialize access".to_string()),
        ])));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_retry_on_deadlock() {
        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "40P01".to_string()),
            (b'M', "deadlock detected".to_string()),
        ])));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_no_retry_on_unique_violation() {
        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "23505".to_string()),
            (b'M', "duplicate key".to_string()),
        ])));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_retry_on_timeout() {
        let err = PgError::Transport(TransportError::Timeout);
        assert!(err.is_retryable());
    }
}
