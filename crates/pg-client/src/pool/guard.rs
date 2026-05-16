//! Pool guard (RAII with `&Pool`).
//!
//! This module defines `PoolGuard` — an RAII guard that holds a connection
//! acquired from the pool. The guard holds a `&Pool` reference (not `&mut Pool`),
//! so the pool can be used while the guard is alive.

use std::ops::{Deref, DerefMut};
use std::time::Instant;

use crate::TransactionStatus;
use crate::Connection;

use super::pool::PooledConnection;
use super::Pool;

#[cfg(feature = "tracing")]
use super::TARGET_POOL;

/// Internal wrapper that preserves pool metadata across acquire/release cycles.
pub(crate) struct AcquiredConnection {
    pub(crate) connection: Connection,
    /// When this connection was originally created.
    pub(crate) created_at: Instant,
}

/// A guard that holds a connection acquired from the pool.
///
/// The guard holds a `&Pool` reference (not `&mut Pool`), so the pool
/// can be used while the guard is alive.
///
/// # Cleanup
///
/// When the guard is dropped, the connection is returned to the pool.
/// However, `Drop` cannot be async, so the connection state cleanup
/// (ROLLBACK, `before_return` hook) cannot be performed in Drop.
///
/// **You should prefer `guard.release().await`** over relying on Drop.
/// The async `release()` method properly cleans up the connection state
/// before returning it to the pool.
///
/// If the guard is dropped without calling `release()`, the connection
/// is returned to the pool but may need cleanup on the next `acquire()`.
#[non_exhaustive]
pub struct PoolGuard<'a> {
    pool: &'a Pool,
    acquired: Option<AcquiredConnection>,
}

impl<'a> PoolGuard<'a> {
    /// Create a new `PoolGuard` from a pooled connection.
    pub(crate) fn new(pool: &'a Pool, pooled: PooledConnection) -> Self {
        PoolGuard {
            pool,
            acquired: Some(AcquiredConnection {
                connection: pooled.connection,
                created_at: pooled.created_at,
            }),
        }
    }

    /// Create a new `PoolGuard` from a freshly created connection.
    pub(crate) fn new_fresh(pool: &'a Pool, connection: Connection) -> Self {
        PoolGuard {
            pool,
            acquired: Some(AcquiredConnection {
                connection,
                created_at: Instant::now(),
            }),
        }
    }

    /// Access the underlying connection.
    ///
    /// Panics if the guard has already been released.
    pub fn conn(&mut self) -> &mut Connection {
        &mut self
            .acquired
            .as_mut()
            .expect("PoolGuard already released")
            .connection
    }

    /// Explicitly release the connection back to the pool.
    ///
    /// This is the **preferred** way to return a connection. It performs
    /// async cleanup (ROLLBACK if in transaction, `before_return` hook)
    /// before returning the connection.
    ///
    /// After calling this, the guard is consumed and cannot be used again.
    pub async fn release(mut self) {
        if let Some(acquired) = self.acquired.take() {
            #[cfg(feature = "tracing")]
            tracing::debug!(target: TARGET_POOL, "Releasing connection back to pool");
            self.pool.release_with_metadata(acquired).await;
        }
    }

    /// Detach the connection from the pool.
    ///
    /// The connection is not returned to the pool. The caller takes
    /// ownership and is responsible for closing it.
    ///
    /// Useful when a connection has special state (e.g., a prepared
    /// LISTEN) that should not be reused by other pool users.
    pub fn detach(mut self) -> Connection {
        let acquired = self.acquired.take().expect("PoolGuard already released");
        // Decrement active count
        let mut inner = self.pool.inner.lock();
        inner.active_count = inner.active_count.saturating_sub(1);
        drop(inner);
        self.pool.notify_waiters();
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_POOL, "Detaching connection from pool");
        acquired.connection
    }

    /// Check if this guard still holds a connection.
    pub fn is_active(&self) -> bool {
        self.acquired.is_some()
    }
}

// Deref to Connection for ergonomic use
impl<'a> Deref for PoolGuard<'a> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        &self
            .acquired
            .as_ref()
            .expect("PoolGuard already released")
            .connection
    }
}

impl<'a> DerefMut for PoolGuard<'a> {
    fn deref_mut(&mut self) -> &mut Connection {
        &mut self
            .acquired
            .as_mut()
            .expect("PoolGuard already released")
            .connection
    }
}

impl<'a> Drop for PoolGuard<'a> {
    fn drop(&mut self) {
        if let Some(acquired) = self.acquired.take() {
            // Drop cannot be async. We do the best we can:
            // 1. Decrement the active count (sync)
            let mut inner = self.pool.inner.lock();
            inner.active_count = inner.active_count.saturating_sub(1);

            // 2. Check connection state. If the connection is not idle (e.g.,
            //    mid-transaction, CopyIn, CopyOut, Streaming), or if the
            //    backend still reports a failed/in-transaction status, we cannot
            //    safely return it to the pool. Discard it instead.
            if !acquired.connection.is_idle()
                || acquired.connection.transaction_status() != TransactionStatus::Idle
            {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: TARGET_POOL,
                    "PoolGuard dropped with dirty connection (not idle). \
                     Connection discarded. Prefer guard.release().await for proper cleanup."
                );
                // Don't push back; the Connection's own Drop will set state to
                // Closed and handle socket shutdown.
                drop(inner);
                self.pool.notify_waiters();
                drop(acquired);
                return;
            }

            // 3. If the pool was closed while this guard was checked out,
            //    discard the connection instead of returning it to idle storage.
            if inner.closed {
                drop(inner);
                self.pool.notify_waiters();
                drop(acquired);
                return;
            }

            // 4. Connection is idle — safe to return to the pool.
            let now = Instant::now();
            inner.idle.push_back(PooledConnection {
                connection: acquired.connection,
                created_at: acquired.created_at,
                last_used_at: now,
                acquire_count: 0,
            });

            // 5. Warn because Drop-based return skips async cleanup hooks.

            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: TARGET_POOL,
                "PoolGuard dropped without calling release().await. \
                 Connection returned to pool but may need cleanup on next acquire. \
                 Prefer guard.release().await for proper cleanup."
            );
            drop(inner);
            self.pool.notify_waiters();
        }
    }
}
