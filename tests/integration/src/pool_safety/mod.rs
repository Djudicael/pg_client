//! Pool RefCell safety tests.
//!
//! These tests verify that the Pool implementation never holds a `RefCell`
//! borrow across an `.await` point, which would cause a runtime panic if
//! another method tried to borrow while the future is suspended.
//!
//! The pool uses `RefCell` for interior mutability (since WASI P2 is
//! single-threaded). This means:
//! - `acquire()` takes `&self`, allowing multiple guards to coexist
//! - `status()` takes `&self`, can be called while guards are alive
//! - No method holds a `borrow_mut()` across an `.await` point

#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use wasi_pg_pool::PoolConfig;

// PoolConfig fields are private (pub(crate)), so we test them indirectly
// through the builder methods. The pool_safety integration tests below
// verify the actual behavior.

#[test]
fn test_pool_config_builder_chain() {
    // Verify builder methods return Self for chaining
    let _config = PoolConfig::default()
        .max_size(20)
        .min_idle(5)
        .test_on_acquire(false)
        .acquire_timeout(Some(Duration::from_secs(10)))
        .max_lifetime(Some(Duration::from_secs(3600)))
        .idle_timeout(Some(Duration::from_secs(300)))
        .after_connect("SET timezone = 'UTC'")
        .before_return("RESET ALL");
    // If this compiles, the builder chain works correctly
}

// ========================================================================
// Integration tests with real PostgreSQL (behind tokio-transport feature)
// ========================================================================

#[cfg(feature = "tokio-transport")]
mod integration {
    use super::*;
    use crate::common::{test_pool_config, test_pool_config_fast};
    use wasi_pg_pool::Pool;

    /// Verify that the Pool implementation never holds a RefCell borrow
    /// across an .await point. This test acquires multiple guards and
    /// checks pool status while they're alive — if a RefCell borrow were
    /// held across an await, this would panic at runtime.
    #[tokio::test]
    async fn test_pool_refcell_no_borrow_across_await() {
        let pool = Pool::new(test_pool_config_fast()).await.unwrap();

        // Acquire a connection (borrows RefCell internally)
        let guard1 = pool.acquire().await.unwrap();

        // Check status while guard is alive (borrows RefCell again)
        let status = pool.status();
        assert_eq!(status.active, 1);

        // Acquire another connection while first guard is alive
        let guard2 = pool.acquire().await.unwrap();

        // Check status again
        let status = pool.status();
        assert_eq!(status.active, 2);

        // Release both
        guard1.release().await;
        guard2.release().await;

        pool.close().await;
    }

    /// Verify that dropping a PoolGuard returns the connection to the pool.
    #[tokio::test]
    async fn test_pool_guard_drop_returns_connection() {
        let pool = Pool::new(test_pool_config_fast()).await.unwrap();

        {
            let _guard = pool.acquire().await.unwrap();
            // guard dropped here without calling release()
        }

        // Connection should be back in the pool (via Drop)
        // Note: it may have dirty state since Drop can't do async cleanup
        let status = pool.status();
        assert_eq!(status.idle, 1);
        assert_eq!(status.active, 0);

        pool.close().await;
    }

    /// Verify that pool.maintain() discards expired idle connections.
    #[tokio::test]
    async fn test_pool_maintain_discards_expired() {
        let mut pool_config = test_pool_config_fast();
        pool_config = pool_config.idle_timeout(Some(Duration::from_millis(1))); // very short

        let pool = Pool::new(pool_config).await.unwrap();

        // Acquire and release a connection
        let guard = pool.acquire().await.unwrap();
        guard.release().await;

        // Wait for idle timeout
        std::thread::sleep(Duration::from_millis(10));

        // Maintain should discard the expired connection
        pool.maintain().await;

        let status = pool.status();
        assert_eq!(status.idle, 0);

        pool.close().await;
    }

    /// Verify that acquire/release cycle works correctly.
    #[tokio::test]
    async fn test_pool_acquire_release() {
        let pool = Pool::new(test_pool_config()).await.unwrap();

        let mut guard = pool.acquire().await.unwrap();
        let result = guard.query("SELECT 1").await.unwrap();
        assert_eq!(result.len(), 1);
        guard.release().await;

        let status = pool.status();
        assert_eq!(status.idle, 1);
        assert_eq!(status.active, 0);

        pool.close().await;
    }

    /// Verify that pool exhaustion returns an error.
    #[tokio::test]
    async fn test_pool_exhaustion() {
        let mut pool_config = test_pool_config_fast();
        pool_config = pool_config
            .max_size(1)
            .acquire_timeout(Some(Duration::from_millis(100)));

        let pool = Pool::new(pool_config).await.unwrap();

        let _guard1 = pool.acquire().await.unwrap();
        // Pool is now exhausted (max_size=1)

        let result = pool.acquire().await;
        assert!(result.is_err());

        // Drop guard1 to return connection
    }

    /// Verify that multiple guards can coexist (acquire takes &self).
    #[tokio::test]
    async fn test_pool_multiple_guards() {
        let mut pool_config = test_pool_config_fast();
        pool_config = pool_config.max_size(3);

        let pool = Pool::new(pool_config).await.unwrap();

        let guard1 = pool.acquire().await.unwrap();
        let guard2 = pool.acquire().await.unwrap();

        let status = pool.status();
        assert_eq!(status.active, 2);

        guard1.release().await;
        guard2.release().await;

        pool.close().await;
    }

    /// Verify that detach removes a connection from the pool permanently.
    #[tokio::test]
    async fn test_pool_detach() {
        let pool = Pool::new(test_pool_config_fast()).await.unwrap();

        let guard = pool.acquire().await.unwrap();
        let mut conn = guard.detach();

        // Connection is no longer tracked by the pool
        let status = pool.status();
        assert_eq!(status.active, 0);
        assert_eq!(status.idle, 0);

        // We own the connection now
        conn.close().await.unwrap();

        pool.close().await;
    }

    /// Verify pool close discards idle connections.
    #[tokio::test]
    async fn test_pool_close_discards_idle() {
        let pool = Pool::new(test_pool_config_fast()).await.unwrap();

        // Acquire and release to create an idle connection
        let guard = pool.acquire().await.unwrap();
        guard.release().await;

        let status = pool.status();
        assert_eq!(status.idle, 1);

        pool.close().await;

        // After close, pool should be empty
        let status = pool.status();
        assert_eq!(status.idle, 0);
        assert!(status.closed);
    }

    /// Verify that the pool can be used after a connection is dropped
    /// (dirty state recovery).
    #[tokio::test]
    async fn test_pool_dirty_state_recovery() {
        let pool = Pool::new(test_pool_config_fast()).await.unwrap();

        // Acquire, start a transaction, then drop without release
        {
            let mut guard = pool.acquire().await.unwrap();
            let _ = guard.query("BEGIN").await;
            // Drop without release — connection has dirty state
        }

        // Next acquire should still work (pool should detect and recover)
        let guard = pool.acquire().await.unwrap();
        guard.release().await;

        pool.close().await;
    }
}
