//! Connection pooling for wasi-pg-client.
//!
//! This module implements the `Pool` struct — an async connection pool that
//! uses interior mutability (`Mutex` on native, `RefCell` on WASI) so that
//! `acquire()` takes `&self` (not `&mut self`). This allows multiple guards
//! to coexist and the pool to be used while guards are alive.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use async_channel::{bounded, Receiver, Sender};

use super::sync::Mutex;

use crate::TransactionStatus;
use crate::{Connection, PgError};

use super::config::PoolConfig;
use super::guard::AcquiredConnection;
use super::status::PoolStatus;
use super::guard::PoolGuard;
use crate::error::PoolErrorVariant;

/// Platform-aware async sleep.
///
/// Uses `wstd::time::Timer::after` on WASI P2 and `tokio::time::sleep` on native.
#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
async fn sleep(duration: Duration) {
    wstd::time::Timer::after(duration.into()).wait().await;
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
async fn sleep(duration: Duration) {
    #[cfg(feature = "tokio-transport")]
    tokio::time::sleep(duration).await;

    #[cfg(not(feature = "tokio-transport"))]
    {
        // Fallback: use std::thread::sleep
        std::thread::sleep(duration);
    }
}

#[cfg(feature = "tracing")]
use super::TARGET_POOL;

/// Metadata tracked for each pooled connection.
pub(crate) struct PooledConnection {
    pub(crate) connection: Connection,
    /// When this connection was created.
    pub(crate) created_at: Instant,
    /// When this connection was last used (returned to pool or created).
    pub(crate) last_used_at: Instant,
    /// How many times this connection has been acquired.
    pub(crate) acquire_count: u64,
}

/// Inner pool state, wrapped in Mutex for interior mutability.
pub(crate) struct PoolInner {
    pub(crate) config: PoolConfig,
    /// Idle connections ready to be acquired.
    pub(crate) idle: VecDeque<PooledConnection>,
    /// Number of currently active (checked out) connections.
    pub(crate) active_count: usize,
    /// Number of connections currently being created asynchronously.
    pub(crate) pending_count: usize,
    /// Total number of connections ever created by this pool.
    pub(crate) total_created: u64,
    /// Whether the pool is closed (no new acquisitions allowed).
    pub(crate) closed: bool,
}

impl PoolInner {
    /// Total number of connections managed by this pool (idle + active + pending).
    fn total(&self) -> usize {
        self.idle.len() + self.active_count + self.pending_count
    }
}

/// An async connection pool for PostgreSQL connections.
///
/// The pool uses interior mutability (`Mutex` on native, `RefCell` on WASI)
/// so that `acquire()` takes `&self` (not `&mut self`). This allows multiple
/// guards to coexist and the pool to be used while guards are alive.
///
/// # Platform Mutex
///
/// On native targets, `std::sync::Mutex` provides thread-safe interior
/// mutability. On WASI (single-threaded), a `RefCell`-backed `Mutex` is
/// used instead since `std::sync::Mutex` may not compile.
///
/// # Send + Sync
///
/// On native targets, `Pool` is automatically `Send + Sync` because
/// `Mutex<T>` implements `Send`/`Sync` when `T: Send`. No unsafe impls
/// are needed.
///
/// # Example
///
/// ```rust,ignore
/// let pool = Pool::new(pool_config).await?;
///
/// // Acquire a connection (takes &self, not &mut self)
/// let mut guard = pool.acquire().await?;
/// let result = guard.query("SELECT 1").await?;
///
/// // Return the connection to the pool
/// guard.release().await;
///
/// // Can acquire again — pool is not borrowed
/// let mut guard2 = pool.acquire().await?;
/// ```
#[non_exhaustive]
pub struct Pool {
    pub(crate) inner: Mutex<PoolInner>,
    wake_tx: Sender<()>,
    #[allow(dead_code)]
    wake_rx: Receiver<()>,
}

impl Drop for Pool {
    fn drop(&mut self) {
        // When the pool is dropped, drain and drop all idle connections.
        // Each Connection::drop will trigger the transport's Drop impl,
        // which shuts down the underlying TCP socket synchronously.
        //
        // We cannot call conn.close().await here (Drop is sync), but
        // the transport Drop impls ensure the TCP FIN is sent promptly,
        // preventing connection leaks in long-running test suites.
        //
        // Active connections (checked out via PoolGuard) are not affected
        // — they are borrowed from the pool and cannot outlive it.
        let mut inner = self.inner.lock();
        inner.closed = true;
        let idle = std::mem::take(&mut inner.idle);
        drop(idle);
        // Each PooledConnection::drop → Connection::drop → Transport::drop → socket shutdown
    }
}

impl Pool {
    /// Create a new connection pool.
    ///
    /// Pre-creates `min_idle` connections if configured.
    #[must_use = "pool creation errors should be checked"]
    pub async fn new(config: PoolConfig) -> Result<Self, PgError> {
        config.validate()?;

        let mut inner = PoolInner {
            config,
            idle: VecDeque::new(),
            active_count: 0,
            pending_count: 0,
            total_created: 0,
            closed: false,
        };

        // Pre-warm: create min_idle connections
        for _ in 0..inner.config.min_idle {
            match Self::create_connection(&inner.config).await {
                Ok(conn) => {
                    let now = Instant::now();
                    inner.idle.push_back(PooledConnection {
                        connection: conn,
                        created_at: now,
                        last_used_at: now,
                        acquire_count: 0,
                    });
                    inner.total_created += 1;
                }
                Err(e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        target: TARGET_POOL,
                        error = %e,
                        "Failed to pre-warm pool connection (min_idle may not be met)"
                    );
                    // Don't fail pool creation if pre-warming fails.
                    // The pool will create connections on demand.
                    let _ = e; // suppress unused warning when tracing is off
                }
            }
        }

        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_POOL,
            min_idle = inner.config.min_idle,
            max_size = inner.config.max_size,
            "Connection pool created"
        );

        let (wake_tx, wake_rx) = bounded(1);

        Ok(Pool {
            inner: Mutex::new(inner),
            wake_tx,
            wake_rx,
        })
    }

    pub(crate) fn notify_waiters(&self) {
        let _ = self.wake_tx.try_send(());
    }

    async fn wait_for_pool_event(&self, timeout: Duration) {
        #[cfg(all(not(target_arch = "wasm32"), feature = "tokio-transport"))]
        {
            let wake_rx = self.wake_rx.clone();
            let _ = tokio::time::timeout(timeout, wake_rx.recv()).await;
        }

        #[cfg(not(all(not(target_arch = "wasm32"), feature = "tokio-transport")))]
        {
            let retry_interval = Duration::from_millis(50);
            sleep(timeout.min(retry_interval)).await;
        }
    }

    /// Create a new connection using the pool's configuration.
    async fn create_connection(config: &PoolConfig) -> Result<Connection, PgError> {
        // If reconnection is enabled in the connection config, use retry policy
        if config.connection.get_reconnect().enabled {
            let retry_policy = crate::reconnect::RetryPolicy::exponential_backoff(
                config.connection.get_reconnect().max_attempts,
                config.connection.get_reconnect().initial_delay,
                config.connection.get_reconnect().max_delay,
            );
            let mut conn =
                crate::Connection::connect_with_retry(&config.connection, &retry_policy)
                    .await?;

            // Run after_connect hook
            if let Some(ref sql) = config.after_connect {
                conn.execute(sql).await?;
                conn.set_reconnect_init_sql(sql.clone());
            }

            Ok(conn)
        } else {
            let mut conn = Connection::connect(&config.connection).await?;

            // Run after_connect hook
            if let Some(ref sql) = config.after_connect {
                conn.execute(sql).await?;
                conn.set_reconnect_init_sql(sql.clone());
            }

            Ok(conn)
        }
    }

    /// Acquire a connection from the pool.
    ///
    /// Takes `&self` (not `&mut self`), so the pool can be shared while
    /// guards are alive. The returned `PoolGuard` holds a `&Pool` reference.
    ///
    /// # Acquisition Strategy
    ///
    /// 1. Try to get an idle connection from the pool
    ///    - Discard connections that exceed `max_lifetime` or `idle_timeout`
    ///    - Optionally ping the connection (`test_on_acquire`)
    ///    - Discard broken connections
    /// 2. If no idle connection is available, create a new one (if under `max_size`)
    /// 3. If at `max_size` and no idle connections, wait until `acquire_timeout`
    ///    (in WASI P2, this is a busy-wait with async yield since we can't
    ///    be notified by another task returning a connection)
    ///
    /// # WASI P2 Limitation
    ///
    /// Since WASI P2 has no `spawn`, there's no way for another task to
    /// return a connection to the pool while we're waiting. The acquire
    /// timeout is only useful in cooperative async contexts where the
    /// same executor runs multiple futures that share the pool.
    #[must_use = "pool acquisition errors should be checked"]
    #[allow(clippy::await_holding_lock)]
    pub async fn acquire(&self) -> Result<PoolGuard<'_>, PgError> {
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_POOL, "Attempting to acquire connection from pool");

        // We loop because we may need to discard expired/unhealthy connections
        // and try again. Each iteration drops the lock guard before any
        // async operation (.await) and re-locks after.
        loop {
            let (decision, config_snapshot) = {
                let mut inner = self.inner.lock();

                if inner.closed {
                    return Err(PgError::Pool(PoolErrorVariant::Closed));
                }

                // 1. Peek at the first idle connection to decide what to do
                if let Some(pooled) = inner.idle.front() {
                    if Self::is_expired(pooled, &inner.config) {
                        (AcquireDecision::DiscardExpired, inner.config.clone())
                    } else if inner.config.test_on_acquire {
                        (AcquireDecision::NeedsHealthCheck, inner.config.clone())
                    } else {
                        // Connection is good — claim it
                        (AcquireDecision::Ready, inner.config.clone())
                    }
                } else {
                    // No idle connections
                    if inner.total() < inner.config.max_size {
                        inner.pending_count += 1;
                        (AcquireDecision::CreateNew, inner.config.clone())
                    } else {
                        (AcquireDecision::Exhausted, inner.config.clone())
                    }
                }
            }; // borrow dropped here

            match decision {
                AcquireDecision::DiscardExpired => {
                    let pooled = {
                        let mut inner = self.inner.lock();
                        inner.idle.pop_front()
                    };
                    if let Some(mut pooled) = pooled {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            target: TARGET_POOL,
                            age_secs = pooled.created_at.elapsed().as_secs(),
                            idle_secs = pooled.last_used_at.elapsed().as_secs(),
                            "Discarding expired connection from pool"
                        );
                        let _ = pooled.connection.close().await;
                    }
                    continue;
                }
                AcquireDecision::NeedsHealthCheck => {
                    // Pop the connection, drop borrow, then ping
                    let pooled = {
                        let mut inner = self.inner.lock();
                        inner.idle.pop_front()
                    };
                    if let Some(mut pooled) = pooled {
                        match pooled.connection.ping().await {
                            Ok(()) => {
                                // Connection is healthy — claim it
                                let mut inner = self.inner.lock();
                                pooled.acquire_count += 1;
                                inner.active_count += 1;

                                #[cfg(feature = "tracing")]
                                tracing::debug!(
                                    target: TARGET_POOL,
                                    active = inner.active_count,
                                    idle = inner.idle.len(),
                                    "Acquired existing connection from pool"
                                );

                                return Ok(PoolGuard::new(self, pooled));
                            }
                            Err(e) => {
                                #[cfg(feature = "tracing")]
                                tracing::debug!(
                                    target: TARGET_POOL,
                                    error = %e,
                                    "Discarding connection: health check failed"
                                );
                                let _ = &e; // suppress unused warning when tracing is disabled
                                let _ = pooled.connection.close().await;
                                continue; // retry
                            }
                        }
                    } else {
                        // Race: connection was taken between our check and pop
                        continue;
                    }
                }
                AcquireDecision::Ready => {
                    // Claim the front idle connection
                    let mut inner = self.inner.lock();
                    if let Some(mut pooled) = inner.idle.pop_front() {
                        pooled.acquire_count += 1;
                        inner.active_count += 1;

                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            target: TARGET_POOL,
                            active = inner.active_count,
                            idle = inner.idle.len(),
                            "Acquired existing connection from pool"
                        );

                        return Ok(PoolGuard::new(self, pooled));
                    } else {
                        // Race: connection was taken
                        drop(inner);
                        continue;
                    }
                }
                AcquireDecision::CreateNew => {
                    // Borrow is already dropped — safe to do async work
                    match Self::create_connection(&config_snapshot).await {
                        Ok(conn) => {
                            let mut inner = self.inner.lock();
                            inner.pending_count = inner.pending_count.saturating_sub(1);
                            if inner.closed {
                                drop(inner);
                                let mut conn = conn;
                                let _ = conn.close().await;
                                return Err(PgError::Pool(PoolErrorVariant::Closed));
                            }
                            inner.active_count += 1;
                            inner.total_created += 1;

                            #[cfg(feature = "tracing")]
                            tracing::debug!(
                                target: TARGET_POOL,
                                active = inner.active_count,
                                idle = inner.idle.len(),
                                total_created = inner.total_created,
                                "Created new connection for pool"
                            );

                            return Ok(PoolGuard::new_fresh(self, conn));
                        }
                        Err(e) => {
                            let mut inner = self.inner.lock();
                            inner.pending_count = inner.pending_count.saturating_sub(1);
                            return Err(PgError::Pool(PoolErrorVariant::CreateFailed(
                                e.to_string(),
                            )));
                        }
                    }
                }
                AcquireDecision::Exhausted => {
                    // Pool exhausted — try with timeout if configured
                    if let Some(timeout) = config_snapshot.acquire_timeout {
                        return self.acquire_with_timeout(timeout).await;
                    } else {
                        return Err(PgError::Pool(PoolErrorVariant::Exhausted));
                    }
                }
            }
        }
    }

    /// Try to acquire a connection with a timeout.
    ///
    /// On native tokio builds this waits on a lightweight notification channel
    /// so acquires can wake promptly when capacity becomes available instead of
    /// polling every fixed interval. On other targets we fall back to a short
    /// timed sleep to preserve portability.
    #[allow(clippy::await_holding_lock)]
    async fn acquire_with_timeout(&self, timeout: Duration) -> Result<PoolGuard<'_>, PgError> {
        let deadline = Instant::now() + timeout;

        loop {
            let (decision, config_snapshot) = {
                let mut inner = self.inner.lock();

                if inner.closed {
                    return Err(PgError::Pool(PoolErrorVariant::Closed));
                }

                if let Some(pooled) = inner.idle.front() {
                    if Self::is_expired(pooled, &inner.config) {
                        (AcquireDecision::DiscardExpired, inner.config.clone())
                    } else if inner.config.test_on_acquire {
                        (AcquireDecision::NeedsHealthCheck, inner.config.clone())
                    } else {
                        (AcquireDecision::Ready, inner.config.clone())
                    }
                } else if inner.total() < inner.config.max_size {
                    inner.pending_count += 1;
                    (AcquireDecision::CreateNew, inner.config.clone())
                } else {
                    (AcquireDecision::Exhausted, inner.config.clone())
                }
            };

            match decision {
                AcquireDecision::DiscardExpired => {
                    let pooled = {
                        let mut inner = self.inner.lock();
                        inner.idle.pop_front()
                    };
                    if let Some(mut pooled) = pooled {
                        let _ = pooled.connection.close().await;
                    }
                    continue;
                }
                AcquireDecision::NeedsHealthCheck => {
                    let pooled = {
                        let mut inner = self.inner.lock();
                        inner.idle.pop_front()
                    };
                    if let Some(mut pooled) = pooled {
                        match pooled.connection.ping().await {
                            Ok(()) => {
                                let mut inner = self.inner.lock();
                                pooled.acquire_count += 1;
                                inner.active_count += 1;
                                return Ok(PoolGuard::new(self, pooled));
                            }
                            Err(_) => {
                                let _ = pooled.connection.close().await;
                            }
                        }
                    }
                    continue;
                }
                AcquireDecision::Ready => {
                    let mut inner = self.inner.lock();
                    if let Some(mut pooled) = inner.idle.pop_front() {
                        pooled.acquire_count += 1;
                        inner.active_count += 1;
                        return Ok(PoolGuard::new(self, pooled));
                    }
                    continue;
                }
                AcquireDecision::CreateNew => {
                    match Self::create_connection(&config_snapshot).await {
                        Ok(conn) => {
                            let mut inner = self.inner.lock();
                            inner.pending_count = inner.pending_count.saturating_sub(1);
                            if inner.closed {
                                drop(inner);
                                let mut conn = conn;
                                let _ = conn.close().await;
                                return Err(PgError::Pool(PoolErrorVariant::Closed));
                            }
                            inner.active_count += 1;
                            inner.total_created += 1;
                            return Ok(PoolGuard::new_fresh(self, conn));
                        }
                        Err(e) => {
                            let mut inner = self.inner.lock();
                            inner.pending_count = inner.pending_count.saturating_sub(1);
                            return Err(PgError::Pool(PoolErrorVariant::CreateFailed(
                                e.to_string(),
                            )));
                        }
                    }
                }
                AcquireDecision::Exhausted => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(PgError::Pool(PoolErrorVariant::Exhausted));
                    }

                    self.wait_for_pool_event(deadline.saturating_duration_since(now))
                        .await;
                }
            }
        }
    }

    /// Acquire a connection, with automatic reconnection if the first
    /// available connection is broken.
    ///
    /// This extends the basic `acquire()` with reconnection logic:
    /// 1. Try to get an idle connection
    /// 2. If it's broken (ping fails), discard it and try the next one
    /// 3. If all idle connections are broken, create a new one
    /// 4. If creation fails, retry with the configured retry policy
    ///
    /// Unlike `acquire()`, this method always performs a health check
    /// on idle connections before returning them, regardless of the
    /// `test_on_acquire` setting.
    #[must_use = "pool acquisition errors should be checked"]
    #[allow(clippy::await_holding_lock)]
    pub async fn acquire_resilient(&self) -> Result<PoolGuard<'_>, PgError> {
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_POOL, "Attempting resilient acquire from pool");

        loop {
            let (decision, config_snapshot) = {
                let mut inner = self.inner.lock();

                if inner.closed {
                    return Err(PgError::Pool(PoolErrorVariant::Closed));
                }

                // Try idle connections
                if let Some(pooled) = inner.idle.front() {
                    if Self::is_expired(pooled, &inner.config) {
                        (AcquireDecision::DiscardExpired, inner.config.clone())
                    } else {
                        // Always health check in acquire_resilient
                        (AcquireDecision::NeedsHealthCheck, inner.config.clone())
                    }
                } else {
                    // No idle connections
                    if inner.total() < inner.config.max_size {
                        inner.pending_count += 1;
                        (AcquireDecision::CreateNew, inner.config.clone())
                    } else {
                        (AcquireDecision::Exhausted, inner.config.clone())
                    }
                }
            }; // borrow dropped here

            match decision {
                AcquireDecision::DiscardExpired => {
                    let pooled = {
                        let mut inner = self.inner.lock();
                        inner.idle.pop_front()
                    };
                    if let Some(mut pooled) = pooled {
                        let _ = pooled.connection.close().await;
                    }
                    continue;
                }
                AcquireDecision::NeedsHealthCheck => {
                    let pooled = {
                        let mut inner = self.inner.lock();
                        inner.idle.pop_front()
                    };
                    if let Some(mut pooled) = pooled {
                        match pooled.connection.ping().await {
                            Ok(()) => {
                                // Connection is alive
                                let mut inner = self.inner.lock();
                                pooled.acquire_count += 1;
                                inner.active_count += 1;

                                #[cfg(feature = "tracing")]
                                tracing::debug!(
                                    target: TARGET_POOL,
                                    active = inner.active_count,
                                    idle = inner.idle.len(),
                                    "Acquired resilient connection from pool"
                                );

                                return Ok(PoolGuard::new(self, pooled));
                            }
                            Err(e) => {
                                #[cfg(feature = "tracing")]
                                tracing::debug!(
                                    target: TARGET_POOL,
                                    error = %e,
                                    "Discarding broken connection from pool (resilient acquire)"
                                );
                                let _ = &e; // suppress unused warning when tracing is disabled
                                let _ = pooled.connection.close().await;
                                continue; // try next idle connection
                            }
                        }
                    } else {
                        continue;
                    }
                }
                AcquireDecision::Ready => {
                    // Should not reach here in acquire_resilient
                    // (we always do health check), but handle it anyway
                    let mut inner = self.inner.lock();
                    if let Some(mut pooled) = inner.idle.pop_front() {
                        pooled.acquire_count += 1;
                        inner.active_count += 1;
                        return Ok(PoolGuard::new(self, pooled));
                    } else {
                        drop(inner);
                        continue;
                    }
                }
                AcquireDecision::CreateNew => {
                    // Create with retry policy
                    let retry_policy = crate::reconnect::RetryPolicy::exponential_backoff(
                        3,
                        std::time::Duration::from_millis(100),
                        std::time::Duration::from_secs(5),
                    );

                    match Self::create_connection_with_retry(&config_snapshot, &retry_policy).await
                    {
                        Ok(conn) => {
                            let mut inner = self.inner.lock();
                            inner.pending_count = inner.pending_count.saturating_sub(1);
                            if inner.closed {
                                drop(inner);
                                let mut conn = conn;
                                let _ = conn.close().await;
                                return Err(PgError::Pool(PoolErrorVariant::Closed));
                            }
                            inner.active_count += 1;
                            inner.total_created += 1;
                            return Ok(PoolGuard::new_fresh(self, conn));
                        }
                        Err(e) => {
                            let mut inner = self.inner.lock();
                            inner.pending_count = inner.pending_count.saturating_sub(1);
                            return Err(e);
                        }
                    }
                }
                AcquireDecision::Exhausted => {
                    if let Some(timeout) = config_snapshot.acquire_timeout {
                        return self.acquire_with_timeout(timeout).await;
                    } else {
                        return Err(PgError::Pool(PoolErrorVariant::Exhausted));
                    }
                }
            }
        }
    }

    /// Check if a pooled connection has expired.
    fn is_expired(pooled: &PooledConnection, config: &PoolConfig) -> bool {
        if let Some(max_life) = config.max_lifetime {
            if pooled.created_at.elapsed() > max_life {
                return true;
            }
        }
        if let Some(idle_to) = config.idle_timeout {
            if pooled.last_used_at.elapsed() > idle_to {
                return true;
            }
        }
        false
    }

    /// Create a new connection with retry policy.
    async fn create_connection_with_retry(
        config: &PoolConfig,
        retry_policy: &crate::reconnect::RetryPolicy,
    ) -> Result<Connection, PgError> {
        let mut conn =
            crate::Connection::connect_with_retry(&config.connection, retry_policy)
                .await?;

        if let Some(ref sql) = config.after_connect {
            conn.execute(sql).await?;
            conn.set_reconnect_init_sql(sql.clone());
        }

        Ok(conn)
    }

    /// Internal: release a connection back to the pool, preserving its creation time.
    ///
    /// This method carefully avoids holding a lock guard across `.await` points.
    #[allow(clippy::await_holding_lock)]
    pub(crate) async fn release_with_metadata(&self, mut acquired: AcquiredConnection) {
        // Step 1: Decrement active count (sync, no await)
        {
            let mut inner = self.inner.lock();
            inner.active_count = inner.active_count.saturating_sub(1);

            // Don't return connections to a closed pool
            if inner.closed {
                drop(inner);
                let _ = acquired.connection.close().await;
                return;
            }
        } // borrow dropped

        // Step 2: Check connection state before attempting reset.
        // Connections in CopyIn, CopyOut, Streaming, or active query states
        // cannot be synchronously reset and must be discarded.
        if !acquired.connection.is_idle() {
            #[cfg(feature = "tracing")]
            tracing::debug!(
                target: TARGET_POOL,
                state = ?acquired.connection.state(),
                "Discarding connection on release: not idle (cannot reset)"
            );
            let _ = acquired.connection.close().await;
            self.maintain_min_idle().await;
            return;
        }

        // Step 3: Reset connection state (async — no borrow held)
        let config_snapshot = {
            let inner = self.inner.lock();
            inner.config.clone()
        };
        let should_keep = match Self::reset_connection(&mut acquired.connection, &config_snapshot)
            .await
        {
            Ok(()) => true,
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(target: TARGET_POOL, error = %e, "Discarding connection: reset failed on release");
                let _ = &e; // suppress unused warning when tracing is disabled
                false
            }
        };

        if !should_keep {
            let _ = acquired.connection.close().await;
            self.maintain_min_idle().await;
            return;
        }

        // Step 3b: Check max_lifetime before returning (sync check, no await)
        {
            let inner = self.inner.lock();
            if let Some(max_life) = inner.config.max_lifetime {
                if acquired.created_at.elapsed() > max_life {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        target: TARGET_POOL,
                        age_secs = acquired.created_at.elapsed().as_secs(),
                        "Discarding connection on return: exceeded max_lifetime"
                    );
                    drop(inner);
                    let _ = acquired.connection.close().await;
                    self.maintain_min_idle().await;
                    return;
                }
            }
        } // borrow dropped

        // Step 4: Return to idle pool with preserved created_at (sync, no await)
        let mut inner = self.inner.lock();
        let now = Instant::now();
        inner.idle.push_back(PooledConnection {
            connection: acquired.connection,
            created_at: acquired.created_at, // preserved!
            last_used_at: now,
            acquire_count: 0,
        });

        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: TARGET_POOL,
            active = inner.active_count,
            idle = inner.idle.len(),
            "Returned connection to pool (with metadata)"
        );
        drop(inner);
        self.notify_waiters();
    }

    /// Ensure `min_idle` connections are maintained by creating new ones if needed.
    #[allow(clippy::await_holding_lock)]
    async fn maintain_min_idle(&self) {
        let config = {
            let mut inner = self.inner.lock();
            if inner.closed || inner.idle.len() + inner.pending_count >= inner.config.min_idle {
                return;
            }
            inner.pending_count += 1;
            inner.config.clone()
        };

        match Self::create_connection(&config).await {
            Ok(new_conn) => {
                let mut inner = self.inner.lock();
                inner.pending_count = inner.pending_count.saturating_sub(1);
                if inner.closed || inner.idle.len() >= inner.config.min_idle {
                    drop(inner);
                    let mut new_conn = new_conn;
                    let _ = new_conn.close().await;
                    return;
                }
                let now = Instant::now();
                inner.idle.push_back(PooledConnection {
                    connection: new_conn,
                    created_at: now,
                    last_used_at: now,
                    acquire_count: 0,
                });
                inner.total_created += 1;
                drop(inner);
                self.notify_waiters();
            }
            Err(e) => {
                let mut inner = self.inner.lock();
                inner.pending_count = inner.pending_count.saturating_sub(1);
                #[cfg(feature = "tracing")]
                tracing::warn!(target: TARGET_POOL, error = %e, "Failed to create replacement connection for pool");
                let _ = e;
            }
        }
    }

    /// Reset a connection to a clean state before returning it to the pool.
    async fn reset_connection(conn: &mut Connection, config: &PoolConfig) -> Result<(), PgError> {
        // Roll back any in-flight transaction
        if conn.transaction_status() != TransactionStatus::Idle {
            conn.execute("ROLLBACK").await?;
        }

        // Run before_return hook
        if let Some(ref sql) = config.before_return {
            conn.execute(sql).await?;
        }

        Ok(())
    }

    /// Close the pool: discard all idle connections and prevent new acquisitions.
    ///
    /// Active connections (currently checked out) are not affected — they
    /// will be discarded when their guards are dropped or released.
    #[allow(clippy::await_holding_lock)]
    pub async fn close(&self) {
        #[cfg(feature = "tracing")]
        tracing::info!(target: TARGET_POOL, "Closing connection pool");

        // Step 1: Mark closed and drain idle queue (sync)
        let to_close: Vec<_> = {
            let mut inner = self.inner.lock();
            inner.closed = true;
            inner.idle.drain(..).collect()
        }; // borrow dropped
        self.notify_waiters();

        // Step 2: Close all connections (async, no borrow held)
        for mut pooled in to_close {
            let _ = pooled.connection.close().await;
        }

        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_POOL,
            active = self.inner.lock().active_count,
            "Pool closed. Active connections will be discarded on return."
        );
    }

    /// Check if the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }

    /// Get pool status/metrics.
    pub fn status(&self) -> PoolStatus {
        let inner = self.inner.lock();
        PoolStatus {
            idle: inner.idle.len(),
            active: inner.active_count,
            total_created: inner.total_created,
            max_size: inner.config.max_size,
            closed: inner.closed,
        }
    }

    /// Perform lazy maintenance: discard expired idle connections.
    ///
    /// This is called automatically during `acquire()`, but can also be
    /// called manually if you want to clean up the pool without acquiring
    /// a connection.
    #[allow(clippy::await_holding_lock)]
    pub async fn maintain(&self) {
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_POOL, "Running pool maintenance");

        // Single lock acquisition to avoid TOCTOU race
        let (to_keep, to_discard) = {
            let mut inner = self.inner.lock();
            let max_lifetime = inner.config.max_lifetime;
            let idle_timeout = inner.config.idle_timeout;

            let all: Vec<_> = inner.idle.drain(..).collect();
            let (keep, discard): (Vec<_>, Vec<_>) = all.into_iter().partition(|pooled| {
                let over_lifetime =
                    max_lifetime.is_some_and(|max| pooled.created_at.elapsed() > max);

                let over_idle =
                    idle_timeout.is_some_and(|idle| pooled.last_used_at.elapsed() > idle);

                !over_lifetime && !over_idle
            });

            // Put kept connections back immediately
            inner.idle = keep.into_iter().collect();

            (true, discard)
        }; // borrow dropped here

        let _ = to_keep; // partition already handled

        // Close discarded connections (async, no borrow held)
        for mut pooled in to_discard {
            let _ = pooled.connection.close().await;
            #[cfg(feature = "tracing")]
            tracing::debug!(target: TARGET_POOL, "Discarded expired idle connection during maintenance");
        }
    }
}

/// Internal decision enum for the acquire loop.
enum AcquireDecision {
    /// The front idle connection has expired and should be discarded.
    DiscardExpired,
    /// Connection needs a health check (ping) before it can be used.
    NeedsHealthCheck,
    /// Connection is ready to be used (no health check needed).
    Ready,
    /// No idle connections available; need to create a new one.
    CreateNew,
    /// Pool is at max_size with no idle connections.
    Exhausted,
}
