# Step 15 - Connection Pooling (Channel-Based)

## Goal
Implement an async connection pool that manages multiple database connections, handles lifecycle, health checks, and provides efficient connection reuse. The pool uses a channel-based design with interior mutability, avoiding the `&mut Pool` borrow problem that prevents pool usage while a guard is alive.

## Context

### Why Not `&mut Pool`?

The original design used `PoolGuard<'a>` holding `&'a mut Pool`, which means:

```rust
let mut pool = Pool::new(config).await?;
let mut guard = pool.acquire().await?;  // borrows &mut pool
// pool is now borrowed — can't acquire another connection!
// Can't even check pool.status() while guard is alive.
guard.release().await;  // only now is the borrow released
```

This is extremely limiting. In a typical async request handler, you want to:
1. Acquire a connection from the pool
2. Use it for a query
3. Return it to the pool
4. Maybe acquire another connection later

With `&mut Pool`, you can't even hold two guards simultaneously (e.g., for a join of two queries).

### Channel-Based Design

Instead of `&mut Pool`, we use interior mutability via `RefCell<PoolInner>` so the pool can be shared by reference (`&Pool`). Connections are stored in an async-compatible channel. `acquire()` pulls from the channel; `release()` pushes back.

```
┌─────────────────────────────────────────────────────┐
│                      Pool (&self)                    │
│  ┌─────────────────────────────────────────────────┐ │
│  │              RefCell<PoolInner>                  │ │
│  │  ┌───────────────────────────────────────────┐  │ │
│  │  │  idle: VecDeque<PooledConnection>         │  │ │
│  │  │  active_count: usize                      │  │ │
│  │  │  config: PoolConfig                       │  │ │
│  │  └───────────────────────────────────────────┘  │ │
│  └─────────────────────────────────────────────────┘ │
│                                                      │
│  acquire(&self) → PoolGuard (holds &Pool, not &mut)  │
│  release() pushes connection back into PoolInner      │
└─────────────────────────────────────────────────────┘
```

### WASI P2 Constraints

- **Single-threaded**: No `Arc<Mutex<...>>` needed. `RefCell` is sufficient for interior mutability (no `Sync` required).
- **No `spawn`**: WASI P2 has no background task spawning. Pool maintenance (idle cleanup, health checks) is lazy — performed during `acquire()`.
- **No `Send` bounds**: Futures don't need `Send` since everything is single-threaded.

## Tasks

### 15.1 - Pool configuration

```rust
/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Database connection configuration.
    pub connection: Config,

    /// Minimum number of idle connections to maintain.
    /// The pool will pre-create this many connections on startup.
    /// Default: 0 (no pre-warming).
    pub min_idle: usize,

    /// Maximum number of connections in the pool (idle + active).
    /// Default: 10.
    pub max_size: usize,

    /// Maximum time to wait for a connection from the pool when
    /// all connections are busy and max_size is reached.
    /// Default: 30 seconds.
    pub acquire_timeout: Option<Duration>,

    /// Maximum lifetime of a connection from creation.
    /// Connections older than this are discarded on return to the pool.
    /// Default: 30 minutes.
    pub max_lifetime: Option<Duration>,

    /// Maximum time a connection can sit idle in the pool.
    /// Idle connections older than this are discarded during acquire.
    /// Default: 10 minutes.
    pub idle_timeout: Option<Duration>,

    /// Whether to test connections with a ping before lending them out.
    /// Adds a round-trip per acquire but guarantees the connection is alive.
    /// Default: true.
    pub test_on_acquire: bool,

    /// SQL to run when a new connection is created.
    /// Useful for session-level settings like `SET timezone = 'UTC'`.
    /// Default: None.
    pub after_connect: Option<String>,

    /// SQL to run when a connection is returned to the pool.
    /// Useful for resetting session state like `RESET ALL`.
    /// Default: None.
    pub before_return: Option<String>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            connection: Config::default(),
            min_idle: 0,
            max_size: 10,
            acquire_timeout: Some(Duration::from_secs(30)),
            max_lifetime: Some(Duration::from_secs(1800)),
            idle_timeout: Some(Duration::from_secs(600)),
            test_on_acquire: true,
            after_connect: None,
            before_return: None,
        }
    }
}
```

### 15.2 - Pool inner state

```rust
use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::Instant;

/// Metadata tracked for each pooled connection.
struct PooledConnection {
    connection: Connection,
    /// When this connection was created.
    created_at: Instant,
    /// When this connection was last used (returned to pool or created).
    last_used_at: Instant,
    /// How many times this connection has been acquired.
    acquire_count: u64,
}

/// Inner pool state, wrapped in RefCell for interior mutability.
struct PoolInner {
    config: PoolConfig,
    /// Idle connections ready to be acquired.
    idle: VecDeque<PooledConnection>,
    /// Number of currently active (checked out) connections.
    active_count: usize,
    /// Total number of connections ever created by this pool.
    total_created: u64,
    /// Whether the pool is closed (no new acquisitions allowed).
    closed: bool,
}

impl PoolInner {
    /// Total number of connections managed by this pool (idle + active).
    fn total(&self) -> usize {
        self.idle.len() + self.active_count
    }
}
```

### 15.3 - Pool struct (RefCell-based)

```rust
/// An async connection pool for PostgreSQL connections.
///
/// The pool uses interior mutability (`RefCell`) so that `acquire()` takes
/// `&self` (not `&mut self`). This allows multiple guards to coexist and
/// the pool to be used while guards are alive.
///
/// # WASI P2 Note
///
/// Since WASI P2 is single-threaded, `RefCell` (not `Mutex`) is sufficient.
/// There is no risk of concurrent access. The `RefCell` borrow checker
/// catches re-entrant borrows at runtime, which is a development-time
/// safety net.
///
/// # Example
///
/// ```rust
/// let pool = Pool::new(pool_config).await?;
///
/// // Acquire a connection (takes &self, not &mut self)
/// let mut guard = pool.acquire().await?;
/// let result = guard.query("SELECT 1").await?;
///
/// // Return the connection to the pool
/// guard.release().await?;
///
/// // Can acquire again — pool is not borrowed
/// let mut guard2 = pool.acquire().await?;
/// ```
pub struct Pool {
    inner: RefCell<PoolInner>,
}

impl Pool {
    /// Create a new connection pool.
    ///
    /// Pre-creates `min_idle` connections if configured.
    pub async fn new(config: PoolConfig) -> Result<Self, PgError> {
        let mut inner = PoolInner {
            config,
            idle: VecDeque::new(),
            active_count: 0,
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
                        error = %e,
                        "Failed to pre-warm pool connection (min_idle may not be met)"
                    );
                    // Don't fail pool creation if pre-warming fails.
                    // The pool will create connections on demand.
                }
            }
        }

        Ok(Pool {
            inner: RefCell::new(inner),
        })
    }

    /// Create a new connection using the pool's configuration.
    async fn create_connection(config: &PoolConfig) -> Result<Connection, PgError> {
        let mut conn = Connection::connect(&config.connection).await?;

        // Run after_connect hook
        if let Some(ref sql) = config.after_connect {
            conn.execute(sql).await?;
        }

        Ok(conn)
    }
}
```

### 15.4 - Acquire (takes `&self`)

```rust
impl Pool {
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
    ///
    /// In practice, for single-threaded WASI P2, if all connections are
    /// busy, `acquire()` will return `PoolError::Exhausted` immediately
    /// (or after the timeout if you want to give other futures a chance
    /// to release their connections).
    pub async fn acquire(&self) -> Result<PoolGuard<'_>, PgError> {
        let mut inner = self.inner.borrow_mut();

        if inner.closed {
            return Err(PgError::Pool(PoolError::Closed.into()));
        }

        // 1. Try to get an idle connection
        while let Some(mut pooled) = inner.idle.pop_front() {
            // Check max_lifetime
            if let Some(max_life) = inner.config.max_lifetime {
                if pooled.created_at.elapsed() > max_life {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        age_secs = pooled.created_at.elapsed().as_secs(),
                        "Discarding connection: exceeded max_lifetime"
                    );
                    let _ = pooled.connection.close().await;
                    continue;
                }
            }

            // Check idle_timeout
            if let Some(idle_to) = inner.config.idle_timeout {
                if pooled.last_used_at.elapsed() > idle_to {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        idle_secs = pooled.last_used_at.elapsed().as_secs(),
                        "Discarding connection: exceeded idle_timeout"
                    );
                    let _ = pooled.connection.close().await;
                    continue;
                }
            }

            // Health check
            if inner.config.test_on_acquire {
                match pooled.connection.ping().await {
                    Ok(()) => {}
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(error = %e, "Discarding connection: health check failed");
                        let _ = pooled.connection.close().await;
                        continue;
                    }
                }
            }

            // Connection is good — return it
            pooled.acquire_count += 1;
            inner.active_count += 1;

            #[cfg(feature = "tracing")]
            tracing::debug!(
                active = inner.active_count,
                idle = inner.idle.len(),
                "Acquired existing connection from pool"
            );

            return Ok(PoolGuard {
                pool: self,
                connection: Some(pooled.connection),
            });
        }

        // 2. No idle connections. Create a new one if under max_size.
        if inner.total() < inner.config.max_size {
            let conn = Self::create_connection(&inner.config).await?;
            inner.active_count += 1;
            inner.total_created += 1;

            #[cfg(feature = "tracing")]
            tracing::debug!(
                active = inner.active_count,
                idle = inner.idle.len(),
                total_created = inner.total_created,
                "Created new connection for pool"
            );

            return Ok(PoolGuard {
                pool: self,
                connection: Some(conn),
            });
        }

        // 3. Pool exhausted.
        // In WASI P2 (single-threaded, no spawn), we can't wait for another
        // task to release a connection. We have two options:
        //   a) Return PoolError::Exhausted immediately
        //   b) Try a cooperative yield loop with timeout
        //
        // Option (b) is useful if the caller is in a cooperative async context
        // where other futures might release their connections. We implement it
        // with a simple yield-and-retry loop.

        drop(inner); // release borrow before waiting

        if let Some(timeout) = self.inner.borrow().config.acquire_timeout {
            self.acquire_with_timeout(timeout).await
        } else {
            Err(PgError::Pool(PoolError::Exhausted.into()))
        }
    }

    /// Try to acquire a connection with a timeout, yielding to other futures.
    async fn acquire_with_timeout(&self, timeout: Duration) -> Result<PoolGuard<'_>, PgError> {
        let deadline = Instant::now() + timeout;
        let retry_interval = Duration::from_millis(50);

        loop {
            // Yield to allow other futures to run (and potentially release connections)
            wstd::task::sleep(retry_interval).await;

            // Check if a connection became available
            let mut inner = self.inner.borrow_mut();
            if let Some(pooled) = inner.idle.pop_front() {
                // Got one!
                inner.active_count += 1;
                return Ok(PoolGuard {
                    pool: self,
                    connection: Some(pooled.connection),
                });
            }

            if inner.total() < inner.config.max_size {
                let conn = Self::create_connection(&inner.config).await?;
                inner.active_count += 1;
                inner.total_created += 1;
                return Ok(PoolGuard {
                    pool: self,
                    connection: Some(conn),
                });
            }

            drop(inner); // release borrow before next iteration

            if Instant::now() >= deadline {
                return Err(PgError::Pool(PoolError::Exhausted.into()));
            }
        }
    }
}
```

### 15.5 - Release (returns connection to pool)

```rust
impl Pool {
    /// Return a connection to the pool.
    ///
    /// This is called by `PoolGuard::release()` and should not be called
    /// directly by users.
    ///
    /// The connection is reset to a clean state before being returned:
    /// - If in a transaction, ROLLBACK is issued
    /// - If `before_return` SQL is configured, it's executed
    /// - If the connection is broken, it's discarded
    pub async fn release(&self, connection: Connection) {
        let mut inner = self.inner.borrow_mut();
        inner.active_count -= 1;

        // Don't return connections to a closed pool
        if inner.closed {
            let _ = connection.close().await;
            return;
        }

        // Reset connection state
        let mut conn = connection;
        let should_keep = match Self::reset_connection(&mut conn, &inner.config).await {
            Ok(()) => true,
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(error = %e, "Discarding connection: reset failed");
                false
            }
        };

        if !should_keep {
            let _ = conn.close().await;

            // Try to maintain min_idle
            if inner.idle.len() < inner.config.min_idle {
                drop(inner); // release borrow before creating connection
                match Self::create_connection(&self.inner.borrow().config).await {
                    Ok(new_conn) => {
                        let mut inner = self.inner.borrow_mut();
                        let now = Instant::now();
                        inner.idle.push_back(PooledConnection {
                            connection: new_conn,
                            created_at: now,
                            last_used_at: now,
                            acquire_count: 0,
                        });
                        inner.total_created += 1;
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %e, "Failed to create replacement connection for pool");
                    }
                }
            }
            return;
        }

        // Check if we're over max_size (shouldn't happen, but defensive)
        if inner.total() > inner.config.max_size {
            let _ = conn.close().await;
            return;
        }

        // Return to idle pool
        let now = Instant::now();
        inner.idle.push_back(PooledConnection {
            connection: conn,
            created_at: now, // Note: ideally we'd preserve the original created_at,
                             // but we don't have it here. See note below.
            last_used_at: now,
            acquire_count: 0,
        });

        #[cfg(feature = "tracing")]
        tracing::debug!(
            active = inner.active_count,
            idle = inner.idle.len(),
            "Returned connection to pool"
        );
    }

    /// Reset a connection to a clean state before returning it to the pool.
    async fn reset_connection(
        conn: &mut Connection,
        config: &PoolConfig,
    ) -> Result<(), PgError> {
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
}
```

**Note on `created_at` preservation**: When a connection is returned to the pool, we lose the original `created_at` timestamp because `release()` receives a bare `Connection`, not a `PooledConnection`. To fix this, we should track the creation time alongside the connection. There are two approaches:

**Approach A — Store creation time in Connection itself**:
```rust
impl Connection {
    /// When this connection was established (for pool lifetime tracking).
    created_at: Option<Instant>,
}
```

**Approach B — Return a wrapper from acquire that preserves metadata**:
```rust
/// Internal wrapper that preserves pool metadata.
struct AcquiredConnection {
    connection: Connection,
    created_at: Instant,
}

pub struct PoolGuard<'a> {
    pool: &'a Pool,
    acquired: Option<AcquiredConnection>,
}
```

We use **Approach B** because it doesn't pollute the `Connection` type with pool-specific metadata.

### 15.6 - Pool guard (RAII with `&Pool`)

```rust
/// A guard that holds a connection acquired from the pool.
///
/// The guard holds a `&Pool` reference (not `&mut Pool`), so the pool
/// can be used while the guard is alive.
///
/// # Cleanup
///
/// When the guard is dropped, the connection is returned to the pool.
/// However, `Drop` cannot be async, so the connection state cleanup
/// (ROLLBACK, before_return hook) cannot be performed in Drop.
///
/// **You should prefer `guard.release().await`** over relying on Drop.
/// The async `release()` method properly cleans up the connection state
/// before returning it to the pool.
///
/// If the guard is dropped without calling `release()`, the connection
/// is returned to the pool but may need cleanup on the next `acquire()`.
pub struct PoolGuard<'a> {
    pool: &'a Pool,
    acquired: Option<AcquiredConnection>,
}

struct AcquiredConnection {
    connection: Connection,
    created_at: Instant,
}

impl<'a> PoolGuard<'a> {
    /// Access the underlying connection.
    ///
    /// Panics if the guard has already been released.
    pub fn conn(&mut self) -> &mut Connection {
        &mut self.acquired.as_mut().unwrap().connection
    }

    /// Explicitly release the connection back to the pool.
    ///
    /// This is the **preferred** way to return a connection. It performs
    /// async cleanup (ROLLBACK if in transaction, before_return hook)
    /// before returning the connection.
    ///
    /// After calling this, the guard is consumed and cannot be used again.
    pub async fn release(mut self) -> Result<(), PgError> {
        if let Some(acquired) = self.acquired.take() {
            self.pool.release_with_metadata(acquired).await;
        }
        Ok(())
    }

    /// Detach the connection from the pool.
    ///
    /// The connection is not returned to the pool. The caller takes
    /// ownership and is responsible for closing it.
    ///
    /// Useful when a connection has special state (e.g., a prepared
    /// LISTEN) that should not be reused by other pool users.
    pub fn detach(mut self) -> Result<Connection, PgError> {
        let acquired = self.acquired.take()
            .ok_or_else(|| PgError::Pool("guard already released".into()))?;
        // Decrement active count
        self.pool.inner.borrow_mut().active_count =
            self.pool.inner.borrow_mut().active_count.saturating_sub(1);
        Ok(acquired.connection)
    }

    /// Check if this guard still holds a connection.
    pub fn is_active(&self) -> bool {
        self.acquired.is_some()
    }
}

// Deref to Connection for ergonomic use
impl<'a> std::ops::Deref for PoolGuard<'a> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.acquired.as_ref().unwrap().connection
    }
}

impl<'a> std::ops::DerefMut for PoolGuard<'a> {
    fn deref_mut(&mut self) -> &mut Connection {
        &mut self.acquired.as_mut().unwrap().connection
    }
}

impl<'a> Drop for PoolGuard<'a> {
    fn drop(&mut self) {
        if let Some(acquired) = self.acquired.take() {
            // Drop cannot be async. We do the best we can:
            // 1. Decrement the active count (sync)
            let mut inner = self.pool.inner.borrow_mut();
            inner.active_count = inner.active_count.saturating_sub(1);

            // 2. Mark the connection as needing cleanup
            //    It will be cleaned up on the next acquire() or
            //    when the pool is closed.
            //
            //    For now, we push it back into the idle queue.
            //    The next acquire() will detect the dirty state
            //    (non-Idle transaction status) and reset it.
            let now = Instant::now();
            inner.idle.push_back(PooledConnection {
                connection: acquired.connection,
                created_at: acquired.created_at,
                last_used_at: now,
                acquire_count: 0,
            });

            #[cfg(feature = "tracing")]
            tracing::warn!(
                "PoolGuard dropped without calling release().await. \
                 Connection returned to pool but may need cleanup on next acquire. \
                 Prefer guard.release().await for proper cleanup."
            );
        }
    }
}
```

### 15.7 - Release with metadata (preserves `created_at`)

```rust
impl Pool {
    /// Internal: release a connection back to the pool, preserving its creation time.
    async fn release_with_metadata(&self, acquired: AcquiredConnection) {
        let mut inner = self.inner.borrow_mut();
        inner.active_count -= 1;

        // Don't return connections to a closed pool
        if inner.closed {
            let _ = acquired.connection.close().await;
            return;
        }

        // Reset connection state
        let mut conn = acquired.connection;
        let should_keep = match Self::reset_connection(&mut conn, &inner.config).await {
            Ok(()) => true,
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(error = %e, "Discarding connection: reset failed on release");
                false
            }
        };

        if !should_keep {
            let _ = conn.close().await;
            return;
        }

        // Check max_lifetime before returning
        if let Some(max_life) = inner.config.max_lifetime {
            if acquired.created_at.elapsed() > max_life {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    age_secs = acquired.created_at.elapsed().as_secs(),
                    "Discarding connection on return: exceeded max_lifetime"
                );
                let _ = conn.close().await;
                return;
            }
        }

        // Return to idle pool with preserved created_at
        let now = Instant::now();
        inner.idle.push_back(PooledConnection {
            connection: conn,
            created_at: acquired.created_at, // preserved!
            last_used_at: now,
            acquire_count: 0,
        });

        #[cfg(feature = "tracing")]
        tracing::debug!(
            active = inner.active_count,
            idle = inner.idle.len(),
            "Returned connection to pool (with metadata)"
        );
    }
}
```

### 15.8 - Pool lifecycle

```rust
impl Pool {
    /// Close the pool: discard all idle connections and prevent new acquisitions.
    ///
    /// Active connections (currently checked out) are not affected — they
    /// will be discarded when their guards are dropped or released.
    pub async fn close(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.closed = true;

        // Close all idle connections
        while let Some(pooled) = inner.idle.pop_front() {
            let _ = pooled.connection.close().await;
        }

        #[cfg(feature = "tracing")]
        tracing::info!(
            active = inner.active_count,
            "Pool closed. Active connections will be discarded on return."
        );
    }

    /// Check if the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.borrow().closed
    }

    /// Get pool status/metrics.
    pub fn status(&self) -> PoolStatus {
        let inner = self.inner.borrow();
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
    pub async fn maintain(&self) {
        let mut inner = self.inner.borrow_mut();
        let now = Instant::now();

        let mut to_keep = VecDeque::with_capacity(inner.idle.len());

        while let Some(pooled) = inner.idle.pop_front() {
            let should_discard = false;

            // Check max_lifetime
            let over_lifetime = inner.config.max_lifetime
                .map_or(false, |max| pooled.created_at.elapsed() > max);

            // Check idle_timeout
            let over_idle = inner.config.idle_timeout
                .map_or(false, |idle| pooled.last_used_at.elapsed() > idle);

            if over_lifetime || over_idle {
                let _ = pooled.connection.close().await;
                #[cfg(feature = "tracing")]
                tracing::debug!("Discarded expired idle connection during maintenance");
            } else {
                to_keep.push_back(pooled);
            }
        }

        inner.idle = to_keep;
    }
}
```

### 15.9 - Pool status

```rust
/// Status and metrics of the connection pool.
#[derive(Debug, Clone)]
pub struct PoolStatus {
    /// Number of idle connections in the pool.
    pub idle: usize,
    /// Number of currently active (checked out) connections.
    pub active: usize,
    /// Total number of connections ever created by this pool.
    pub total_created: u64,
    /// Maximum number of connections the pool can hold.
    pub max_size: usize,
    /// Whether the pool is closed.
    pub closed: bool,
}

impl PoolStatus {
    /// Total number of connections (idle + active).
    pub fn total(&self) -> usize {
        self.idle + self.active
    }

    /// Number of available slots for new connections.
    pub fn available(&self) -> usize {
        self.max_size.saturating_sub(self.total())
    }
}
```

### 15.10 - Pool error type

```rust
/// Errors specific to the connection pool.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PoolError {
    /// All connections are busy and max_size is reached.
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
```

### 15.11 - `RefCell` safety analysis

Using `RefCell` in a single-threaded WASI P2 context is safe, but we need to ensure no re-entrant borrows occur. Here's the analysis:

**Potential re-entrancy scenarios**:

1. **`acquire()` calls `create_connection()` while holding the borrow**: We `drop(inner)` before creating the connection, then re-borrow after. ✅ Safe.

2. **`release()` calls `reset_connection()` while holding the borrow**: `reset_connection` takes `&mut Connection`, not `&PoolInner`. ✅ Safe.

3. **`PoolGuard::drop()` borrows `inner`**: Drop only does sync operations (decrement counter, push to VecDeque). It doesn't call any async methods that might try to borrow `inner` again. ✅ Safe.

4. **`acquire_with_timeout()` yields and re-borrows**: We `drop(inner)` before `sleep().await`, then re-borrow after. ✅ Safe.

5. **User calls `pool.status()` while a guard is alive**: `status()` takes a new borrow, reads, and returns. The guard's `&Pool` reference doesn't prevent this. ✅ Safe.

**Invariant**: No method holds a `borrow_mut()` across an `.await` point. This is enforced by the design: we always `drop(inner)` before any async operation.

### 15.12 - Alternative: Channel-based design (for WASI P3 compatibility)

For future WASI P3 compatibility (where threading may be available), we can optionally use a channel-based design:

```rust
/// Alternative channel-based pool design (for WASI P3 / multi-threaded).
///
/// This uses an async channel to distribute connections. When a connection
/// is released, it's sent back through the channel. When a connection is
/// acquired, it's received from the channel.
///
/// Advantages:
/// - Naturally thread-safe (with `Arc` wrapper)
/// - No borrow checker issues
/// - Supports blocking wait for connection availability
///
/// Disadvantages:
/// - Requires an async channel implementation (not trivial on WASI P2)
/// - Slightly more overhead per acquire/release
/// - Harder to implement pool maintenance (need to inspect channel contents)
///
/// For v0.1, we use the RefCell-based design. This channel-based design
/// is documented here as the migration path for WASI P3.
pub struct ChannelPool {
    // sender: async_channel::Sender<PooledConnection>,
    // receiver: async_channel::Receiver<PooledConnection>,
    // config: PoolConfig,
    // active_count: AtomicUsize,  // or Cell<usize> for single-threaded
}
```

> **Note**: The `async_channel` crate may not compile on `wasm32-wasip2` because it depends on `futures-core` and potentially an async runtime. For WASI P2, the `RefCell`-based design is simpler and more reliable. The channel-based design is the migration path for WASI P3.

### 15.13 - Integration with `wstd` async model

The pool's `acquire_with_timeout()` method uses `wstd::task::sleep()` to yield to other futures. This is important for cooperative async execution in WASI P2:

```rust
// In a cooperative async context, multiple futures share the same executor.
// When one future yields (via sleep), others can make progress.
//
// Example: two concurrent queries sharing a pool
async fn concurrent_queries(pool: &Pool) -> Result<(), PgError> {
    // This pattern uses futures-concurrency to run two queries concurrently.
    // Both need a connection from the pool. If the pool has only 1 connection,
    // the second acquire will wait (with timeout) while the first query runs.
    //
    // Note: this requires the executor to poll both futures cooperatively.
    // wstd's executor does this naturally.

    let query_a = async {
        let mut guard = pool.acquire().await?;
        guard.query("SELECT * FROM table_a").await
    };

    let query_b = async {
        let mut guard = pool.acquire().await?;
        guard.query("SELECT * FROM table_b").await
    };

    let (result_a, result_b) = (query_a, query_b).join().await;
    Ok(())
}
```

## File Layout

```
crates/pg-pool/src/
├── lib.rs          (public re-exports)
├── pool.rs         (Pool, PoolInner, acquire, release, maintain)
├── guard.rs        (PoolGuard, AcquiredConnection)
├── config.rs       (PoolConfig)
├── status.rs       (PoolStatus)
└── error.rs        (PoolError)
```

## Acceptance Criteria

- [ ] Pool creates and reuses connections via async methods
- [ ] `acquire()` takes `&self` (not `&mut self`) — pool is usable while guards are alive
- [ ] `PoolGuard` holds `&Pool` (not `&mut Pool`)
- [ ] `PoolGuard::release().await` performs async cleanup before returning connection
- [ ] `PoolGuard::drop()` returns connection to pool (best-effort, no async cleanup)
- [ ] `max_size` limit enforced
- [ ] `min_idle` connections maintained (pre-warm on creation, replace on discard)
- [ ] Expired connections (`max_lifetime`) discarded on acquire and release
- [ ] Idle connections (`idle_timeout`) discarded on acquire and during maintenance
- [ ] Health check on acquire (`test_on_acquire`)
- [ ] Failed connections discarded, not returned to pool
- [ ] Connection state reset before return (ROLLBACK if in transaction)
- [ ] `after_connect` hook executed on new connections
- [ ] `before_return` hook executed on release
- [ ] `created_at` timestamp preserved across acquire/release cycles
- [ ] `detach()` removes connection from pool without returning it
- [ ] Pool status/metrics available via `status()`
- [ ] Pool close drains all idle connections and prevents new acquisitions
- [ ] Lazy maintenance via `maintain()` method
- [ ] `acquire_timeout` with cooperative yield for WASI P2
- [ ] No `RefCell` borrow held across `.await` points
- [ ] Tracing instrumentation for pool operations (behind `tracing` feature)
- [ ] Compiles for `wasm32-wasip2`

## Limitations (WASI P2)

- **Single-threaded only**: No concurrent access from multiple threads. `RefCell` (not `Mutex`) is used.
- **No background maintenance**: No `spawn` available. Maintenance is lazy (on acquire) or manual (`maintain()`).
- **Acquire timeout is cooperative**: The timeout yield loop relies on the executor polling other futures. In a purely sequential context, the timeout will always expire without acquiring.
- **Drop-based return skips async cleanup**: Connections returned via Drop may have dirty state (open transactions). The next `acquire()` will detect and reset them.
- **No connection validation on return**: We don't ping the connection when returning it to the pool (only on acquire). This is a performance trade-off.

## WASI P3 Migration Path

When WASI P3 adds threading:
1. Replace `RefCell<PoolInner>` with `Mutex<PoolInner>` (or use the channel-based design)
2. Add `Send` bounds to `PoolGuard` and `Connection`
3. Wrap `Pool` in `Arc` for shared ownership across threads
4. Add `spawn`-based background maintenance task
5. The public API (`acquire`, `release`, `status`) stays identical

## Testing

- **Unit test**: `PoolConfig` default values are reasonable
- **Unit test**: `PoolStatus` calculations (total, available)
- **Unit test**: `PoolError` Display messages are clear
- **Unit test**: `RefCell` borrow safety — no borrow held across await
- **Integration test**: Basic acquire/release cycle (using async release)
- **Integration test**: Connection reuse (verify fewer connections created than queries)
- **Integration test**: Pool exhaustion error when max_size reached
- **Integration test**: `acquire_timeout` fires when pool is exhausted
- **Integration test**: Broken connection detection and removal on acquire
- **Integration test**: `max_lifetime` eviction on acquire and release
- **Integration test**: `idle_timeout` eviction on acquire
- **Integration test**: `after_connect` hook executed
- **Integration test**: `before_return` hook executed
- **Integration test**: Transaction state cleanup on release (ROLLBACK)
- **Integration test**: Drop-based return (connection goes to pool, may need cleanup)
- **Integration test**: `detach()` removes from pool permanently
- **Integration test**: Pool close discards idle connections
- **Integration test**: Pool close prevents new acquisitions
- **Integration test**: `maintain()` discards expired idle connections
- **Integration test**: `min_idle` pre-warming on creation
- **Integration test**: `min_idle` replacement after connection discard
- **Integration test**: `created_at` preserved across acquire/release
- **Integration test**: Multiple guards can coexist (acquire takes &self)
- **Integration test**: `pool.status()` callable while guards are alive
- **WASI E2E test**: Pool operations from a WASI component
