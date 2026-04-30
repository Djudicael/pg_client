# Step 16 - Automatic Reconnection & Connection Resilience

## Goal
Implement automatic reconnection, retry policies, and connection resilience patterns so that the library can recover from transient failures (network drops, server restarts, timeouts) without requiring the application to manage reconnection logic manually.

## Context

Database connections are inherently fragile — they can break at any time due to:

1. **Network failures**: TCP connection drops, DNS resolution failures, firewall timeouts
2. **Server restarts**: PostgreSQL restarts, failover, maintenance mode
3. **Idle disconnects**: Server closes idle connections (`tcp_keepalives_idle`, PgBouncer `server_idle_timeout`)
4. **Timeouts**: Query execution exceeds `statement_timeout`, connection establishment exceeds `connect_timeout`
5. **Server-side errors**: `FATAL: sorry, too many clients already`, `FATAL: the database system is in recovery mode`

The original plan had no reconnection story. Users had to detect broken connections and create new ones manually. This step adds:

- **Automatic reconnection**: When a connection is detected as broken, it can be re-established transparently
- **Retry policies**: Configurable retry logic for transient errors (serialization failures, deadlocks, timeouts)
- **Connection health tracking**: Proactive detection of broken connections before they cause query failures
- **Integration with the pool**: The pool can automatically replace broken connections

### WASI P2 Considerations

- **No background tasks**: We can't run a background health-check loop. All health checks and reconnection happen lazily (on use) or explicitly (via `ping()` / `recover()`).
- **Single-threaded**: Reconnection is sequential — we can't reconnect in parallel with other work.
- **No persistent state**: WASI components may be stateless (e.g., HTTP handlers). Connection state doesn't survive component restarts.

### Design Philosophy

**Transparent reconnection is opt-in**. By default, broken connections return errors. Users must explicitly enable reconnection via `Config` or use the pool (which handles reconnection internally). This is because:

1. Transparent reconnection can mask serious problems (server down, network partition)
2. Reconnection may lose session state (prepared statements, temporary tables, LISTEN channels)
3. Some operations must not be retried (non-idempotent INSERTs, DDL)

**Retry policies are explicit**. The library provides retry helpers but does not automatically retry queries. Users choose when and how to retry.

## Tasks

### 16.1 - Connection state tracking

```rust
/// Internal state tracking for connection health and reconnection.
pub struct ConnectionState {
    /// Whether the connection is believed to be alive.
    /// Set to false when a transport error occurs or ping fails.
    alive: bool,

    /// Number of times this connection has been reconnected.
    reconnect_count: u32,

    /// When this connection was last confirmed alive (successful query or ping).
    last_confirmed_alive: Option<Instant>,

    /// Whether the connection needs recovery (e.g., incomplete stream consumption).
    needs_recovery: bool,

    /// Session state that would be lost on reconnection.
    /// Used to decide whether reconnection is safe and to rebuild state after reconnect.
    session_state: SessionState,
}

/// Session state that is lost when a connection is closed and re-established.
pub struct SessionState {
    /// Prepared statements (would need to be re-prepared after reconnect).
    prepared_statements: HashSet<String>,

    /// Channels currently being listened on (would need to re-LISTEN).
    listen_channels: HashSet<String>,

    /// Temporary tables created in this session.
    temporary_tables: HashSet<String>,

    /// Custom GUC parameters set via SET commands.
    custom_gucs: HashMap<String, String>,

    /// Whether we're inside a transaction (reconnection mid-transaction is dangerous).
    in_transaction: bool,
}

impl SessionState {
    /// Returns true if the session has state that would be lost on reconnection.
    pub fn has_state(&self) -> bool {
        !self.prepared_statements.is_empty()
            || !self.listen_channels.is_empty()
            || !self.temporary_tables.is_empty()
            || !self.custom_gucs.is_empty()
    }

    /// Returns true if reconnection is safe (no important state would be lost).
    pub fn is_reconnect_safe(&self) -> bool {
        !self.in_transaction && !self.has_state()
    }
}
```

### 16.2 - Detecting broken connections

```rust
impl Connection {
    /// Check if the connection is believed to be alive.
    ///
    /// This is a fast check based on internal state. It does not send a query.
    /// For a definitive check, use `ping()`.
    pub fn is_alive(&self) -> bool {
        self.state.alive
    }

    /// Check if the connection might be broken based on time since last use.
    ///
    /// Returns true if the connection hasn't been confirmed alive in longer
    /// than the specified threshold. This is a heuristic — the connection
    /// might still be alive, but it's worth checking before use.
    pub fn is_stale(&self, threshold: Duration) -> bool {
        match self.state.last_confirmed_alive {
            Some(last) => last.elapsed() > threshold,
            None => true, // never confirmed alive
        }
    }

    /// Detect if an error indicates the connection is broken.
    ///
    /// This classifies errors into three categories:
    /// - **Broken**: The connection is definitely dead. Must reconnect.
    /// - **Transient**: The error might resolve on retry. Connection may still be alive.
    /// - **Permanent**: The error will not resolve on retry. Connection is still alive.
    pub fn classify_error(err: &PgError) -> ErrorClass {
        match err {
            // Connection is definitely broken
            PgError::ConnectionClosed => ErrorClass::Broken,
            PgError::Transport(ref e) if e.is_connection_broken() => ErrorClass::Broken,
            PgError::Transport(TransportError::UnexpectedEof) => ErrorClass::Broken,

            // Transient errors — connection is alive, but the operation failed
            PgError::Server(ref e) if e.is_serialization_failure() => ErrorClass::Transient,
            PgError::Server(ref e) if e.is_deadlock_detected() => ErrorClass::Transient,
            PgError::Server(ref e) if e.is_connection_exception() => ErrorClass::Broken,
            PgError::Transport(TransportError::Timeout) => ErrorClass::Transient,

            // Permanent errors — connection is alive, operation is invalid
            PgError::Server(_) => ErrorClass::Permanent,
            PgError::TypeConversion(_) => ErrorClass::Permanent,
            PgError::Config(_) => ErrorClass::Permanent,
            PgError::Auth(_) => ErrorClass::Permanent,

            // Default: treat unknown errors as permanent
            _ => ErrorClass::Permanent,
        }
    }
}

/// Classification of a PostgreSQL error for retry/reconnection decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// The connection is definitely broken. Must reconnect.
    /// Examples: ConnectionClosed, ConnectionReset, UnexpectedEof.
    Broken,

    /// The error is transient and may resolve on retry.
    /// The connection is still alive.
    /// Examples: SerializationFailure, DeadlockDetected, Timeout.
    Transient,

    /// The error is permanent and will not resolve on retry.
    /// The connection is still alive.
    /// Examples: SyntaxError, PermissionDenied, UniqueViolation.
    Permanent,
}
```

### 16.3 - Reconnection configuration

```rust
/// Reconnection policy configuration.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Whether automatic reconnection is enabled.
    /// When enabled, the connection will attempt to reconnect when a broken
    /// connection is detected.
    /// Default: false (opt-in).
    pub enabled: bool,

    /// Maximum number of reconnection attempts before giving up.
    /// Each attempt may involve DNS resolution, TCP connect, TLS, and auth.
    /// Default: 3.
    pub max_attempts: u32,

    /// Delay between reconnection attempts.
    /// Uses exponential backoff: delay * 2^attempt (capped at max_delay).
    /// Default: 100ms initial, 10s max.
    pub initial_delay: Duration,
    pub max_delay: Duration,

    /// Whether to rebuild session state after reconnection.
    /// When enabled, the connection will re-prepare statements, re-LISTEN
    /// channels, and re-SET custom GUC parameters after reconnecting.
    /// Default: true.
    pub rebuild_session: bool,

    /// Whether reconnection is allowed mid-transaction.
    /// When false (default), reconnection is only attempted if the connection
    /// is not inside a transaction. Mid-transaction reconnection is dangerous
    /// because the transaction state is lost and the operation may have
    /// partially completed.
    /// Default: false.
    pub allow_mid_transaction: bool,

    /// Callback invoked before a reconnection attempt.
    /// Can be used for logging, metrics, or custom logic.
    /// The callback receives the attempt number and the error that triggered
    /// the reconnection.
    pub on_before_reconnect: Option<ReconnectCallback>,
}

pub type ReconnectCallback = Box<dyn Fn(u32, &PgError)>;

impl Default for ReconnectConfig {
    fn default() -> Self {
        ReconnectConfig {
            enabled: false,
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            rebuild_session: true,
            allow_mid_transaction: false,
            on_before_reconnect: None,
        }
    }
}
```

### 16.4 - Reconnection implementation

```rust
impl Connection {
    /// Attempt to reconnect this connection.
    ///
    /// This closes the current (broken) connection and establishes a new one
    /// using the original configuration. If `rebuild_session` is enabled,
    /// session state (prepared statements, LISTEN channels, GUCs) is rebuilt.
    ///
    /// # Safety
    ///
    /// This should only be called when the connection is known to be broken.
    /// Calling this on a live connection will close it and create a new one,
    /// which may cause server-side state to be lost.
    pub async fn reconnect(&mut self) -> Result<(), PgError> {
        let config = self.config.clone();
        let session_state = self.state.session_state.clone();

        #[cfg(feature = "tracing")]
        tracing::info!(
            reconnect_count = self.state.reconnect_count,
            has_session_state = session_state.has_state(),
            "Attempting to reconnect"
        );

        // 1. Close the old connection (best-effort — it's probably already broken)
        self.state.alive = false;
        let _ = self.transport.shutdown().await; // ignore errors

        // 2. Establish a new connection
        let new_conn = Connection::connect(&config).await?;

        // 3. Replace our internals with the new connection's
        self.transport = new_conn.transport;
        self.codec = new_conn.codec;
        self.server_params = new_conn.server_params;
        self.transaction_status = TransactionStatus::Idle;
        self.notification_queue.clear();
        self.state.alive = true;
        self.state.reconnect_count += 1;
        self.state.last_confirmed_alive = Some(Instant::now());
        self.state.needs_recovery = false;

        // 4. Rebuild session state if configured
        if config.reconnect.rebuild_session {
            self.rebuild_session(&session_state).await?;
        }

        #[cfg(feature = "tracing")]
        tracing::info!(
            reconnect_count = self.state.reconnect_count,
            "Reconnection successful"
        );

        Ok(())
    }

    /// Rebuild session state after reconnection.
    ///
    /// This re-prepares statements, re-LISTENs on channels, and re-SETs
    /// custom GUC parameters. Errors during rebuild are logged but not
    /// propagated — partial rebuild is acceptable.
    async fn rebuild_session(&mut self, state: &SessionState) -> Result<(), PgError> {
        let mut rebuild_errors = Vec::new();

        // Re-prepare statements
        for stmt_name in &state.prepared_statements {
            // We need the original SQL to re-prepare. This means we need to
            // store the SQL alongside the statement name in the session state.
            // For now, we skip re-preparing and let the statement cache
            // handle it lazily (the cache will re-prepare on next use).
            #[cfg(feature = "tracing")]
            tracing::debug!(
                statement = %stmt_name,
                "Skipping re-prepare of statement (will be re-prepared lazily on next use)"
            );
        }

        // Re-LISTEN on channels
        for channel in &state.listen_channels {
            match self.execute(&format!("LISTEN {}", quote_identifier(channel))).await {
                Ok(_) => {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(channel = %channel, "Re-LISTENed on channel after reconnect");
                }
                Err(e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(channel = %channel, error = %e, "Failed to re-LISTEN on channel after reconnect");
                    rebuild_errors.push(e);
                }
            }
        }

        // Re-SET custom GUC parameters
        for (key, value) in &state.custom_gucs {
            match self.execute(&format!("SET {} = '{}'", key, value.replace('\'', "''"))).await {
                Ok(_) => {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(key = %key, "Re-SET GUC parameter after reconnect");
                }
                Err(e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(key = %key, error = %e, "Failed to re-SET GUC parameter after reconnect");
                    rebuild_errors.push(e);
                }
            }
        }

        // Rebuild the session state tracking
        self.state.session_state.listen_channels = state.listen_channels.clone();
        self.state.session_state.custom_gucs = state.custom_gucs.clone();
        // Note: prepared_statements are not rebuilt here — they're handled
        // lazily by the statement cache.

        if !rebuild_errors.is_empty() {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                error_count = rebuild_errors.len(),
                "Some session state rebuild operations failed after reconnection"
            );
        }

        Ok(())
    }
}
```

### 16.5 - Automatic reconnection with retry policy

```rust
impl Connection {
    /// Execute an operation with automatic reconnection and retry.
    ///
    /// This is the primary resilience method. It:
    /// 1. Executes the operation
    /// 2. If the connection is broken, attempts to reconnect and retry
    /// 3. If the error is transient (serialization failure, deadlock), retries
    /// 4. Respects the configured retry policy (max attempts, backoff)
    ///
    /// # Example
    ///
    /// ```rust
    /// let result = conn.with_retry(|conn| {
    ///     conn.query_params("SELECT * FROM users WHERE id = $1", &[&user_id])
    /// }).await?;
    /// ```
    pub async fn with_retry<T, F, Fut>(&mut self, f: F) -> Result<T, PgError>
    where
        F: Fn(&mut Connection) -> Fut,
        Fut: Future<Output = Result<T, PgError>>,
    {
        let config = self.config.reconnect.clone();
        let max_attempts = if config.enabled {
            config.max_attempts.max(1)
        } else {
            1 // no retry if reconnection is disabled
        };

        let mut attempt = 0;
        let mut last_error = None;

        loop {
            attempt += 1;

            // Execute the operation
            match f(self).await {
                Ok(result) => {
                    self.state.last_confirmed_alive = Some(Instant::now());
                    return Ok(result);
                }
                Err(err) => {
                    let class = Self::classify_error(&err);
                    last_error = Some(err);

                    match class {
                        ErrorClass::Permanent => {
                            // Permanent error — no retry
                            return Err(last_error.unwrap());
                        }
                        ErrorClass::Transient => {
                            // Transient error — retry if attempts remain
                            if attempt >= max_attempts {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    attempt = attempt,
                                    max_attempts = max_attempts,
                                    "Transient error: max retry attempts reached"
                                );
                                return Err(last_error.unwrap());
                            }

                            let delay = self.calculate_backoff(attempt, &config);
                            #[cfg(feature = "tracing")]
                            tracing::debug!(
                                attempt = attempt,
                                delay_ms = delay.as_millis(),
                                "Transient error: retrying after backoff"
                            );
                            wstd::task::sleep(delay).await;
                            continue;
                        }
                        ErrorClass::Broken => {
                            // Broken connection — reconnect and retry if enabled
                            if !config.enabled {
                                return Err(last_error.unwrap());
                            }

                            // Check if reconnection is safe
                            if !config.allow_mid_transaction
                                && self.state.session_state.in_transaction
                            {
                                #[cfg(feature = "tracing")]
                                tracing::error!(
                                    "Connection broken mid-transaction. \
                                     Reconnection is disabled for mid-transaction failures \
                                     (set allow_mid_transaction=true to override)."
                                );
                                return Err(last_error.unwrap());
                            }

                            if attempt >= max_attempts {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    attempt = attempt,
                                    max_attempts = max_attempts,
                                    "Connection broken: max reconnection attempts reached"
                                );
                                return Err(last_error.unwrap());
                            }

                            // Invoke callback
                            if let Some(ref callback) = config.on_before_reconnect {
                                callback(attempt, last_error.as_ref().unwrap());
                            }

                            // Reconnect
                            let delay = self.calculate_backoff(attempt, &config);
                            #[cfg(feature = "tracing")]
                            tracing::debug!(
                                attempt = attempt,
                                delay_ms = delay.as_millis(),
                                "Connection broken: reconnecting after backoff"
                            );
                            wstd::task::sleep(delay).await;

                            match self.reconnect().await {
                                Ok(()) => continue, // retry the operation
                                Err(reconnect_err) => {
                                    #[cfg(feature = "tracing")]
                                    tracing::error!(
                                        error = %reconnect_err,
                                        "Reconnection failed"
                                    );
                                    // Return the original error, not the reconnection error
                                    return Err(last_error.unwrap());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Calculate the backoff delay for the given attempt number.
    fn calculate_backoff(&self, attempt: u32, config: &ReconnectConfig) -> Duration {
        // Exponential backoff: initial_delay * 2^(attempt-1)
        // Capped at max_delay
        let multiplier = 2u32.saturating_pow(attempt.saturating_sub(1));
        let delay = config.initial_delay * multiplier;
        delay.min(config.max_delay)
    }
}
```

### 16.6 - Retry policy helpers (standalone, no connection needed)

```rust
/// A standalone retry policy that can be used without a Connection.
///
/// Useful for retrying connection establishment itself, or for custom
/// retry logic in application code.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts.
    pub max_attempts: u32,
    /// Initial delay between attempts.
    pub initial_delay: Duration,
    /// Maximum delay between attempts (cap for exponential backoff).
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        }
    }
}

impl RetryPolicy {
    /// Create a retry policy that does not retry (single attempt).
    pub fn no_retry() -> Self {
        RetryPolicy {
            max_attempts: 1,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    /// Create a retry policy with the specified number of attempts and fixed delay.
    pub fn fixed_delay(max_attempts: u32, delay: Duration) -> Self {
        RetryPolicy {
            max_attempts,
            initial_delay: delay,
            max_delay: delay,
        }
    }

    /// Create a retry policy with exponential backoff.
    pub fn exponential_backoff(max_attempts: u32, initial_delay: Duration, max_delay: Duration) -> Self {
        RetryPolicy {
            max_attempts,
            initial_delay,
            max_delay,
        }
    }

    /// Execute an async operation with this retry policy.
    ///
    /// The operation is retried if it returns a transient error (as classified
    /// by `should_retry`). Permanent errors and broken connection errors are
    /// not retried by this helper — use `Connection::with_retry()` for that.
    ///
    /// # Example
    ///
    /// ```rust
    /// let policy = RetryPolicy::exponential_backoff(5, Duration::from_millis(100), Duration::from_secs(5));
    /// let result = policy.retry(|| async {
    ///     some_fallible_operation().await
    /// }).await?;
    /// ```
    pub async fn retry<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut attempt = 0;

        loop {
            attempt += 1;
            match f().await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if attempt >= self.max_attempts {
                        return Err(err);
                    }

                    let delay = self.delay_for_attempt(attempt);
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        attempt = attempt,
                        delay_ms = delay.as_millis(),
                        error = %err,
                        "Retrying after error"
                    );
                    wstd::task::sleep(delay).await;
                }
            }
        }
    }

    /// Calculate the delay for a given attempt number (1-based).
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let multiplier = 2u32.saturating_pow(attempt.saturating_sub(1));
        let delay = self.initial_delay * multiplier;
        delay.min(self.max_delay)
    }
}
```

### 16.7 - Connection establishment with retry

```rust
impl Connection {
    /// Connect to PostgreSQL with a retry policy.
    ///
    /// This retries the entire connection establishment (TCP + TLS + auth)
    /// on transient failures (DNS errors, connection refused, timeouts).
    ///
    /// # Example
    ///
    /// ```rust
    /// let policy = RetryPolicy::exponential_backoff(3, Duration::from_millis(500), Duration::from_secs(10));
    /// let mut conn = Connection::connect_with_retry(&config, &policy).await?;
    /// ```
    pub async fn connect_with_retry(
        config: &Config,
        retry_policy: &RetryPolicy,
    ) -> Result<Connection, PgError> {
        let mut attempt = 0;
        let mut last_error = None;

        loop {
            attempt += 1;

            match Connection::connect(config).await {
                Ok(conn) => {
                    #[cfg(feature = "tracing")]
                    tracing::info!(
                        attempt = attempt,
                        "Successfully connected to PostgreSQL"
                    );
                    return Ok(conn);
                }
                Err(err) => {
                    last_error = Some(err);

                    if attempt >= retry_policy.max_attempts {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            attempt = attempt,
                            max_attempts = retry_policy.max_attempts,
                            "Failed to connect to PostgreSQL after all attempts"
                        );
                        return Err(last_error.unwrap());
                    }

                    let delay = retry_policy.delay_for_attempt(attempt);
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        attempt = attempt,
                        delay_ms = delay.as_millis(),
                        "Failed to connect to PostgreSQL, retrying after backoff"
                    );
                    wstd::task::sleep(delay).await;
                }
            }
        }
    }
}
```

### 16.8 - Session state tracking integration

The connection must track session state so that reconnection can rebuild it. This requires hooks in various connection methods:

```rust
impl Connection {
    /// Track that a LISTEN command was issued.
    /// Called internally after a successful LISTEN.
    fn track_listen(&mut self, channel: &str) {
        self.state.session_state.listen_channels.insert(channel.to_string());
    }

    /// Track that an UNLISTEN command was issued.
    fn track_unlisten(&mut self, channel: &str) {
        self.state.session_state.listen_channels.remove(channel);
    }

    /// Track that a SET command was issued.
    fn track_set_guc(&mut self, key: &str, value: &str) {
        self.state.session_state.custom_gucs.insert(key.to_string(), value.to_string());
    }

    /// Track that a prepared statement was created.
    fn track_prepare(&mut self, name: &str, sql: &str) {
        self.state.session_state.prepared_statements.insert(name.to_string());
        // Also store the SQL for re-preparation after reconnect
        self.statement_sql_map.insert(name.to_string(), sql.to_string());
    }

    /// Track that a prepared statement was closed.
    fn track_close_statement(&mut self, name: &str) {
        self.state.session_state.prepared_statements.remove(name);
        self.statement_sql_map.remove(name);
    }

    /// Update transaction tracking based on ReadyForQuery status.
    fn update_transaction_tracking(&mut self, status: TransactionStatus) {
        self.transaction_status = status;
        self.state.session_state.in_transaction = match status {
            TransactionStatus::Idle => false,
            TransactionStatus::InTransaction => true,
            TransactionStatus::Failed => true, // still in a transaction block
        };
    }
}
```

### 16.9 - Integration with the pool

The connection pool should use reconnection internally when it detects broken connections:

```rust
impl Pool {
    /// Acquire a connection, with automatic reconnection if the first
    /// available connection is broken.
    ///
    /// This extends the basic `acquire()` with reconnection logic:
    /// 1. Try to get an idle connection
    /// 2. If it's broken, discard it and try the next one
    /// 3. If all idle connections are broken, create a new one
    /// 4. If creation fails, retry with the configured retry policy
    pub async fn acquire_resilient(&self) -> Result<PoolGuard<'_>, PgError> {
        let mut inner = self.inner.borrow_mut();

        if inner.closed {
            return Err(PgError::Pool(PoolError::Closed.into()));
        }

        // 1. Try idle connections
        while let Some(mut pooled) = inner.idle.pop_front() {
            // Check expiry
            if Self::is_expired(&pooled, &inner.config) {
                let _ = pooled.connection.close().await;
                continue;
            }

            // Health check
            if inner.config.test_on_acquire {
                match pooled.connection.ping().await {
                    Ok(()) => {
                        // Connection is alive
                        pooled.acquire_count += 1;
                        inner.active_count += 1;
                        return Ok(PoolGuard {
                            pool: self,
                            acquired: Some(AcquiredConnection {
                                connection: pooled.connection,
                                created_at: pooled.created_at,
                            }),
                        });
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(error = %e, "Discarding broken connection from pool");
                        let _ = pooled.connection.close().await;
                        continue; // try next idle connection
                    }
                }
            } else {
                // No health check — assume alive
                pooled.acquire_count += 1;
                inner.active_count += 1;
                return Ok(PoolGuard {
                    pool: self,
                    acquired: Some(AcquiredConnection {
                        connection: pooled.connection,
                        created_at: pooled.created_at,
                    }),
                });
            }
        }

        // 2. No idle connections. Create a new one.
        if inner.total() < inner.config.max_size {
            drop(inner); // release borrow before creating connection

            let retry_policy = RetryPolicy::exponential_backoff(
                3,
                Duration::from_millis(100),
                Duration::from_secs(5),
            );

            match Self::create_connection_with_retry(&self.inner.borrow().config, &retry_policy).await {
                Ok(conn) => {
                    let mut inner = self.inner.borrow_mut();
                    inner.active_count += 1;
                    inner.total_created += 1;
                    return Ok(PoolGuard {
                        pool: self,
                        acquired: Some(AcquiredConnection {
                            connection: conn,
                            created_at: Instant::now(),
                        }),
                    });
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        // 3. Pool exhausted
        drop(inner);
        Err(PgError::Pool(PoolError::Exhausted.into()))
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
        retry_policy: &RetryPolicy,
    ) -> Result<Connection, PgError> {
        Connection::connect_with_retry(&config.connection, retry_policy).await
    }
}
```

### 16.10 - Stale connection detection (proactive)

```rust
/// Configuration for proactive stale connection detection.
#[derive(Debug, Clone)]
pub struct StaleConfig {
    /// Time threshold after which a connection is considered "stale"
    /// and should be pinged before use.
    /// Default: 30 seconds.
    pub stale_threshold: Duration,

    /// Whether to automatically ping stale connections before use.
    /// If false, stale connections are used without checking (may fail).
    /// Default: true.
    pub ping_on_stale: bool,
}

impl Default for StaleConfig {
    fn default() -> Self {
        StaleConfig {
            stale_threshold: Duration::from_secs(30),
            ping_on_stale: true,
        }
    }
}

impl Connection {
    /// Ensure the connection is alive before use.
    ///
    /// If the connection is stale (hasn't been used recently), ping it
    /// to verify it's still alive. If it's broken, attempt reconnection
    /// if configured.
    pub async fn ensure_alive(&mut self) -> Result<(), PgError> {
        if !self.state.alive {
            // Connection is known to be broken
            if self.config.reconnect.enabled {
                self.reconnect().await?;
            } else {
                return Err(PgError::ConnectionClosed);
            }
            return Ok(());
        }

        if self.is_stale(self.config.stale.stale_threshold) {
            if self.config.stale.ping_on_stale {
                match self.ping().await {
                    Ok(()) => {
                        self.state.last_confirmed_alive = Some(Instant::now());
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(error = %e, "Stale connection ping failed");
                        self.state.alive = false;

                        if self.config.reconnect.enabled {
                            self.reconnect().await?;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
```

### 16.11 - Config integration

```rust
/// Add reconnection and stale detection to the Config struct.
impl Config {
    pub fn reconnect(&self) -> &ReconnectConfig {
        &self.reconnect
    }

    pub fn stale(&self) -> &StaleConfig {
        &self.stale
    }
}

// Config additions:
pub struct Config {
    // ... existing fields ...

    /// Reconnection policy.
    pub reconnect: ReconnectConfig,

    /// Stale connection detection.
    pub stale: StaleConfig,
}

// ConfigBuilder additions:
impl ConfigBuilder {
    pub fn reconnect(mut self, config: ReconnectConfig) -> Self {
        self.config.reconnect = config;
        self
    }

    pub fn enable_reconnect(mut self) -> Self {
        self.config.reconnect.enabled = true;
        self
    }

    pub fn max_reconnect_attempts(mut self, n: u32) -> Self {
        self.config.reconnect.max_attempts = n;
        self
    }

    pub fn stale_threshold(mut self, threshold: Duration) -> Self {
        self.config.stale.stale_threshold = threshold;
        self
    }
}
```

### 16.12 - Connection string parameter support

Add reconnection-related parameters to the connection string format:

```
postgresql://user:pass@host/db?reconnect=true&reconnect_max_attempts=5&reconnect_initial_delay_ms=200&stale_threshold_secs=60
```

```rust
impl Config {
    fn parse_reconnect_params(&mut self, params: &HashMap<String, String>) -> Result<(), ConfigError> {
        if let Some(val) = params.get("reconnect") {
            self.reconnect.enabled = val.parse().map_err(|_| {
                ConfigError::InvalidParam("reconnect".into(), val.clone())
            })?;
        }
        if let Some(val) = params.get("reconnect_max_attempts") {
            self.reconnect.max_attempts = val.parse().map_err(|_| {
                ConfigError::InvalidParam("reconnect_max_attempts".into(), val.clone())
            })?;
        }
        if let Some(val) = params.get("reconnect_initial_delay_ms") {
            let ms: u64 = val.parse().map_err(|_| {
                ConfigError::InvalidParam("reconnect_initial_delay_ms".into(), val.clone())
            })?;
            self.reconnect.initial_delay = Duration::from_millis(ms);
        }
        if let Some(val) = params.get("reconnect_max_delay_ms") {
            let ms: u64 = val.parse().map_err(|_| {
                ConfigError::InvalidParam("reconnect_max_delay_ms".into(), val.clone())
            })?;
            self.reconnect.max_delay = Duration::from_millis(ms);
        }
        if let Some(val) = params.get("stale_threshold_secs") {
            let secs: u64 = val.parse().map_err(|_| {
                ConfigError::InvalidParam("stale_threshold_secs".into(), val.clone())
            })?;
            self.stale.stale_threshold = Duration::from_secs(secs);
        }
        Ok(())
    }
}
```

### 16.13 - Environment variable support

```rust
// Additional environment variables for reconnection:
//
// PGRECONNECT           - "true" / "false" (enable/disable reconnection)
// PGRECONNECT_ATTEMPTS  - max reconnection attempts (e.g., "5")
// PGRECONNECT_DELAY_MS  - initial delay in milliseconds (e.g., "200")
// PGSTALE_THRESHOLD_SECS - stale threshold in seconds (e.g., "60")

impl Config {
    fn apply_reconnect_env(&mut self) {
        if let Ok(val) = std::env::var("PGRECONNECT") {
            self.reconnect.enabled = val == "true" || val == "1";
        }
        if let Ok(val) = std::env::var("PGRECONNECT_ATTEMPTS") {
            if let Ok(n) = val.parse() {
                self.reconnect.max_attempts = n;
            }
        }
        if let Ok(val) = std::env::var("PGRECONNECT_DELAY_MS") {
            if let Ok(ms) = val.parse() {
                self.reconnect.initial_delay = Duration::from_millis(ms);
            }
        }
        if let Ok(val) = std::env::var("PGSTALE_THRESHOLD_SECS") {
            if let Ok(secs) = val.parse() {
                self.stale.stale_threshold = Duration::from_secs(secs);
            }
        }
    }
}
```

## File Layout

```
crates/pg-client/src/
├── reconnect/
│   ├── mod.rs              (reconnect, with_retry, ensure_alive)
│   ├── config.rs           (ReconnectConfig, StaleConfig)
│   ├── retry.rs            (RetryPolicy, exponential backoff)
│   ├── session.rs          (SessionState tracking)
│   ├── classify.rs         (ErrorClass, classify_error)
│   └── env.rs              (environment variable parsing for reconnect)
```

## Acceptance Criteria

- [ ] `Connection::reconnect()` closes broken connection and establishes a new one
- [ ] `Connection::with_retry()` retries operations on transient/broken errors
- [ ] `Connection::connect_with_retry()` retries connection establishment
- [ ] `Connection::ensure_alive()` proactively checks stale connections
- [ ] `ErrorClass` correctly classifies errors into Broken/Transient/Permanent
- [ ] Exponential backoff with configurable initial delay and max delay
- [ ] Session state is rebuilt after reconnection (LISTEN channels, GUC parameters)
- [ ] Prepared statements are re-prepared lazily after reconnection (via statement cache)
- [ ] Mid-transaction reconnection is blocked by default (configurable)
- [ ] Reconnection is opt-in (disabled by default)
- [ ] Stale connection detection with configurable threshold
- [ ] Connection string parameters for reconnection settings
- [ ] Environment variables for reconnection settings
- [ ] Pool uses `acquire_resilient()` to handle broken connections
- [ ] Pool creates new connections with retry policy
- [ ] Tracing instrumentation for all reconnection events
- [ ] Compiles for `wasm32-wasip2`

## Key Design Decisions

1. **Reconnection is opt-in**: By default, broken connections return errors. This prevents masking serious issues and avoids surprising state loss. Users must explicitly enable reconnection via `Config::enable_reconnect()` or connection string `reconnect=true`.

2. **Mid-transaction reconnection is blocked by default**: If a connection breaks mid-transaction, the transaction state is lost and the operation may have partially completed. Retrying could cause duplicate effects. Users must explicitly set `allow_mid_transaction=true` to override this.

3. **Session state is rebuilt on a best-effort basis**: Reconnection may not perfectly restore all session state. LISTEN channels and GUC parameters are re-established, but temporary tables and prepared statements may need to be recreated. The statement cache handles prepared statements lazily.

4. **`with_retry()` is the primary resilience API**: Instead of adding retry logic to every method, `with_retry()` wraps any operation with retry/reconnection. This keeps individual methods simple and gives users full control over retry behavior.

5. **Stale detection is proactive**: Connections that haven't been used recently are pinged before use. This prevents "first query fails" surprises after idle periods. The stale threshold is configurable (default 30 seconds).

6. **Pool handles broken connections internally**: The pool's `acquire_resilient()` method automatically discards broken connections and creates new ones. Pool users don't need to worry about reconnection.

## Limitations (WASI P2)

- **No background health checks**: All health checks and reconnection happen on the calling thread. There's no way to run a background task that proactively detects and replaces broken connections.
- **Sequential reconnection**: Reconnection blocks the current task. Other tasks can't use the connection while it's reconnecting.
- **No connection multiplexing**: Each reconnection creates a new TCP connection. There's no way to queue queries during reconnection.
- **Session state may be incomplete**: Not all session state can be rebuilt after reconnection (e.g., temporary tables, advisory locks, cursor state).

## Testing

- **Unit test**: `ErrorClass` classification for all error types
- **Unit test**: `RetryPolicy::delay_for_attempt()` exponential backoff calculation
- **Unit test**: `SessionState::is_reconnect_safe()` for various states
- **Unit test**: `Config` parsing of reconnection parameters from connection string
- **Unit test**: Environment variable parsing for reconnection settings
- **Integration test**: Reconnect after killing the TCP connection
- **Integration test**: `with_retry()` retries on serialization failure
- **Integration test**: `with_retry()` retries on deadlock
- **Integration test**: `with_retry()` does NOT retry on permanent errors (syntax error)
- **Integration test**: `with_retry()` reconnects on broken connection
- **Integration test**: `with_retry()` respects max_attempts limit
- **Integration test**: `with_retry()` blocks mid-transaction reconnection
- **Integration test**: Session state rebuilt after reconnection (LISTEN, SET)
- **Integration test**: `connect_with_retry()` retries on connection refused
- **Integration test**: `ensure_alive()` detects and reconnects stale connections
- **Integration test**: Pool `acquire_resilient()` discards broken connections
- **Integration test**: Pool creates connections with retry on temporary failures
- **Integration test**: Stale threshold triggers ping before use
- **WASI E2E test**: Reconnection from a WASI component after server restart
