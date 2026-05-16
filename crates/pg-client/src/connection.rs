//! PostgreSQL connection management.
//!
//! This module defines the `Connection` struct which represents a connection to a PostgreSQL server.
//! It handles the connection lifecycle, authentication, protocol state, and graceful close.

use std::collections::VecDeque;

use pg_protocol::{BackendMessage, FrontendMessage, TransactionStatus};

use crate::auth::{self, ServerParams};
use crate::config::Config;
use crate::ensure_random_available;
use crate::error::{Error, PgError, Result};
use crate::notification::Notification;
use crate::query::{Notice, NoticeHandler};
use crate::transport::{
    AsyncTransport, BufferedTransport, ClientTransport, PgTransport, SslMode, TlsConfig,
};

#[cfg(feature = "tracing")]
use crate::tracing_ext::{TARGET_CONNECTION, TARGET_NOTIFICATION, TARGET_RECONNECT};

// ---------------------------------------------------------------------------
// Connection state machine
// ---------------------------------------------------------------------------

/// Internal state of a PostgreSQL connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionState {
    /// Initial state before any network activity.
    Disconnected,
    /// TCP/TLS handshake in progress.
    Connecting,
    /// Startup message sent, waiting for Authentication* or ReadyForQuery.
    StartingUp,
    /// Authentication challenge/response exchange in progress.
    Authenticating,
    /// ReadyForQuery received; connection is idle and can accept commands.
    Idle,
    /// A simple query is in flight.
    ActiveSimpleQuery,
    /// An extended query is in flight.
    ActiveExtendedQuery,
    /// A COPY IN operation is in progress.
    CopyIn,
    /// A COPY OUT operation is in progress.
    CopyOut,
    /// A row stream is active and borrowing the connection.
    Streaming,
    /// Connection is being closed gracefully.
    Closing,
    /// Connection is closed or unusable due to error.
    Closed,
}

impl ConnectionState {
    /// Returns `true` if the connection is in a state where queries may be sent.
    pub fn is_idle(self) -> bool {
        matches!(self, ConnectionState::Idle)
    }

    /// Returns `true` if the connection is fully closed.
    pub fn is_closed(self) -> bool {
        matches!(self, ConnectionState::Closed)
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// A connection to a PostgreSQL server.
///
/// The connection is established when `Connection::connect` is called and is closed when the
/// connection is dropped. The connection can be used to execute queries and manage transactions.
pub struct Connection {
    pub(crate) transport: PgTransport<ClientTransport>,
    pub(crate) codec: auth::Codec,
    pub(crate) server_params: ServerParams,
    pub(crate) state: ConnectionState,
    pub(crate) config: Config,
    pub(crate) transaction_status: TransactionStatus,
    pub(crate) notification_queue: VecDeque<Notification>,
    pub(crate) notice_handler: Option<NoticeHandler>,
    pub(crate) statement_counter: u32,
    /// Whether the connection needs recovery (e.g., a RowStream was dropped
    /// before being fully consumed).
    pub(crate) needs_recovery: bool,
    /// Health and reconnection state.
    pub(crate) health: crate::reconnect::session::ConnectionHealth,
    /// Session state tracking for reconnection.
    pub(crate) session_state: crate::reconnect::session::SessionState,
}

impl Connection {
    // =======================================================================
    // Connection establishment
    // =======================================================================

    /// Establishes a new connection to the PostgreSQL server using the given configuration.
    ///
    /// This method performs the following steps:
    /// 1. Resolves the host and port to a TCP address.
    /// 2. Establishes a TCP connection (with optional TLS).
    /// 3. Performs the PostgreSQL startup handshake.
    /// 4. Authenticates with the server.
    /// 5. Collects server parameters until `ReadyForQuery`.
    #[must_use = "connection errors should be checked"]
    pub async fn connect(config: &Config) -> Result<Self> {
        #[cfg(feature = "tracing")]
        let span = tracing::info_span!(
            target: TARGET_CONNECTION,
            "connect",
            host = %config.get_host(),
            port = config.get_port(),
            database = ?config.get_database(),
            user = %config.get_user(),
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        ensure_random_available();

        let mut transport = build_pg_transport(config).await?;
        let mut codec = auth::Codec::new();

        // Send StartupMessage
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_CONNECTION, "Sending startup message");
        let startup = pg_protocol::FrontendMessage::Startup {
            params: config.startup_params(),
        };
        codec
            .send(&mut transport, &startup)
            .await
            .map_err(Error::from)?;

        // Authenticate
        let server_params = auth::authenticate(&mut transport, &mut codec, config)
            .await
            .map_err(Error::from)?;

        #[cfg(feature = "tracing")]
        tracing::info!(target: TARGET_CONNECTION, server_version = %server_params.server_version, process_id = server_params.process_id, "Connection established successfully");

        Ok(Self {
            transport,
            codec,
            server_params,
            state: ConnectionState::Idle,
            config: config.clone(),
            transaction_status: TransactionStatus::Idle,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
            needs_recovery: false,
            health: crate::reconnect::session::ConnectionHealth::new(),
            session_state: crate::reconnect::session::SessionState::new(),
        })
    }

    /// Connect with automatic retry using the given retry policy.
    ///
    /// This is useful for connection establishment that may fail transiently
    /// (e.g. the server is temporarily unavailable). The retry policy
    /// controls the number of attempts and the delay between them.
    #[must_use = "connection errors should be checked"]
    pub async fn connect_with_retry(
        config: &Config,
        retry_policy: &crate::reconnect::RetryPolicy,
    ) -> Result<Self> {
        retry_policy.retry(|| Self::connect(config)).await
    }

    /// Convenience: connect from a connection string (URI or key-value format).
    #[must_use = "connection errors should be checked"]
    pub async fn connect_str(s: &str) -> Result<Self> {
        let config = match Config::from_uri(s) {
            Ok(c) => c,
            Err(uri_err) => match Config::from_key_value(s) {
                Ok(c) => c,
                Err(kv_err) => {
                    return Err(PgError::Config(format!(
                        "could not parse connection string as URI ({uri_err}) or key=value format ({kv_err})"
                    )));
                }
            },
        };
        Self::connect(&config).await
    }

    // =======================================================================
    // Accessors
    // =======================================================================

    /// Returns a reference to the configuration used for this connection.
    pub fn config(&self) -> &Config {
        &self.config
    }

    fn escape_sql_literal(value: &str) -> String {
        value.replace('\\', "\\\\").replace('\'', "''")
    }

    pub(crate) fn build_set_param_sql(key: &str, value: &str) -> String {
        format!(
            "SET {} = '{}'",
            crate::transaction::quote_identifier(key),
            Self::escape_sql_literal(value)
        )
    }

    pub(crate) fn build_listen_sql(channel: &str) -> String {
        format!("LISTEN {}", crate::transaction::quote_identifier(channel))
    }

    pub(crate) fn build_unlisten_sql(channel: &str) -> String {
        format!("UNLISTEN {}", crate::transaction::quote_identifier(channel))
    }

    /// Set a runtime parameter on the server.
    ///
    /// Sends `SET key = value` and tracks the change in [`SessionState`]
    /// for automatic re-application on reconnection.
    ///
    /// # Example
    /// ```ignore
    /// conn.set_param("timezone", "UTC").await?;
    /// ```
    #[must_use = "set_param errors should be checked"]
    pub async fn set_param(&mut self, key: &str, value: &str) -> Result<()> {
        let sql = Self::build_set_param_sql(key, value);
        self.execute(&sql).await?;
        self.session_state.track_set_guc(key, value);
        Ok(())
    }

    /// Register opaque initialization SQL to replay after a reconnect.
    ///
    /// This is intended for idempotent baseline session setup such as pool-level
    /// `after_connect` hooks. The SQL is replayed before tracked session state
    /// (for example, `set_param()` changes) is rebuilt, so later runtime
    /// overrides still win.
    pub fn set_reconnect_init_sql(&mut self, sql: impl Into<String>) {
        self.session_state.set_reconnect_init_sql(sql);
    }

    /// Clear any previously registered reconnect initialization SQL.
    pub fn clear_reconnect_init_sql(&mut self) {
        self.session_state.clear_reconnect_init_sql();
    }

    /// Return the registered reconnect initialization SQL, if any.
    pub fn reconnect_init_sql(&self) -> Option<&str> {
        self.session_state.reconnect_init_sql()
    }

    /// Returns the current connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Returns whether the connection is closed or unusable.
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    /// Returns whether the connection is idle and ready to accept commands.
    pub fn is_idle(&self) -> bool {
        self.state.is_idle()
    }

    /// Returns the server parameters collected during connection startup.
    pub fn server_params(&self) -> &ServerParams {
        &self.server_params
    }

    /// Returns the server version string (e.g. "16.0").
    pub fn server_version(&self) -> &str {
        &self.server_params.server_version
    }

    /// Returns the backend process ID.
    pub fn process_id(&self) -> i32 {
        self.server_params.process_id
    }

    /// Returns the secret key (used for cancel requests).
    pub fn secret_key(&self) -> i32 {
        self.server_params.secret_key
    }

    /// Returns the current transaction status.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    /// Takes any queued notifications, leaving the queue empty.
    pub fn drain_notifications(&mut self) -> Vec<Notification> {
        self.notification_queue.drain(..).collect()
    }

    /// Get a cancellation token for this connection.
    ///
    /// The token can be sent to another task or thread to cancel a
    /// running query. It contains the host, port, process ID, and
    /// secret key needed to send an out-of-band cancellation request.
    ///
    /// # Example
    /// ```ignore
    /// let token = conn.cancel_token();
    ///
    /// // In another task:
    /// tokio::spawn(async move {
    ///     tokio::time::sleep(Duration::from_secs(5)).await;
    ///     token.cancel().await.unwrap();
    /// });
    /// ```
    pub fn cancel_token(&self) -> crate::cancel::CancelToken {
        crate::cancel::CancelToken {
            host: self.config.host.clone(),
            port: self.config.port,
            process_id: self.server_params.process_id,
            secret_key: self.server_params.secret_key,
            ssl_mode: self.config.ssl_mode,
            accept_invalid_certs: self.config.accept_invalid_certs,
        }
    }

    /// Set a handler that will be called for every server notice.
    ///
    /// The previous handler (if any) is replaced.
    pub fn set_notice_handler(&mut self, handler: NoticeHandler) {
        self.notice_handler = Some(handler);
    }

    /// Remove the current notice handler.
    pub fn clear_notice_handler(&mut self) {
        self.notice_handler = None;
    }

    /// Returns true if the connection is using TLS.
    pub fn is_tls(&self) -> bool {
        #[cfg(feature = "tls")]
        {
            matches!(self.transport, crate::transport::PgTransport::Tls(_))
        }
        #[cfg(not(feature = "tls"))]
        {
            false
        }
    }

    /// Get TLS info if the connection is encrypted.
    ///
    /// Returns `None` if the connection is not using TLS.
    #[cfg(feature = "tls")]
    pub fn tls_info(&self) -> Option<crate::transport::TlsInfo> {
        self.transport.tls_info()
    }

    /// Check if the connection is still alive by sending a simple query.
    #[must_use = "ping errors should be checked"]
    pub async fn ping(&mut self) -> Result<()> {
        self.query("SELECT 1").await?;
        self.health.mark_alive();
        Ok(())
    }

    /// Check connection state without sending a query.
    /// Examines the transport and protocol state.
    pub fn is_healthy(&self) -> bool {
        !self.is_closed() && self.transaction_status != pg_protocol::TransactionStatus::Failed
    }

    /// Reset the connection state (clear failed transaction, discard temp objects).
    #[must_use = "reset errors should be checked"]
    pub async fn reset(&mut self) -> Result<()> {
        if self.transaction_status == pg_protocol::TransactionStatus::Failed
            || self.transaction_status == pg_protocol::TransactionStatus::InTransaction
        {
            self.execute("ROLLBACK").await?;
        }
        self.execute("DISCARD ALL").await?;
        self.session_state.clear();
        Ok(())
    }

    /// Whether the connection needs recovery (e.g., a `RowStream` was dropped
    /// before being fully consumed).
    ///
    /// When this returns `true`, the connection may have unread protocol
    /// messages in its buffer. Call [`Connection::recover`] to drain them
    /// and restore the connection to a usable state.
    pub fn needs_recovery(&self) -> bool {
        self.needs_recovery
    }

    /// Recover the connection after an incomplete stream consumption.
    ///
    /// Reads messages until `ReadyForQuery` is received, discarding
    /// everything. This is needed when a `RowStream` is dropped before
    /// being fully consumed.
    #[must_use = "recovery errors should be checked"]
    pub async fn recover(&mut self) -> Result<()> {
        if self.needs_recovery {
            self.read_until_ready().await?;
            self.needs_recovery = false;
        }
        Ok(())
    }

    // =======================================================================
    // Reconnection & Resilience
    // =======================================================================

    /// Check if the connection is believed to be alive.
    ///
    /// This is a fast check based on internal state. It does not send a query.
    /// For a definitive check, use `ping()`.
    pub fn is_alive(&self) -> bool {
        self.health.is_alive()
    }

    /// Check if the connection might be broken based on time since last use.
    ///
    /// Returns true if the connection hasn't been confirmed alive in longer
    /// than the specified threshold. This is a heuristic — the connection
    /// might still be alive, but it's worth checking before use.
    pub fn is_stale(&self, threshold: std::time::Duration) -> bool {
        match self.health.last_confirmed_alive() {
            Some(last) => last.elapsed() > threshold,
            None => true, // never confirmed alive
        }
    }

    /// Get the number of times this connection has been reconnected.
    pub fn reconnect_count(&self) -> u32 {
        self.health.reconnect_count()
    }

    /// Get a reference to the session state.
    pub fn session_state(&self) -> &crate::reconnect::session::SessionState {
        &self.session_state
    }

    /// Attempt to reconnect this connection.
    ///
    /// This closes the current (broken) connection and establishes a new one
    /// using the original configuration. If `rebuild_session` is enabled in
    /// the reconnection config, registered reconnect initialization SQL is
    /// replayed first and tracked session state (LISTEN channels, GUC
    /// parameters) is rebuilt afterwards.
    ///
    /// # Safety
    ///
    /// This should only be called when the connection is known to be broken.
    /// Calling this on a live connection will close it and create a new one,
    /// which may cause server-side state to be lost.
    #[must_use = "reconnection errors should be checked"]
    pub async fn reconnect(&mut self) -> crate::error::Result<()> {
        let session_state = self.session_state.clone();

        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_RECONNECT,
            reconnect_count = self.health.reconnect_count(),
            has_session_state = session_state.has_state(),
            has_reconnect_init_sql = session_state.reconnect_init_sql().is_some(),
            "Attempting to reconnect"
        );

        // 1. Close the old connection (best-effort — it's probably already broken)
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_RECONNECT, "Shutting down old transport before reconnect");
        self.health.mark_broken();
        if let Err(e) = self.transport.shutdown().await {
            #[cfg(feature = "tracing")]
            tracing::debug!(target: TARGET_RECONNECT, error = %e, "Old transport shutdown error (expected during reconnect)");
            let _ = &e;
        }

        // 2. Establish and fully initialize a replacement connection before we
        //    swap it into self. That way, a reconnect-init failure does not
        //    leave self holding a partially initialized replacement transport.
        let mut new_conn = Self::connect(&self.config).await?;
        new_conn.rebuild_reconnect_init_sql(&session_state).await?;
        if self.config.get_reconnect().rebuild_session {
            new_conn.rebuild_session(&session_state).await?;
        }

        // 3. Replace our internals with the new connection's using swap
        //    (Connection implements Drop, so we can't move fields out)
        std::mem::swap(&mut self.transport, &mut new_conn.transport);
        std::mem::swap(&mut self.codec, &mut new_conn.codec);
        std::mem::swap(&mut self.server_params, &mut new_conn.server_params);
        self.transaction_status = new_conn.transaction_status;
        self.notification_queue.clear();
        self.state = new_conn.state;
        self.health.reset_after_reconnect();
        self.needs_recovery = false;

        // new_conn will be dropped here, cleaning up the old transport

        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_RECONNECT,
            reconnect_count = self.health.reconnect_count(),
            "Reconnection successful"
        );

        Ok(())
    }

    async fn rebuild_reconnect_init_sql(
        &mut self,
        state: &crate::reconnect::session::SessionState,
    ) -> crate::error::Result<()> {
        if let Some(sql) = state.reconnect_init_sql() {
            #[cfg(feature = "tracing")]
            tracing::debug!(target: TARGET_RECONNECT, "Replaying reconnect initialization SQL");
            self.execute(sql).await?;
        }

        Ok(())
    }

    /// Rebuild session state after reconnection.
    ///
    /// This re-LISTENs on channels and re-SETs custom GUC parameters.
    /// Errors during rebuild are logged but not propagated — partial rebuild
    /// is acceptable.
    async fn rebuild_session(
        &mut self,
        state: &crate::reconnect::session::SessionState,
    ) -> crate::error::Result<()> {
        // Re-prepare statements (lazily — just clear tracking, the statement
        // cache will re-prepare on next use)
        #[cfg(feature = "tracing")]
        if !state.prepared_statements().is_empty() {
            tracing::debug!(
                target: TARGET_RECONNECT,
                count = state.prepared_statements().len(),
                "Prepared statements will be re-prepared lazily on next use"
            );
        }

        // Re-LISTEN on channels
        for channel in state.listen_channels() {
            let sql = Self::build_listen_sql(channel);
            match self.execute(&sql).await {
                Ok(_) => {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(target: TARGET_RECONNECT, channel = %channel, "Re-LISTENed on channel after reconnect");
                    self.session_state.track_listen(channel);
                }
                Err(e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(target: TARGET_RECONNECT, channel = %channel, error = %e, "Failed to re-LISTEN on channel after reconnect");
                    let _ = &e; // suppress unused warning when tracing is disabled
                }
            }
        }

        // Re-SET custom GUC parameters
        for (key, value) in state.custom_gucs() {
            let sql = Self::build_set_param_sql(key, value);
            match self.execute(&sql).await {
                Ok(_) => {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(target: TARGET_RECONNECT, key = %key, "Re-SET GUC parameter after reconnect");
                    self.session_state.track_set_guc(key, value);
                }
                Err(e) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(target: TARGET_RECONNECT, key = %key, error = %e, "Failed to re-SET GUC parameter after reconnect");
                    let _ = &e; // suppress unused warning when tracing is disabled
                }
            }
        }

        Ok(())
    }

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
    /// ```rust,ignore
    /// let result = conn.with_retry(|conn| {
    ///     conn.query_params("SELECT * FROM users WHERE id = $1", &[&user_id])
    /// }).await?;
    /// ```
    #[must_use = "retry errors should be checked"]
    pub async fn with_retry<T, F, Fut>(&mut self, f: F) -> crate::error::Result<T>
    where
        F: Fn(&mut Connection) -> Fut,
        Fut: std::future::Future<Output = crate::error::Result<T>>,
    {
        let config = self.config.get_reconnect().clone();
        let max_attempts = if config.enabled {
            config.max_attempts.max(1)
        } else {
            1 // no retry if reconnection is disabled
        };

        let mut attempt = 0;

        loop {
            attempt += 1;

            // Execute the operation
            match f(self).await {
                Ok(result) => {
                    self.health.mark_alive();
                    self.session_state.set_in_transaction(
                        self.transaction_status != pg_protocol::TransactionStatus::Idle,
                    );
                    return Ok(result);
                }
                Err(err) => {
                    let class = crate::reconnect::classify::classify_error(&err);

                    match class {
                        crate::reconnect::classify::ErrorClass::Permanent => {
                            // Permanent error — no retry
                            return Err(err);
                        }
                        crate::reconnect::classify::ErrorClass::Transient => {
                            // Transient error — retry if attempts remain
                            if attempt >= max_attempts {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    target: TARGET_RECONNECT,
                                    attempt = attempt,
                                    max_attempts = max_attempts,
                                    "Transient error: max retry attempts reached"
                                );
                                return Err(err);
                            }

                            let delay = config.delay_for_attempt(attempt);
                            #[cfg(feature = "tracing")]
                            tracing::debug!(
                                target: TARGET_RECONNECT,
                                attempt = attempt,
                                delay_ms = delay.as_millis(),
                                "Transient error: retrying after backoff"
                            );
                            reconnect_sleep(delay).await;
                            continue;
                        }
                        crate::reconnect::classify::ErrorClass::Broken => {
                            // Broken connection — reconnect and retry if enabled
                            if !config.enabled {
                                self.health.mark_broken();
                                return Err(err);
                            }

                            // Check if reconnection is safe
                            if !config.allow_mid_transaction && self.session_state.in_transaction()
                            {
                                #[cfg(feature = "tracing")]
                                tracing::error!(
                                    target: TARGET_RECONNECT,
                                    "Connection broken mid-transaction. \
                                     Reconnection is disabled for mid-transaction failures \
                                     (set allow_mid_transaction=true to override)."
                                );
                                self.health.mark_broken();
                                return Err(err);
                            }

                            if attempt >= max_attempts {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    target: TARGET_RECONNECT,
                                    attempt = attempt,
                                    max_attempts = max_attempts,
                                    "Connection broken: max reconnection attempts reached"
                                );
                                return Err(err);
                            }

                            // Invoke callback
                            if let Some(ref callback) = config.on_before_reconnect {
                                callback(attempt, &err);
                            }

                            // Reconnect
                            let delay = config.delay_for_attempt(attempt);
                            #[cfg(feature = "tracing")]
                            tracing::debug!(
                                target: TARGET_RECONNECT,
                                attempt = attempt,
                                delay_ms = delay.as_millis(),
                                "Connection broken: reconnecting after backoff"
                            );
                            reconnect_sleep(delay).await;

                            match self.reconnect().await {
                                Ok(()) => continue, // retry the operation
                                Err(reconnect_err) => {
                                    #[cfg(feature = "tracing")]
                                    tracing::error!(
                                        target: TARGET_RECONNECT,
                                        error = %reconnect_err,
                                        "Reconnection failed"
                                    );
                                    let _ = &reconnect_err; // suppress unused warning when tracing is disabled
                                                            // Return the original error, not the reconnection error
                                    return Err(err);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Ensure the connection is alive before use.
    ///
    /// If the connection is stale (hasn't been used recently), ping it
    /// to verify it's still alive. If it's broken, attempt reconnection
    /// if configured.
    #[must_use = "health check errors should be checked"]
    pub async fn ensure_alive(&mut self) -> crate::error::Result<()> {
        if !self.health.is_alive() {
            // Connection is known to be broken
            #[cfg(feature = "tracing")]
            tracing::warn!(target: TARGET_RECONNECT, "Connection is known to be broken");
            if self.config.get_reconnect().enabled {
                self.reconnect().await?;
            } else {
                return Err(crate::error::PgError::ConnectionClosed);
            }
            return Ok(());
        }

        if self.is_stale(self.config.get_stale().stale_threshold)
            && self.config.get_stale().ping_on_stale
        {
            match self.ping().await {
                Ok(()) => {
                    self.health.mark_alive();
                }
                Err(e) => {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(target: TARGET_RECONNECT, error = %e, "Stale connection ping failed");
                    self.health.mark_broken();

                    if self.config.get_reconnect().enabled {
                        self.reconnect().await?;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) fn handle_notice(&self, notice: &Notice) {
        if let Some(ref handler) = self.notice_handler {
            handler(notice);
        } else {
            #[cfg(feature = "tracing")]
            tracing::warn!(severity = %notice.severity(), code = %notice.code(), message = %notice.message(), "server notice");
        }
    }

    // =======================================================================
    // Lifecycle
    // =======================================================================

    /// Gracefully closes the connection.
    ///
    /// Sends a `Terminate` message (`X`) to the server and shuts down the
    /// underlying transport. After closing, the connection cannot be used for
    /// further operations.
    #[must_use = "close errors should be checked"]
    pub async fn close(&mut self) -> Result<()> {
        if self.state.is_closed() {
            return Ok(());
        }

        #[cfg(feature = "tracing")]
        tracing::info!(target: TARGET_CONNECTION, "Closing connection");

        self.state = ConnectionState::Closing;

        // Best-effort: send Terminate, ignore errors.
        let _ = self
            .codec
            .send(&mut self.transport, &FrontendMessage::Terminate)
            .await;

        let _ = self.transport.shutdown().await;

        self.state = ConnectionState::Closed;

        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_CONNECTION, "Connection closed");

        Ok(())
    }

    /// Force-close the connection without sending a Terminate message.
    ///
    /// This is useful when the connection is already known to be broken.
    pub async fn abort(&mut self) {
        self.state = ConnectionState::Closed;
        let _ = self.transport.shutdown().await;
    }

    /// Internal: read messages until `ReadyForQuery`, discarding everything else.
    ///
    /// Used after errors to resync the protocol state or to drain the response
    /// stream after a query.
    pub(crate) async fn read_until_ready(&mut self) -> Result<()> {
        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            match msg {
                BackendMessage::ReadyForQuery(body) => {
                    self.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    self.session_state.set_in_transaction(
                        self.transaction_status != pg_protocol::TransactionStatus::Idle,
                    );
                    self.state = ConnectionState::Idle;
                    return Ok(());
                }
                BackendMessage::ParameterStatus(body) => {
                    if let (Ok(name), Ok(value)) = (body.name(), body.value()) {
                        self.server_params
                            .params
                            .insert(name.to_string(), value.to_string());
                    }
                }
                BackendMessage::NotificationResponse(body) => {
                    let channel = body.channel().unwrap_or("").to_string();
                    let payload = body.message().unwrap_or("").to_string();
                    let process_id = body.process_id();
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        target: TARGET_NOTIFICATION,
                        channel = %channel,
                        process_id = process_id,
                        payload_len = payload.len(),
                        "Received notification"
                    );
                    self.notification_queue.push_back(Notification {
                        process_id,
                        channel,
                        payload,
                    });
                }
                BackendMessage::NoticeResponse(body) => {
                    if let Ok(notice) = Notice::from_fields(&body) {
                        self.handle_notice(&notice);
                    }
                }
                _ => {} // discard other messages
            }
        }
    }

    /// Internal: handle asynchronous messages that can arrive at any time.
    ///
    /// PostgreSQL can send `NotificationResponse`, `NoticeResponse`, and
    /// `ParameterStatus` messages asynchronously, interleaved with query
    /// results. This method handles those messages and returns `true` if
    /// the message was consumed (the caller should read the next message).
    /// Returns `false` if the message is a synchronous response that the
    /// caller should handle itself.
    ///
    /// Every message-reading loop in the library should call this method
    /// for each message before processing it, to ensure no notifications
    /// are lost.
    pub(crate) fn handle_async_message(&mut self, msg: &BackendMessage) -> bool {
        match msg {
            BackendMessage::NotificationResponse(body) => {
                let channel = body.channel().unwrap_or("").to_string();
                let payload = body.message().unwrap_or("").to_string();
                let process_id = body.process_id();
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    target: TARGET_NOTIFICATION,
                    channel = %channel,
                    process_id = process_id,
                    payload_len = payload.len(),
                    "Received notification"
                );
                #[cfg(feature = "tracing")]
                tracing::trace!(
                    target: TARGET_NOTIFICATION,
                    channel = %channel,
                    payload = %payload,
                    "Received notification (with payload)"
                );
                self.notification_queue.push_back(Notification {
                    process_id,
                    channel,
                    payload,
                });
                true
            }
            BackendMessage::NoticeResponse(body) => {
                if let Ok(notice) = Notice::from_fields(body) {
                    self.handle_notice(&notice);
                }
                true
            }
            BackendMessage::ParameterStatus(body) => {
                if let (Ok(name), Ok(value)) = (body.name(), body.value()) {
                    self.server_params
                        .params
                        .insert(name.to_string(), value.to_string());
                }
                true
            }
            _ => false,
        }
    }

    /// Internal: transition to a new state, returning an error if the current
    /// state does not permit the transition.
    pub(crate) fn transition(&mut self, new_state: ConnectionState) -> Result<()> {
        match (self.state, new_state) {
            // Any → Closed is always allowed (error recovery).
            (_, ConnectionState::Closed) => {}
            // Idle → active states.
            (ConnectionState::Idle, ConnectionState::ActiveSimpleQuery)
            | (ConnectionState::Idle, ConnectionState::ActiveExtendedQuery)
            | (ConnectionState::Idle, ConnectionState::CopyIn)
            | (ConnectionState::Idle, ConnectionState::CopyOut)
            | (ConnectionState::Idle, ConnectionState::Streaming) => {}
            // Active → Idle (completion).
            (ConnectionState::ActiveSimpleQuery, ConnectionState::Idle)
            | (ConnectionState::ActiveExtendedQuery, ConnectionState::Idle)
            | (ConnectionState::CopyIn, ConnectionState::Idle)
            | (ConnectionState::CopyOut, ConnectionState::Idle)
            | (ConnectionState::Streaming, ConnectionState::Idle) => {}
            // Idle → Closing.
            (ConnectionState::Idle, ConnectionState::Closing) => {}
            // Any other transition is invalid.
            (old, new) => {
                return Err(PgError::InvalidState(format!(
                    "invalid state transition from {old:?} to {new:?}"
                )));
            }
        }
        self.state = new_state;
        Ok(())
    }

    // =======================================================================
    // Synchronous COPY recovery helpers (for Drop implementations)
    // =======================================================================

    /// Best-effort synchronous cancellation of a COPY IN operation.
    ///
    /// This is called from `CopyIn::drop()` when the `CopyIn` was not
    /// properly finished or cancelled. It encodes a `CopyFail` message
    /// and attempts a blocking write + flush on the underlying transport.
    ///
    /// **Limitations:**
    /// - On WASI targets (async-only I/O), this is a no-op because we
    ///   cannot perform I/O in a synchronous `Drop` context.
    /// - On native targets with `NativeTcpTransport`, this will attempt
    ///   a blocking write of the `CopyFail` message followed by a drain
    ///   of the server's response.
    /// - If the write fails (e.g., broken pipe), the error is silently
    ///   ignored — the connection is already in a bad state.
    pub(crate) fn cancel_copy_in_sync(&mut self, reason: &str) {
        // Encode CopyFail message using the Codec
        let copy_fail = FrontendMessage::CopyFail {
            message: reason.to_string(),
        };
        if self.codec.encode_to_buffer(&copy_fail).is_err() {
            self.state = ConnectionState::Closed;
            return;
        }
        // Clone the buffer data to avoid borrow conflict with try_sync_write_and_flush
        let data = self.codec.write_buffer().to_vec();

        // Attempt to write and flush synchronously
        let written = self.try_sync_write_and_flush(&data);

        if written {
            // Try to drain the server's response (ErrorResponse + ReadyForQuery)
            // so the connection might be recoverable
            self.try_sync_drain_until_ready();
            self.state = ConnectionState::Idle;
        } else {
            // Could not send CopyFail — connection is broken
            self.state = ConnectionState::Closed;
        }
    }

    /// Best-effort synchronous drain of a COPY OUT operation.
    ///
    /// This is called from `CopyOut::drop()` when there is unread COPY
    /// data. It attempts to read and discard data until `ReadyForQuery`
    /// so the connection can be reused.
    ///
    /// **Limitations:**
    /// - On WASI targets (async-only I/O), this is a no-op.
    /// - On native targets, this performs blocking reads.
    /// - If the read fails, the connection is marked as `Closed`.
    pub(crate) fn drain_copy_out_sync(&mut self) {
        if self.try_sync_drain_until_ready() {
            self.state = ConnectionState::Idle;
        } else {
            self.state = ConnectionState::Closed;
        }
    }

    /// Attempt a synchronous write + flush on the underlying transport.
    ///
    /// Returns `true` if the write succeeded, `false` otherwise.
    /// This only works for `NativeTcpTransport`; for WASI it's a no-op.
    #[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
    fn try_sync_write_and_flush(&mut self, data: &[u8]) -> bool {
        use std::io::Write;

        match &mut self.transport {
            PgTransport::Plain(ref mut buffered) => {
                let inner = buffered.inner_mut();
                match inner {
                    ClientTransport::Native(ref mut tcp) => {
                        // First, try to flush any data that the BufferedTransport
                        // may have buffered. We can't call async flush, so we
                        // write directly to the TcpStream.
                        //
                        // Note: This bypasses the BufferedTransport's buffer,
                        // which means any previously buffered data may be lost.
                        // This is acceptable because we're in an error recovery
                        // path and the connection state is already compromised.
                        if let Err(e) = tcp.stream.write_all(data) {
                            let _ = &e;
                            false
                        } else {
                            tcp.stream.flush().is_ok()
                        }
                    }
                    _ => false,
                }
            }
            PgTransport::Tls(_) => {
                // TLS transport doesn't support easy sync I/O
                false
            }
        }
    }

    /// Attempt a synchronous write + flush — WASI no-op.
    #[cfg(target_arch = "wasm32")]
    fn try_sync_write_and_flush(&mut self, _data: &[u8]) -> bool {
        // WASI I/O is async-only; cannot perform I/O in Drop
        false
    }

    /// Attempt a synchronous write + flush — fallback no-op when no native transport.
    #[cfg(all(not(target_arch = "wasm32"), not(feature = "test-native")))]
    fn try_sync_write_and_flush(&mut self, _data: &[u8]) -> bool {
        false
    }

    /// Attempt to synchronously read and discard messages until ReadyForQuery.
    ///
    /// Returns `true` if `ReadyForQuery` was received, `false` otherwise.
    #[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
    fn try_sync_drain_until_ready(&mut self) -> bool {
        use std::io::Read;

        match &mut self.transport {
            PgTransport::Plain(ref mut buffered) => {
                let inner = buffered.inner_mut();
                match inner {
                    ClientTransport::Native(ref mut tcp) => {
                        // Read in a loop looking for ReadyForQuery ('Z')
                        let mut buf = [0u8; 4096];
                        let mut scan_buf: Vec<u8> = Vec::new();

                        loop {
                            // Check if we already have ReadyForQuery in scan_buf
                            // ReadyForQuery: 'Z' (1 byte) + length (4 bytes, big-endian) + status (1 byte)
                            // Length should be 5 (includes the length field itself)
                            if let Some(pos) = scan_buf.iter().position(|&b| b == b'Z') {
                                // Need at least 6 bytes: 'Z' + 4-byte length + 1-byte status
                                if scan_buf.len() >= pos + 6 {
                                    let len = i32::from_be_bytes([
                                        scan_buf[pos + 1],
                                        scan_buf[pos + 2],
                                        scan_buf[pos + 3],
                                        scan_buf[pos + 4],
                                    ]);
                                    if len == 5 {
                                        // Validate status byte is a known transaction status
                                        let status = scan_buf[pos + 5];
                                        if status == b'I' || status == b'T' || status == b'E' {
                                            return true;
                                        }
                                    }
                                }
                            }

                            match tcp.stream.read(&mut buf) {
                                Ok(0) => return false, // EOF
                                Ok(n) => scan_buf.extend_from_slice(&buf[..n]),
                                Err(_) => return false,
                            }

                            // Safety limit: don't read more than 10MB
                            if scan_buf.len() > 10 * 1024 * 1024 {
                                return false;
                            }
                        }
                    }
                    _ => false,
                }
            }
            PgTransport::Tls(_) => false,
        }
    }

    /// Attempt to synchronously drain — WASI no-op.
    #[cfg(target_arch = "wasm32")]
    fn try_sync_drain_until_ready(&mut self) -> bool {
        false
    }

    /// Attempt to synchronously drain — fallback no-op.
    #[cfg(all(not(target_arch = "wasm32"), not(feature = "test-native")))]
    fn try_sync_drain_until_ready(&mut self) -> bool {
        false
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // We cannot perform async I/O in Drop, so we cannot send a
        // PostgreSQL Terminate message. However, we CAN shut down the
        // underlying TCP socket synchronously, which sends a FIN to the
        // server and ensures the connection is properly cleaned up.
        //
        // Without this, connections that are dropped without calling
        // `conn.close().await` would leak until the OS reclaims them or
        // the server times out, causing accumulation and potential lock
        // contention in long-running test suites.
        //
        // The transport's own Drop impl handles the actual socket
        // shutdown (WasiTcpTransport calls socket.shutdown(),
        // NativeTcpTransport calls stream.shutdown()). Here we just
        // ensure the state is marked Closed so the transport is dropped.
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_CONNECTION, state = ?self.state, "Connection dropped; transport Drop will close socket");
        self.state = ConnectionState::Closed;
        // The transport field is dropped here, triggering its Drop impl
        // which performs the actual socket shutdown.
    }
}

// ---------------------------------------------------------------------------
// Platform-specific transport construction
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
async fn build_pg_transport(config: &Config) -> Result<PgTransport<ClientTransport>> {
    let tcp = crate::transport::connect_with_timeout(
        config.get_host(),
        config.get_port(),
        config.get_connect_timeout(),
    )
    .await
    .map_err(PgError::Transport)?;
    apply_tls(ClientTransport::Wasi(tcp), config).await
}

#[cfg(all(not(target_arch = "wasm32"), feature = "tokio-transport"))]
async fn build_pg_transport(config: &Config) -> Result<PgTransport<ClientTransport>> {
    let tcp = crate::transport::connect_with_timeout(
        config.get_host(),
        config.get_port(),
        config.get_connect_timeout(),
    )
    .await
    .map_err(PgError::Transport)?;
    apply_tls(ClientTransport::Tokio(tcp), config).await
}

#[cfg(all(
    not(target_arch = "wasm32"),
    not(feature = "tokio-transport"),
    feature = "test-native"
))]
async fn build_pg_transport(config: &Config) -> Result<PgTransport<ClientTransport>> {
    let tcp = crate::transport::NativeTcpTransport::connect_with_timeout(
        config.get_host(),
        config.get_port(),
        config.get_connect_timeout(),
    )
    .map_err(PgError::Transport)?;
    apply_tls(ClientTransport::Native(tcp), config).await
}

#[cfg(all(
    not(target_arch = "wasm32"),
    not(feature = "tokio-transport"),
    not(feature = "test-native")
))]
async fn build_pg_transport(_config: &Config) -> Result<PgTransport<ClientTransport>> {
    Err(PgError::Unsupported(
        "no transport available for this target. Enable the 'tokio-transport' feature (recommended) or 'test-native' feature, or compile for wasm32-wasip2".into(),
    ))
}

#[allow(dead_code)]
async fn apply_tls(tcp: ClientTransport, config: &Config) -> Result<PgTransport<ClientTransport>> {
    if matches!(config.get_ssl_mode(), SslMode::Disable) {
        Ok(PgTransport::Plain(BufferedTransport::new(tcp)))
    } else {
        let tls_config = TlsConfig {
            mode: config.get_ssl_mode(),
            server_name: config.get_host().into(),
            accept_invalid_certs: config.get_accept_invalid_certs(),
            ..Default::default()
        };
        crate::transport::negotiate_tls(tcp, &tls_config)
            .await
            .map_err(PgError::Transport)
    }
}

/// Platform-aware async sleep for reconnection backoff.
/// Uses `wstd::time::Timer::after` on WASI P2.
#[cfg(target_arch = "wasm32")]
async fn reconnect_sleep(duration: std::time::Duration) {
    wstd::time::Timer::after(duration.into()).wait().await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn reconnect_sleep(duration: std::time::Duration) {
    #[cfg(feature = "tokio-transport")]
    tokio::time::sleep(duration).await;

    #[cfg(not(feature = "tokio-transport"))]
    {
        std::thread::sleep(duration);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::MockTransport;

    /// Compile-time assertion that `Connection` is `Send` on WASI.
    /// This verifies that the wstd 0.6 upgrade (Arc instead of Rc) works.
    #[test]
    #[cfg(target_arch = "wasm32")]
    fn connection_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Connection>();
    }

    #[test]
    fn test_connection_state_transitions() {
        let transport = PgTransport::Plain(BufferedTransport::new(ClientTransport::Mock(
            MockTransport::new(vec![]),
        )));
        let mut conn = Connection {
            transport,
            codec: auth::Codec::new(),
            server_params: ServerParams::default(),
            state: ConnectionState::Idle,
            config: Config::new(),
            transaction_status: TransactionStatus::Idle,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
            needs_recovery: false,
            health: crate::reconnect::session::ConnectionHealth::new(),
            session_state: crate::reconnect::session::SessionState::new(),
        };

        assert!(conn.is_idle());
        assert!(!conn.is_closed());

        conn.transition(ConnectionState::ActiveSimpleQuery).unwrap();
        assert_eq!(conn.state(), ConnectionState::ActiveSimpleQuery);

        conn.transition(ConnectionState::Idle).unwrap();
        assert!(conn.is_idle());

        conn.transition(ConnectionState::Closing).unwrap();
        assert_eq!(conn.state(), ConnectionState::Closing);

        conn.transition(ConnectionState::Closed).unwrap();
        assert!(conn.is_closed());
    }

    #[test]
    fn test_invalid_state_transition() {
        let transport = PgTransport::Plain(BufferedTransport::new(ClientTransport::Mock(
            MockTransport::new(vec![]),
        )));
        let mut conn = Connection {
            transport,
            codec: auth::Codec::new(),
            server_params: ServerParams::default(),
            state: ConnectionState::ActiveSimpleQuery,
            config: Config::new(),
            transaction_status: TransactionStatus::Idle,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
            needs_recovery: false,
            health: crate::reconnect::session::ConnectionHealth::new(),
            session_state: crate::reconnect::session::SessionState::new(),
        };

        assert!(conn.transition(ConnectionState::Streaming).is_err());
    }

    #[test]
    fn test_is_healthy_idle() {
        let transport = PgTransport::Plain(BufferedTransport::new(ClientTransport::Mock(
            MockTransport::new(vec![]),
        )));
        let conn = Connection {
            transport,
            codec: auth::Codec::new(),
            server_params: ServerParams::default(),
            state: ConnectionState::Idle,
            config: Config::new(),
            transaction_status: TransactionStatus::Idle,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
            needs_recovery: false,
            health: crate::reconnect::session::ConnectionHealth::new(),
            session_state: crate::reconnect::session::SessionState::new(),
        };
        assert!(conn.is_healthy());
    }

    #[test]
    fn test_is_healthy_closed() {
        let transport = PgTransport::Plain(BufferedTransport::new(ClientTransport::Mock(
            MockTransport::new(vec![]),
        )));
        let conn = Connection {
            transport,
            codec: auth::Codec::new(),
            server_params: ServerParams::default(),
            state: ConnectionState::Closed,
            config: Config::new(),
            transaction_status: TransactionStatus::Idle,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
            needs_recovery: false,
            health: crate::reconnect::session::ConnectionHealth::new(),
            session_state: crate::reconnect::session::SessionState::new(),
        };
        assert!(!conn.is_healthy());
    }

    #[test]
    fn test_is_healthy_failed_transaction() {
        let transport = PgTransport::Plain(BufferedTransport::new(ClientTransport::Mock(
            MockTransport::new(vec![]),
        )));
        let conn = Connection {
            transport,
            codec: auth::Codec::new(),
            server_params: ServerParams::default(),
            state: ConnectionState::Idle,
            config: Config::new(),
            transaction_status: TransactionStatus::Failed,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
            needs_recovery: false,
            health: crate::reconnect::session::ConnectionHealth::new(),
            session_state: crate::reconnect::session::SessionState::new(),
        };
        assert!(!conn.is_healthy());
    }

    #[test]
    fn test_is_healthy_in_transaction() {
        let transport = PgTransport::Plain(BufferedTransport::new(ClientTransport::Mock(
            MockTransport::new(vec![]),
        )));
        let conn = Connection {
            transport,
            codec: auth::Codec::new(),
            server_params: ServerParams::default(),
            state: ConnectionState::Idle,
            config: Config::new(),
            transaction_status: TransactionStatus::InTransaction,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
            needs_recovery: false,
            health: crate::reconnect::session::ConnectionHealth::new(),
            session_state: crate::reconnect::session::SessionState::new(),
        };
        // InTransaction is still healthy (just busy)
        assert!(conn.is_healthy());
    }

    fn build_command_complete_msg(tag: &str) -> Vec<u8> {
        let mut buf = vec![b'C'];
        let mut body = Vec::new();
        body.extend_from_slice(tag.as_bytes());
        body.push(0);
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    fn build_ready_for_query(status: u8) -> Vec<u8> {
        vec![b'Z', 0, 0, 0, 5, status]
    }

    #[test]
    fn test_build_set_param_sql_quotes_and_escapes() {
        let sql = Connection::build_set_param_sql("time\"zone", "O'Reilly\\UTC");
        assert_eq!(sql, "SET \"time\"\"zone\" = 'O''Reilly\\\\UTC'");
    }

    #[test]
    fn test_build_listen_and_unlisten_sql_quote_identifier() {
        assert_eq!(
            Connection::build_listen_sql("chan\"nel"),
            "LISTEN \"chan\"\"nel\""
        );
        assert_eq!(
            Connection::build_unlisten_sql("chan\"nel"),
            "UNLISTEN \"chan\"\"nel\""
        );
    }

    fn make_connection(read_data: Vec<u8>) -> Connection {
        let transport = PgTransport::Plain(BufferedTransport::new(ClientTransport::Mock(
            MockTransport::new(read_data),
        )));
        Connection {
            transport,
            codec: auth::Codec::new(),
            server_params: ServerParams::default(),
            state: ConnectionState::Idle,
            config: Config::new(),
            transaction_status: TransactionStatus::Idle,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
            needs_recovery: false,
            health: crate::reconnect::session::ConnectionHealth::new(),
            session_state: crate::reconnect::session::SessionState::new(),
        }
    }

    #[tokio::test]
    async fn test_set_param() {
        // Build mock response for SET command
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("SET"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        assert!(conn.session_state.custom_gucs().get("timezone").is_none());
        conn.set_param("timezone", "UTC").await.unwrap();
        assert_eq!(
            conn.session_state.custom_gucs().get("timezone"),
            Some(&"UTC".to_string())
        );
    }

    #[tokio::test]
    async fn test_reset_clears_session_state_but_preserves_reconnect_init_sql() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("ROLLBACK"));
        data.extend_from_slice(&build_ready_for_query(b'I'));
        data.extend_from_slice(&build_command_complete_msg("DISCARD ALL"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        conn.transaction_status = TransactionStatus::Failed;
        conn.session_state.track_prepare("stmt1", "SELECT 1");
        conn.session_state.track_listen("events");
        conn.session_state.track_temp_table("tmp_data");
        conn.session_state.track_set_guc("timezone", "UTC");
        conn.session_state.set_in_transaction(true);
        conn.set_reconnect_init_sql("SET timezone = 'UTC'");

        conn.reset().await.unwrap();

        assert_eq!(conn.transaction_status(), TransactionStatus::Idle);
        assert!(!conn.session_state().has_state());
        assert!(conn.session_state().listen_channels().is_empty());
        assert!(conn.session_state().prepared_statements().is_empty());
        assert!(conn.session_state().temporary_tables().is_empty());
        assert!(conn.session_state().custom_gucs().is_empty());
        assert!(conn.session_state().is_reconnect_safe());
        assert_eq!(conn.reconnect_init_sql(), Some("SET timezone = 'UTC'"));
    }

    #[test]
    fn test_reconnect_init_sql_accessors() {
        let mut conn = make_connection(vec![]);
        assert_eq!(conn.reconnect_init_sql(), None);

        conn.set_reconnect_init_sql("SET timezone = 'UTC'");
        assert_eq!(conn.reconnect_init_sql(), Some("SET timezone = 'UTC'"));
        assert_eq!(
            conn.session_state.reconnect_init_sql(),
            Some("SET timezone = 'UTC'")
        );

        conn.clear_reconnect_init_sql();
        assert_eq!(conn.reconnect_init_sql(), None);
        assert_eq!(conn.session_state.reconnect_init_sql(), None);
    }
}
