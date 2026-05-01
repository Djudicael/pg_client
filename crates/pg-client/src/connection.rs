//! PostgreSQL connection management.
//!
//! This module defines the `Connection` struct which represents a connection to a PostgreSQL server.
//! It handles the connection lifecycle, authentication, protocol state, and graceful close.

use std::collections::VecDeque;

use pg_protocol::{BackendMessage, FrontendMessage, TransactionStatus};

use crate::auth::{self, ServerParams};
use crate::config::Config;
use crate::ensure_random_available;
use crate::error::{Error, Result};
use crate::notification::Notification;
use crate::query::{Notice, NoticeHandler};
use crate::transport::{
    AsyncTransport, BufferedTransport, ClientTransport, PgTransport, TlsConfig,
};

// ---------------------------------------------------------------------------
// Connection state machine
// ---------------------------------------------------------------------------

/// Internal state of a PostgreSQL connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub async fn connect(config: Config) -> Result<Self> {
        ensure_random_available();

        let mut transport = build_pg_transport(&config).await?;
        let mut codec = auth::Codec::new();

        // Send StartupMessage
        let startup = pg_protocol::FrontendMessage::Startup {
            params: config.startup_params(),
        };
        codec
            .send(&mut transport, &startup)
            .await
            .map_err(Error::from)?;

        // Authenticate
        let server_params = auth::authenticate(&mut transport, &mut codec, &config)
            .await
            .map_err(Error::from)?;

        Ok(Self {
            transport,
            codec,
            server_params,
            state: ConnectionState::Idle,
            config,
            transaction_status: TransactionStatus::Idle,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
        })
    }

    /// Convenience: connect from a connection string (URI or key-value format).
    pub async fn connect_str(s: &str) -> Result<Self> {
        let config = Config::from_uri(s)
            .or_else(|_| Config::from_key_value(s))
            .map_err(|e| Error::Config(e.to_string()))?;
        Self::connect(config).await
    }

    // =======================================================================
    // Accessors
    // =======================================================================

    /// Returns a reference to the configuration used for this connection.
    pub fn config(&self) -> &Config {
        &self.config
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

    pub(crate) fn handle_notice(&self, notice: &Notice) {
        if let Some(ref handler) = self.notice_handler {
            handler(notice);
        } else {
            #[cfg(feature = "tracing")]
            tracing::warn!(severity = %notice.severity, code = %notice.code, message = %notice.message, "server notice");
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
    pub async fn close(&mut self) -> Result<()> {
        if self.state.is_closed() {
            return Ok(());
        }

        self.state = ConnectionState::Closing;

        // Best-effort: send Terminate, ignore errors.
        let _ = self
            .codec
            .send(&mut self.transport, &FrontendMessage::Terminate)
            .await;

        let _ = self.transport.shutdown().await;

        self.state = ConnectionState::Closed;
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
                    self.notification_queue.push_back(Notification {
                        process_id: body.process_id(),
                        channel: body.channel().unwrap_or("").to_string(),
                        payload: body.message().unwrap_or("").to_string(),
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
                self.notification_queue.push_back(Notification {
                    process_id: body.process_id(),
                    channel: body.channel().unwrap_or("").to_string(),
                    payload: body.message().unwrap_or("").to_string(),
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
                return Err(Error::InvalidState(format!(
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
                        } else if let Err(_) = tcp.stream.flush() {
                            false
                        } else {
                            true
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
                            if let Some(pos) = scan_buf.windows(5).position(|w| w[0] == b'Z') {
                                // Verify it looks like ReadyForQuery: 'Z' + 4-byte length (5) + status
                                if scan_buf.len() > pos + 4 {
                                    let len = i32::from_be_bytes([
                                        scan_buf[pos + 1],
                                        scan_buf[pos + 2],
                                        scan_buf[pos + 3],
                                        scan_buf[pos + 4],
                                    ]);
                                    if len == 5 && scan_buf.len() >= pos + 5 + 1 {
                                        return true;
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
        // We cannot perform async I/O in Drop.
        // Best-effort: the transport's Drop will close the TCP socket.
        // For a clean shutdown users must call `conn.close().await`.
        self.state = ConnectionState::Closed;
    }
}

// ---------------------------------------------------------------------------
// Platform-specific transport construction
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
async fn build_pg_transport(config: &Config) -> Result<PgTransport<ClientTransport>> {
    let tcp = crate::transport::WasiTcpTransport::connect(config.get_host(), config.get_port())
        .await
        .map_err(|e| Error::Connection(e.to_string()))?;
    apply_tls(ClientTransport::Wasi(tcp), config).await
}

#[cfg(all(not(target_arch = "wasm32"), feature = "tokio-transport"))]
async fn build_pg_transport(config: &Config) -> Result<PgTransport<ClientTransport>> {
    let tcp = crate::transport::TokioTcpTransport::connect(config.get_host(), config.get_port())
        .await
        .map_err(|e| Error::Connection(e.to_string()))?;
    apply_tls(ClientTransport::Tokio(tcp), config).await
}

#[cfg(all(
    not(target_arch = "wasm32"),
    not(feature = "tokio-transport"),
    feature = "test-native"
))]
async fn build_pg_transport(config: &Config) -> Result<PgTransport<ClientTransport>> {
    let tcp = crate::transport::NativeTcpTransport::connect(config.get_host(), config.get_port())
        .map_err(|e| Error::Connection(e.to_string()))?;
    apply_tls(ClientTransport::Native(tcp), config).await
}

#[cfg(all(
    not(target_arch = "wasm32"),
    not(feature = "tokio-transport"),
    not(feature = "test-native")
))]
async fn build_pg_transport(_config: &Config) -> Result<PgTransport<ClientTransport>> {
    Err(Error::Unsupported(
        "no transport available for this target. Enable the 'tokio-transport' feature (recommended) or 'test-native' feature, or compile for wasm32-wasip2".into(),
    ))
}

async fn apply_tls(tcp: ClientTransport, config: &Config) -> Result<PgTransport<ClientTransport>> {
    if config.get_use_tls() {
        let tls_config = TlsConfig {
            mode: config.get_ssl_mode(),
            server_name: config.get_host().into(),
            accept_invalid_certs: config.get_accept_invalid_certs(),
            ..Default::default()
        };
        crate::transport::negotiate_tls(tcp, &tls_config)
            .await
            .map_err(|e| Error::Connection(e.to_string()))
    } else {
        Ok(PgTransport::Plain(BufferedTransport::new(tcp)))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::MockTransport;

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
        };

        assert!(conn.transition(ConnectionState::Streaming).is_err());
    }
}
