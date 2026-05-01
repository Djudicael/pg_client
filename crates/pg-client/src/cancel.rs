//! PostgreSQL out-of-band query cancellation.
//!
//! PostgreSQL allows cancelling a running query from a **separate connection**.
//! During connection setup, the server sends `BackendKeyData` containing a
//! process ID and secret key. To cancel a query, a new TCP connection is
//! opened and a `CancelRequest` message is sent with these credentials.
//!
//! # Example
//! ```ignore
//! let cancel_token = conn.cancel_token();
//!
//! // In another task/thread:
//! cancel_token.cancel().await?;
//! ```

use std::time::Duration;

use pg_protocol::FrontendMessage;

use crate::auth::Codec;
use crate::config::Config;
use crate::error::{Error, PgError, Result};
use crate::transport::{AsyncTransport, SslMode};

// ---------------------------------------------------------------------------
// CancelToken
// ---------------------------------------------------------------------------

/// A token that can be used to cancel a running query on a connection.
///
/// The token contains the host, port, process ID, and secret key needed
/// to send an out-of-band cancellation request. It can be cloned and sent
/// to another task or thread.
///
/// # Example
/// ```ignore
/// let token = conn.cancel_token();
///
/// // Spawn a task that will cancel the query after 5 seconds
/// tokio::spawn(async move {
///     tokio::time::sleep(Duration::from_secs(5)).await;
///     token.cancel().await.unwrap();
/// });
///
/// // This long-running query will be cancelled
/// conn.query("SELECT pg_sleep(60)").await?;
/// ```
#[derive(Debug, Clone)]
pub struct CancelToken {
    /// Hostname or IP address of the PostgreSQL server.
    pub(crate) host: String,
    /// Port number of the PostgreSQL server.
    pub(crate) port: u16,
    /// Backend process ID from `BackendKeyData`.
    pub(crate) process_id: i32,
    /// Secret key from `BackendKeyData`.
    pub(crate) secret_key: i32,
    /// SSL mode for the cancellation connection.
    pub(crate) ssl_mode: SslMode,
    /// Whether to accept invalid TLS certificates.
    pub(crate) accept_invalid_certs: bool,
}

impl CancelToken {
    /// Send a cancellation request to the server.
    ///
    /// This opens a **new** TCP connection to the server, sends a
    /// `CancelRequest` message, and closes the connection. The server
    /// will then attempt to cancel the running query on the original
    /// connection.
    ///
    /// Note that cancellation is not guaranteed — the server may not
    /// be able to cancel the query if it's in a non-interruptible state.
    /// The cancellation request itself is always acknowledged.
    ///
    /// # Errors
    /// Returns an error if the TCP connection cannot be established or
    /// the cancellation message cannot be sent.
    pub async fn cancel(&self) -> Result<()> {
        self.cancel_with_timeout(None).await
    }

    /// Send a cancellation request with an optional connection timeout.
    pub async fn cancel_with_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        // Build a temporary config for the cancellation connection
        let mut config = Config::new()
            .host(&self.host)
            .port(self.port)
            .user("cancel"); // User doesn't matter for CancelRequest

        config = config.ssl_mode(self.ssl_mode);
        if self.accept_invalid_certs {
            config = config.accept_invalid_certs(true);
        }

        // Open a new TCP connection
        let mut transport = build_cancel_transport(&config, timeout).await?;
        let mut codec = Codec::new();

        // Send CancelRequest message
        codec
            .send(
                &mut transport,
                &FrontendMessage::CancelRequest {
                    process_id: self.process_id,
                    secret_key: self.secret_key,
                },
            )
            .await
            .map_err(Error::from)?;

        // Close the connection — the server processes the cancel and
        // closes its end. We don't need to read any response.
        let _ = transport.shutdown().await;

        Ok(())
    }

    /// Returns the process ID of the backend this token can cancel.
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    /// Returns the secret key of the backend this token can cancel.
    pub fn secret_key(&self) -> i32 {
        self.secret_key
    }
}

// ---------------------------------------------------------------------------
// Platform-specific transport construction for cancellation
// ---------------------------------------------------------------------------

#[cfg(target_arch = "wasm32")]
async fn build_cancel_transport(
    config: &Config,
    timeout: Option<Duration>,
) -> Result<crate::transport::PgTransport<crate::transport::ClientTransport>> {
    use crate::transport::{connect_with_timeout, BufferedTransport, ClientTransport, PgTransport};

    let tcp = connect_with_timeout(config.get_host(), config.get_port(), timeout)
        .await
        .map_err(|e| PgError::Transport(e))?;

    Ok(PgTransport::Plain(BufferedTransport::new(
        ClientTransport::Wasi(tcp),
    )))
}

#[cfg(all(not(target_arch = "wasm32"), feature = "tokio-transport"))]
async fn build_cancel_transport(
    config: &Config,
    timeout: Option<Duration>,
) -> Result<crate::transport::PgTransport<crate::transport::ClientTransport>> {
    use crate::transport::{connect_with_timeout, BufferedTransport, ClientTransport, PgTransport};

    let tcp = connect_with_timeout(config.get_host(), config.get_port(), timeout)
        .await
        .map_err(|e| PgError::Transport(e))?;

    Ok(PgTransport::Plain(BufferedTransport::new(
        ClientTransport::Tokio(tcp),
    )))
}

#[cfg(all(
    not(target_arch = "wasm32"),
    not(feature = "tokio-transport"),
    feature = "test-native"
))]
async fn build_cancel_transport(
    config: &Config,
    timeout: Option<Duration>,
) -> Result<crate::transport::PgTransport<crate::transport::ClientTransport>> {
    use crate::transport::{BufferedTransport, ClientTransport, NativeTcpTransport, PgTransport};

    let tcp =
        NativeTcpTransport::connect_with_timeout(config.get_host(), config.get_port(), timeout)
            .map_err(|e| PgError::Transport(e))?;

    Ok(PgTransport::Plain(BufferedTransport::new(
        ClientTransport::Native(tcp),
    )))
}

#[cfg(all(
    not(target_arch = "wasm32"),
    not(feature = "tokio-transport"),
    not(feature = "test-native")
))]
async fn build_cancel_transport(
    _config: &Config,
    _timeout: Option<Duration>,
) -> Result<crate::transport::PgTransport<crate::transport::ClientTransport>> {
    Err(PgError::Unsupported(
        "no transport available for cancellation. Enable the 'tokio-transport' feature (recommended) or 'test-native' feature, or compile for wasm32-wasip2".into(),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancel_token_clone() {
        let token = CancelToken {
            host: "localhost".to_string(),
            port: 5432,
            process_id: 12345,
            secret_key: 67890,
            ssl_mode: SslMode::Disable,
            accept_invalid_certs: false,
        };

        let cloned = token.clone();
        assert_eq!(cloned.host, "localhost");
        assert_eq!(cloned.port, 5432);
        assert_eq!(cloned.process_id, 12345);
        assert_eq!(cloned.secret_key, 67890);
    }

    #[test]
    fn test_cancel_token_accessors() {
        let token = CancelToken {
            host: "localhost".to_string(),
            port: 5432,
            process_id: 42,
            secret_key: 99,
            ssl_mode: SslMode::Disable,
            accept_invalid_certs: false,
        };

        assert_eq!(token.process_id(), 42);
        assert_eq!(token.secret_key(), 99);
    }
}
