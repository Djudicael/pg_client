//! Async TCP transport using `tokio::net::TcpStream`.
//!
//! This module provides a proper async TCP transport for native (non-WASI)
//! builds. It is gated behind the `tokio-transport` feature and uses
//! `tokio::io` for fully asynchronous I/O — no blocking.

use std::time::Duration;

use super::{AsyncTransport, TransportError};

#[cfg(feature = "tracing")]
use crate::tracing_ext::TARGET_TRANSPORT;

/// Async TCP transport backed by `tokio::net::TcpStream`.
///
/// This is the recommended transport for native (non-WASI) production builds.
/// It uses Tokio's fully asynchronous I/O and will not block the runtime.
#[derive(Debug)]
pub struct TokioTcpTransport {
    stream: tokio::net::TcpStream,
}

impl TokioTcpTransport {
    /// Establish an async TCP connection to the given host and port.
    pub async fn connect(host: &str, port: u16) -> Result<Self, TransportError> {
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_TRANSPORT, host = %host, port = port, "Connecting to PostgreSQL via TCP (tokio)");

        let addr = format!("{}:{}", host, port);
        let stream = match tokio::net::TcpStream::connect(&addr).await {
            Ok(s) => s,
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(target: TARGET_TRANSPORT, host = %host, port = port, error = %e, "TCP connection failed");
                return Err(TransportError::Io(e.to_string()));
            }
        };
        // Disable Nagle's algorithm — the PostgreSQL wire protocol sends
        // many small messages and expects them to be delivered promptly.
        stream
            .set_nodelay(true)
            .map_err(|e| TransportError::Io(e.to_string()))?;

        #[cfg(feature = "tracing")]
        tracing::info!(target: TARGET_TRANSPORT, host = %host, port = port, "TCP connection established (tokio)");

        Ok(Self { stream })
    }

    /// Connect with an optional timeout.
    pub async fn connect_with_timeout(
        host: &str,
        port: u16,
        timeout: Option<Duration>,
    ) -> Result<Self, TransportError> {
        match timeout {
            Some(dur) => {
                let connect_fut = Self::connect(host, port);
                let timeout_fut = tokio::time::sleep(dur);

                tokio::select! {
                    result = connect_fut => result,
                    _ = timeout_fut => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(target: TARGET_TRANSPORT, host = %host, port = port, "TCP connection timed out");
                        Err(TransportError::Timeout)
                    }
                }
            }
            None => Self::connect(host, port).await,
        }
    }
}

/// Connect with an optional timeout.
///
/// On timeout, the in-progress TCP connection is dropped.
pub async fn connect_with_timeout(
    host: &str,
    port: u16,
    timeout: Option<Duration>,
) -> Result<TokioTcpTransport, TransportError> {
    TokioTcpTransport::connect_with_timeout(host, port, timeout).await
}

impl Drop for TokioTcpTransport {
    fn drop(&mut self) {
        // When tokio::net::TcpStream is dropped, the underlying file descriptor
        // is closed, which sends a TCP FIN to the server. This is sufficient
        // for proper cleanup — no explicit shutdown() call is needed.
        //
        // Note: we do NOT send a PostgreSQL Terminate message here because
        // Drop cannot perform async I/O. For a clean protocol-level shutdown,
        // users must call `conn.close().await` before dropping.
        //
        // This prevents connection accumulation in long-running test suites
        // where connections are dropped without calling close().await.
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: TARGET_TRANSPORT,
            "TokioTcpTransport dropped; fd will be closed by tokio"
        );
    }
}

impl AsyncTransport for TokioTcpTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        use tokio::io::AsyncReadExt;
        self.stream
            .read(buf)
            .await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        use tokio::io::AsyncWriteExt;
        self.stream
            .write(buf)
            .await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        use tokio::io::AsyncWriteExt;
        self.stream
            .write_all(buf)
            .await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        tokio::io::AsyncReadExt::read_exact(&mut self.stream, buf)
            .await
            .map(|_| ())
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::UnexpectedEof => TransportError::UnexpectedEof,
                _ => TransportError::Io(e.to_string()),
            })
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        use tokio::io::AsyncWriteExt;
        self.stream
            .flush()
            .await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        use tokio::io::AsyncWriteExt;
        self.stream
            .shutdown()
            .await
            .map_err(|e| TransportError::Io(e.to_string()))
    }
}
