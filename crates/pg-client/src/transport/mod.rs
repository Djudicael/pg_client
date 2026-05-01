//! Transport layer for PostgreSQL client.
//!
//! This module defines the `AsyncTransport` trait which abstracts over the underlying
//! I/O for the PostgreSQL wire protocol. Implementations are provided for TCP (with or without TLS)
//! and for testing (mock transport).

mod buffered;
mod error;
mod params;
mod tls;

#[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
mod native;

#[cfg(target_arch = "wasm32")]
mod tcp;

#[allow(unused_imports)]
pub use buffered::BufferedTransport;
pub use error::TransportError;
#[allow(unused_imports)]
pub use params::ConnectionParams;
pub use tls::{negotiate_tls, PgTransport, SslMode, TlsConfig, TlsInfo};

#[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
#[allow(unused_imports)]
pub use native::NativeTcpTransport;

#[cfg(target_arch = "wasm32")]
#[allow(unused_imports)]
pub use tcp::WasiTcpTransport;

// ---------------------------------------------------------------------------
// Platform-agnostic transport enum (so Connection does not need to be generic)
// ---------------------------------------------------------------------------

/// A transport implementation selected at compile time for the target platform.
#[derive(Debug)]
pub enum ClientTransport {
    /// Native (blocking) TCP transport for non-WASI targets.
    #[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
    Native(NativeTcpTransport),
    /// WASI Preview 2 async TCP transport.
    #[cfg(target_arch = "wasm32")]
    Wasi(WasiTcpTransport),
    /// Mock transport for unit tests.
    #[cfg(test)]
    Mock(MockTransport),
    /// Placeholder to keep the enum inhabited when all other variants
    /// are cfg'd out. Never constructed in practice.
    #[doc(hidden)]
    __Unused,
}

impl AsyncTransport for ClientTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        match self {
            #[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
            ClientTransport::Native(t) => t.read(buf).await,
            #[cfg(target_arch = "wasm32")]
            ClientTransport::Wasi(t) => t.read(buf).await,
            #[cfg(test)]
            ClientTransport::Mock(t) => t.read(buf).await,
            ClientTransport::__Unused => unreachable!(),
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        match self {
            #[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
            ClientTransport::Native(t) => t.write(buf).await,
            #[cfg(target_arch = "wasm32")]
            ClientTransport::Wasi(t) => t.write(buf).await,
            #[cfg(test)]
            ClientTransport::Mock(t) => t.write(buf).await,
            ClientTransport::__Unused => unreachable!(),
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        match self {
            #[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
            ClientTransport::Native(t) => t.write_all(buf).await,
            #[cfg(target_arch = "wasm32")]
            ClientTransport::Wasi(t) => t.write_all(buf).await,
            #[cfg(test)]
            ClientTransport::Mock(t) => t.write_all(buf).await,
            ClientTransport::__Unused => unreachable!(),
        }
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        match self {
            #[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
            ClientTransport::Native(t) => t.read_exact(buf).await,
            #[cfg(target_arch = "wasm32")]
            ClientTransport::Wasi(t) => t.read_exact(buf).await,
            #[cfg(test)]
            ClientTransport::Mock(t) => t.read_exact(buf).await,
            ClientTransport::__Unused => unreachable!(),
        }
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        match self {
            #[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
            ClientTransport::Native(t) => t.flush().await,
            #[cfg(target_arch = "wasm32")]
            ClientTransport::Wasi(t) => t.flush().await,
            #[cfg(test)]
            ClientTransport::Mock(t) => t.flush().await,
            ClientTransport::__Unused => unreachable!(),
        }
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        match self {
            #[cfg(all(not(target_arch = "wasm32"), feature = "test-native"))]
            ClientTransport::Native(t) => t.shutdown().await,
            #[cfg(target_arch = "wasm32")]
            ClientTransport::Wasi(t) => t.shutdown().await,
            #[cfg(test)]
            ClientTransport::Mock(t) => t.shutdown().await,
            ClientTransport::__Unused => unreachable!(),
        }
    }
}

/// Async transport abstraction for PostgreSQL wire protocol I/O.
///
/// This trait uses `async fn` which means only generic dispatch is supported
/// (no `dyn AsyncTransport`). Use generic parameters in all functions that
/// need a transport:
///
/// ```rust,ignore
/// async fn do_query<T: AsyncTransport>(transport: &mut T, sql: &str) { ... }
/// ```
pub trait AsyncTransport {
    /// Read data into `buf`, returning the number of bytes read.
    /// Returns 0 only if the connection is closed (EOF).
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError>;

    /// Write data from `buf`, returning the number of bytes written.
    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError>;

    /// Write all data from `buf`. Retries partial writes internally.
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError>;

    /// Read exactly `buf.len()` bytes. Returns `TransportError::UnexpectedEof`
    /// if the connection closes before the buffer is full.
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError>;

    /// Flush any buffered write data to the underlying transport.
    async fn flush(&mut self) -> Result<(), TransportError>;

    /// Shut down the transport (close the connection).
    async fn shutdown(&mut self) -> Result<(), TransportError>;
}

// ============================================================================
// Mock transport for unit tests
// ============================================================================

#[cfg(test)]
#[derive(Debug)]
pub struct MockTransport {
    /// Data to be returned by `read` calls.
    read_data: Vec<u8>,
    /// Current position in `read_data`.
    read_pos: usize,
    /// Maximum number of bytes to return per `read` call (0 = unlimited).
    max_read_chunk: usize,
    /// All data written via `write` / `write_all`.
    pub written: Vec<u8>,
    /// Whether the transport is closed.
    closed: bool,
    /// Whether flush has been called.
    pub flushed: bool,
    /// Whether shutdown has been called.
    pub shutdown_called: bool,
}

#[cfg(test)]
impl MockTransport {
    pub fn new(read_data: Vec<u8>) -> Self {
        Self {
            read_data,
            read_pos: 0,
            max_read_chunk: 0,
            written: Vec::new(),
            closed: false,
            flushed: false,
            shutdown_called: false,
        }
    }

    /// Limit the number of bytes returned by each `read` call.
    pub fn with_max_read_chunk(mut self, chunk: usize) -> Self {
        self.max_read_chunk = chunk;
        self
    }

    /// Returns all data that has been written so far.
    pub fn written(&self) -> &[u8] {
        &self.written
    }

    /// Returns true if all read data has been consumed.
    pub fn is_read_exhausted(&self) -> bool {
        self.read_pos >= self.read_data.len()
    }
}

#[cfg(test)]
impl AsyncTransport for MockTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        if self.closed {
            return Ok(0);
        }
        if self.read_pos >= self.read_data.len() {
            return Ok(0);
        }
        let remaining = &self.read_data[self.read_pos..];
        let mut to_read = remaining.len().min(buf.len());
        if self.max_read_chunk > 0 {
            to_read = to_read.min(self.max_read_chunk);
        }
        buf[..to_read].copy_from_slice(&remaining[..to_read]);
        self.read_pos += to_read;
        Ok(to_read)
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        if self.closed {
            return Err(TransportError::ConnectionReset);
        }
        self.written.extend_from_slice(buf);
        Ok(buf.len())
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        if self.closed {
            return Err(TransportError::ConnectionReset);
        }
        self.written.extend_from_slice(buf);
        Ok(())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        let mut filled = 0;
        while filled < buf.len() {
            let n = self.read(&mut buf[filled..]).await?;
            if n == 0 {
                return Err(TransportError::UnexpectedEof);
            }
            filled += n;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        self.flushed = true;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        self.shutdown_called = true;
        self.closed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transport_error_classification() {
        assert!(TransportError::ConnectionReset.is_connection_broken());
        assert!(TransportError::UnexpectedEof.is_connection_broken());
        assert!(TransportError::ConnectionRefused.is_connection_broken());
        assert!(!TransportError::Timeout.is_connection_broken());

        assert!(TransportError::Timeout.is_transient());
        assert!(TransportError::DnsResolutionFailed {
            host: "example.com".into()
        }
        .is_transient());
        assert!(!TransportError::ConnectionReset.is_transient());
    }

    #[tokio::test]
    async fn test_mock_transport_basic_read_write() {
        let mut mock = MockTransport::new(vec![1, 2, 3, 4, 5]);

        let mut buf = [0u8; 3];
        assert_eq!(mock.read(&mut buf).await.unwrap(), 3);
        assert_eq!(&buf, &[1, 2, 3]);

        assert_eq!(mock.write(&[10, 11]).await.unwrap(), 2);
        assert_eq!(mock.written(), &[10, 11]);
    }

    #[tokio::test]
    async fn test_mock_transport_read_exact() {
        let mut mock = MockTransport::new(vec![1, 2, 3, 4, 5]).with_max_read_chunk(2);

        let mut buf = [0u8; 4];
        mock.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, &[1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_mock_transport_partial_reads() {
        let mut mock = MockTransport::new(vec![1, 2, 3, 4, 5]).with_max_read_chunk(2);

        let mut buf = [0u8; 5];
        assert_eq!(mock.read(&mut buf).await.unwrap(), 2);
        assert_eq!(&buf[..2], &[1, 2]);
        assert_eq!(mock.read(&mut buf[2..]).await.unwrap(), 2);
        assert_eq!(&buf[..4], &[1, 2, 3, 4]);
        assert_eq!(mock.read(&mut buf[4..]).await.unwrap(), 1);
        assert_eq!(&buf, &[1, 2, 3, 4, 5]);
        assert_eq!(mock.read(&mut buf).await.unwrap(), 0); // EOF
    }

    #[tokio::test]
    async fn test_mock_transport_eof_on_read_exact() {
        let mut mock = MockTransport::new(vec![1, 2]);

        let mut buf = [0u8; 5];
        assert!(matches!(
            mock.read_exact(&mut buf).await,
            Err(TransportError::UnexpectedEof)
        ));
    }
}
