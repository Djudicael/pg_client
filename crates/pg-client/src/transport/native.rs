use std::time::Duration;

use super::{AsyncTransport, TransportError};

/// Native (blocking) TCP transport for non-WASI integration testing.
///
/// This is gated behind the `test-native` feature. It uses `std::net::TcpStream`
/// and performs blocking I/O inside `async fn` bodies. This is acceptable for
/// tests because the futures are polled to completion synchronously in the test
/// executor.
#[cfg(feature = "test-native")]
#[derive(Debug)]
pub struct NativeTcpTransport {
    pub(crate) stream: std::net::TcpStream,
}

#[cfg(feature = "test-native")]
impl NativeTcpTransport {
    /// Connect to a PostgreSQL server using a blocking TCP stream.
    pub fn connect(host: &str, port: u16) -> Result<Self, TransportError> {
        let addr = format!("{}:{}", host, port);
        let stream =
            std::net::TcpStream::connect(&addr).map_err(|e| TransportError::Io(e.to_string()))?;
        stream
            .set_nonblocking(false)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        // Disable Nagle's algorithm — the PostgreSQL wire protocol sends
        // many small messages and expects them to be delivered promptly.
        // Without TCP_NODELAY, the kernel may buffer small writes, causing
        // protocol-level deadlocks (server waits for data, client waits
        // for response).
        stream
            .set_nodelay(true)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        Ok(Self { stream })
    }

    /// Connect with an optional timeout.
    pub fn connect_with_timeout(
        host: &str,
        port: u16,
        timeout: Option<Duration>,
    ) -> Result<Self, TransportError> {
        let addr = format!("{}:{}", host, port);
        let stream = match timeout {
            Some(dur) => {
                let socket_addr = addr
                    .parse::<std::net::SocketAddr>()
                    .map_err(|e| TransportError::Io(e.to_string()))?;
                std::net::TcpStream::connect_timeout(&socket_addr, dur)
            }
            None => std::net::TcpStream::connect(&addr),
        }
        .map_err(|e| TransportError::Io(e.to_string()))?;
        stream
            .set_nonblocking(false)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        Ok(Self { stream })
    }
}

#[cfg(feature = "test-native")]
impl Drop for NativeTcpTransport {
    fn drop(&mut self) {
        // Best-effort: shut down the socket synchronously so the server
        // receives a TCP FIN promptly. Without this, the OS may hold the
        // connection in TIME_WAIT, causing accumulation in test suites.
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }
}

#[cfg(feature = "test-native")]
impl AsyncTransport for NativeTcpTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        use std::io::Read;
        self.stream
            .read(buf)
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        use std::io::Write;
        self.stream
            .write(buf)
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        use std::io::Write;
        self.stream
            .write_all(buf)
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        use std::io::Read;
        self.stream.read_exact(buf).map_err(|e| match e.kind() {
            std::io::ErrorKind::UnexpectedEof => TransportError::UnexpectedEof,
            _ => TransportError::Io(e.to_string()),
        })
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        use std::io::Write;
        self.stream
            .flush()
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        self.stream
            .shutdown(std::net::Shutdown::Both)
            .map_err(|e| TransportError::Io(e.to_string()))
    }
}
