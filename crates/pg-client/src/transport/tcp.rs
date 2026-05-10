use std::time::Duration;

use futures_concurrency::future::Race;
use wasip2::sockets::{
    instance_network::instance_network,
    network::{Ipv4SocketAddress, Ipv6SocketAddress},
    tcp::{IpAddressFamily, IpSocketAddress, ShutdownType, TcpSocket},
    tcp_create_socket::create_tcp_socket,
};
use wstd::io::{AsyncInputStream, AsyncOutputStream, AsyncWrite};
use wstd::runtime::AsyncPollable;

use super::error::TransportError;
use super::AsyncTransport;

#[cfg(feature = "tracing")]
use crate::tracing_ext::TARGET_TRANSPORT;

/// WASI P2 TCP transport using raw `wasi:sockets/tcp` bindings wrapped in
/// `wstd::io` async streams.
#[derive(Debug)]
pub struct WasiTcpTransport {
    input: AsyncInputStream,
    output: AsyncOutputStream,
    socket: TcpSocket,
}

impl WasiTcpTransport {
    /// Establish a TCP connection to the given host and port.
    ///
    /// DNS resolution is attempted via `std::net::ToSocketAddrs` if the host
    /// is not a plain IP address.
    pub async fn connect(host: &str, port: u16) -> Result<Self, TransportError> {
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_TRANSPORT, host = %host, port = port, "Connecting to PostgreSQL via TCP (WASI P2)");

        let std_addr = resolve_address(host, port)?;

        let family = match std_addr {
            std::net::SocketAddr::V4(_) => IpAddressFamily::Ipv4,
            std::net::SocketAddr::V6(_) => IpAddressFamily::Ipv6,
        };

        let socket = create_tcp_socket(family)
            .map_err(|e| TransportError::Io(format!("create_tcp_socket: {:?}", e)))?;
        let network = instance_network();

        let wasi_addr = sockaddr_to_wasi(std_addr);

        if let Err(e) = socket.start_connect(&network, wasi_addr) {
            #[cfg(feature = "tracing")]
            tracing::warn!(target: TARGET_TRANSPORT, host = %host, port = port, error = %format!("{:?}", e), "TCP start_connect failed");
            return Err(TransportError::Io(format!("start_connect: {:?}", e)));
        }
        AsyncPollable::new(socket.subscribe()).wait_for().await;

        let (input, output) = match socket.finish_connect() {
            Ok(streams) => streams,
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(target: TARGET_TRANSPORT, host = %host, port = port, error = %format!("{:?}", e), "TCP finish_connect failed");
                return Err(TransportError::Io(format!("finish_connect: {:?}", e)));
            }
        };

        #[cfg(feature = "tracing")]
        tracing::info!(target: TARGET_TRANSPORT, host = %host, port = port, "TCP connection established (WASI P2)");

        Ok(Self {
            input: AsyncInputStream::new(input),
            output: AsyncOutputStream::new(output),
            socket,
        })
    }
}

impl Drop for WasiTcpTransport {
    fn drop(&mut self) {
        // Best-effort: shut down the socket synchronously so the server
        // receives a TCP FIN promptly. Without this, the WASI resource
        // destructor may not close the underlying socket, causing
        // connection leaks in long-running processes.
        //
        // We cannot send a PostgreSQL Terminate message here (async I/O
        // is impossible in Drop), but the TCP FIN is enough for the
        // server to detect the disconnection.
        let _ = self.socket.shutdown(ShutdownType::Both);
    }
}

impl AsyncTransport for WasiTcpTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        self.input
            .read(buf)
            .await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        self.output
            .write(buf)
            .await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.output
            .write_all(buf)
            .await
            .map_err(|e| TransportError::Io(e.to_string()))
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
        self.output
            .flush()
            .await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        self.socket
            .shutdown(ShutdownType::Both)
            .map_err(|e| TransportError::Io(format!("{:?}", e)))?;
        Ok(())
    }
}

/// Connect with an optional timeout.
///
/// On timeout, the in-progress TCP connection is dropped (closing the socket).
pub async fn connect_with_timeout(
    host: &str,
    port: u16,
    timeout: Option<Duration>,
) -> Result<WasiTcpTransport, TransportError> {
    match timeout {
        Some(duration) => {
            let connect_fut = WasiTcpTransport::connect(host, port);
            let timeout_fut = async {
                wstd::time::Timer::after(duration.into()).wait().await;
                Err(TransportError::Timeout)
            };
            // Race: first one to complete wins; the other is dropped.
            let result = (connect_fut, timeout_fut).race().await;
            if matches!(&result, Err(TransportError::Timeout)) {
                #[cfg(feature = "tracing")]
                tracing::warn!(target: TARGET_TRANSPORT, host = %host, port = port, "TCP connection timed out");
            }
            result
        }
        None => WasiTcpTransport::connect(host, port).await,
    }
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

fn resolve_address(host: &str, port: u16) -> Result<std::net::SocketAddr, TransportError> {
    let addr_str = format!("{}:{}", host, port);

    // Fast path: already an IP address.
    if let Ok(addr) = addr_str.parse::<std::net::SocketAddr>() {
        return Ok(addr);
    }

    // Try DNS resolution via std::net::ToSocketAddrs.
    use std::net::ToSocketAddrs;
    let mut addrs =
        addr_str
            .to_socket_addrs()
            .map_err(|_| TransportError::DnsResolutionFailed {
                host: host.to_string(),
            })?;

    addrs
        .next()
        .ok_or_else(|| TransportError::DnsResolutionFailed {
            host: host.to_string(),
        })
}

fn sockaddr_to_wasi(addr: std::net::SocketAddr) -> IpSocketAddress {
    match addr {
        std::net::SocketAddr::V4(addr) => {
            let ip = addr.ip().octets();
            IpSocketAddress::Ipv4(Ipv4SocketAddress {
                address: (ip[0], ip[1], ip[2], ip[3]),
                port: addr.port(),
            })
        }
        std::net::SocketAddr::V6(addr) => {
            let ip = addr.ip().segments();
            IpSocketAddress::Ipv6(Ipv6SocketAddress {
                address: (ip[0], ip[1], ip[2], ip[3], ip[4], ip[5], ip[6], ip[7]),
                port: addr.port(),
                flow_info: addr.flowinfo(),
                scope_id: addr.scope_id(),
            })
        }
    }
}
