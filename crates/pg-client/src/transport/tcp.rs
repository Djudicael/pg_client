use std::time::Duration;

use futures_concurrency::future::Race;
use wasip3::sockets::{
    ip_name_lookup,
    types::{
        ErrorCode, IpAddress, IpAddressFamily, IpSocketAddress, Ipv4SocketAddress,
        Ipv6SocketAddress, TcpSocket,
    },
};
use wit_bindgen::rt::async_support::{FutureReader, StreamReader, StreamResult, StreamWriter};

use super::error::TransportError;
use super::AsyncTransport;

#[cfg(feature = "tracing")]
use crate::tracing_ext::TARGET_TRANSPORT;

type SocketResult = Result<(), ErrorCode>;

/// WASI 0.3 TCP transport using native asynchronous socket streams.
#[derive(Debug)]
pub struct WasiTcpTransport {
    _socket: TcpSocket,
    input: Option<StreamReader<u8>>,
    input_done: Option<FutureReader<SocketResult>>,
    output: Option<StreamWriter<u8>>,
    output_done: Option<FutureReader<SocketResult>>,
    pending_read_error: Option<TransportError>,
    read_eof: bool,
}

impl WasiTcpTransport {
    /// Establish a TCP connection to the given host and port.
    ///
    /// Every resolved IPv4 and IPv6 address is attempted in resolver order.
    /// Numeric IP addresses bypass DNS resolution.
    pub async fn connect(host: &str, port: u16) -> Result<Self, TransportError> {
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_TRANSPORT, host = %host, port, "Connecting to PostgreSQL via TCP (WASI 0.3)");

        let addresses = resolve_addresses(host, port).await?;
        let mut last_error = None;

        for address in addresses {
            let family = address_family(&address);
            let socket = match TcpSocket::create(family) {
                Ok(socket) => socket,
                Err(error) => {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(target: TARGET_TRANSPORT, host = %host, port, error = %error, "TCP socket creation failed for resolved address");
                    last_error = Some(map_socket_error(error));
                    continue;
                }
            };

            match socket.connect(address).await {
                Ok(()) => {
                    let (input, input_done) = socket.receive();
                    let (output, outgoing) = wasip3::wit_stream::new::<u8>();
                    let output_done = socket.send(outgoing);

                    #[cfg(feature = "tracing")]
                    tracing::info!(target: TARGET_TRANSPORT, host = %host, port, "TCP connection established (WASI 0.3)");

                    return Ok(Self {
                        _socket: socket,
                        input: Some(input),
                        input_done: Some(input_done),
                        output: Some(output),
                        output_done: Some(output_done),
                        pending_read_error: None,
                        read_eof: false,
                    });
                }
                Err(error) => {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(target: TARGET_TRANSPORT, host = %host, port, error = %error, "TCP connection attempt failed for resolved address");
                    last_error = Some(map_socket_error(error));
                }
            }
        }

        Err(
            last_error.unwrap_or_else(|| TransportError::DnsResolutionFailed {
                host: host.to_string(),
            }),
        )
    }

    async fn receive_status(&mut self) -> Result<(), TransportError> {
        match self.input_done.take() {
            Some(status) => status.await.map_err(map_socket_error),
            None => Ok(()),
        }
    }

    async fn send_status(&mut self) -> Result<(), TransportError> {
        match self.output_done.take() {
            Some(status) => status.await.map_err(map_socket_error),
            None => Ok(()),
        }
    }

    fn finish_read(
        &mut self,
        bytes_read: usize,
        terminal: Result<(), TransportError>,
    ) -> Result<usize, TransportError> {
        self.input.take();
        self.read_eof = true;

        match terminal {
            Ok(()) => Ok(bytes_read),
            Err(error) if bytes_read > 0 => {
                // AsyncRead semantics require delivering bytes before reporting
                // the terminal error on the next call.
                self.pending_read_error = Some(error);
                Ok(bytes_read)
            }
            Err(error) => Err(error),
        }
    }
}

impl Drop for WasiTcpTransport {
    fn drop(&mut self) {
        // Dropping the writable stream sends FIN; dropping the readable stream
        // is the P3 equivalent of SHUT_RD. Drop both before the socket resource.
        self.output.take();
        self.input.take();
        self.output_done.take();
        self.input_done.take();
    }
}

impl AsyncTransport for WasiTcpTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        if buf.is_empty() {
            return Ok(0);
        }
        if let Some(error) = self.pending_read_error.take() {
            return Err(error);
        }
        if self.read_eof {
            return Ok(0);
        }

        loop {
            let Some(input) = self.input.as_mut() else {
                return Ok(0);
            };
            let (status, data) = input.read(Vec::with_capacity(buf.len())).await;
            let bytes_read = data.len();
            debug_assert!(bytes_read <= buf.len());
            buf[..bytes_read].copy_from_slice(&data);

            match status {
                StreamResult::Complete(reported) => {
                    debug_assert_eq!(reported, bytes_read);
                    // An empty completed P3 stream item is not EOF. Wait for
                    // data or for the receive future to be dropped.
                    if bytes_read == 0 {
                        continue;
                    }
                    return Ok(bytes_read);
                }
                StreamResult::Dropped => {
                    let terminal = self.receive_status().await;
                    return self.finish_read(bytes_read, terminal);
                }
                StreamResult::Cancelled => {
                    return self.finish_read(
                        bytes_read,
                        Err(TransportError::Io(
                            "WASI 0.3 receive operation was cancelled".to_string(),
                        )),
                    );
                }
            }
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        if buf.is_empty() {
            return Ok(0);
        }

        let Some(output) = self.output.as_mut() else {
            return Err(TransportError::ConnectionReset);
        };
        let (status, _) = output.write(buf.to_vec()).await;

        match status {
            StreamResult::Complete(written) => Ok(written),
            StreamResult::Dropped => {
                self.output.take();
                self.send_status().await?;
                Err(TransportError::ConnectionReset)
            }
            StreamResult::Cancelled => Err(TransportError::Io(
                "WASI 0.3 send operation was cancelled".to_string(),
            )),
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        if buf.is_empty() {
            return Ok(());
        }

        let Some(output) = self.output.as_mut() else {
            return Err(TransportError::ConnectionReset);
        };
        let remaining = output.write_all(buf.to_vec()).await;
        if remaining.is_empty() {
            return Ok(());
        }

        self.output.take();
        self.send_status().await?;
        Err(TransportError::ConnectionReset)
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        let mut filled = 0;
        while filled < buf.len() {
            let read = self.read(&mut buf[filled..]).await?;
            if read == 0 {
                return Err(TransportError::UnexpectedEof);
            }
            filled += read;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        // A completed component-model stream write has already transferred its
        // bytes to the host; WASI 0.3 has no separate socket flush operation.
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        // P3 models TCP half-shutdown by dropping the corresponding streams.
        self.output.take();
        self.input.take();
        self.input_done.take();
        self.read_eof = true;
        self.send_status().await
    }
}

/// Connect with an optional timeout.
///
/// On timeout, dropping the losing connection future releases its in-progress
/// socket and associated component-model resources.
pub async fn connect_with_timeout(
    host: &str,
    port: u16,
    timeout: Option<Duration>,
) -> Result<WasiTcpTransport, TransportError> {
    match timeout {
        Some(duration) => {
            let connect = WasiTcpTransport::connect(host, port);
            let timeout = async {
                wasip3::clocks::monotonic_clock::wait_for(duration_to_wasi(duration)).await;
                Err(TransportError::Timeout)
            };
            let result = (connect, timeout).race().await;
            if matches!(result, Err(TransportError::Timeout)) {
                #[cfg(feature = "tracing")]
                tracing::warn!(target: TARGET_TRANSPORT, host = %host, port, "TCP connection timed out");
            }
            result
        }
        None => WasiTcpTransport::connect(host, port).await,
    }
}

async fn resolve_addresses(host: &str, port: u16) -> Result<Vec<IpSocketAddress>, TransportError> {
    if let Ok(address) = host.parse::<std::net::IpAddr>() {
        return Ok(vec![ip_to_socket_address(address, port)]);
    }

    let addresses = ip_name_lookup::resolve_addresses(host.to_string())
        .await
        .map_err(|_| TransportError::DnsResolutionFailed {
            host: host.to_string(),
        })?;
    if addresses.is_empty() {
        return Err(TransportError::DnsResolutionFailed {
            host: host.to_string(),
        });
    }

    Ok(addresses
        .into_iter()
        .map(|address| match address {
            IpAddress::Ipv4(address) => IpSocketAddress::Ipv4(Ipv4SocketAddress { address, port }),
            IpAddress::Ipv6(address) => IpSocketAddress::Ipv6(Ipv6SocketAddress {
                address,
                port,
                flow_info: 0,
                scope_id: 0,
            }),
        })
        .collect())
}

fn ip_to_socket_address(address: std::net::IpAddr, port: u16) -> IpSocketAddress {
    match address {
        std::net::IpAddr::V4(address) => {
            let [a, b, c, d] = address.octets();
            IpSocketAddress::Ipv4(Ipv4SocketAddress {
                address: (a, b, c, d),
                port,
            })
        }
        std::net::IpAddr::V6(address) => {
            let [a, b, c, d, e, f, g, h] = address.segments();
            IpSocketAddress::Ipv6(Ipv6SocketAddress {
                address: (a, b, c, d, e, f, g, h),
                port,
                flow_info: 0,
                scope_id: 0,
            })
        }
    }
}

fn address_family(address: &IpSocketAddress) -> IpAddressFamily {
    match address {
        IpSocketAddress::Ipv4(_) => IpAddressFamily::Ipv4,
        IpSocketAddress::Ipv6(_) => IpAddressFamily::Ipv6,
    }
}

fn map_socket_error(error: ErrorCode) -> TransportError {
    match error {
        ErrorCode::ConnectionRefused => TransportError::ConnectionRefused,
        ErrorCode::ConnectionReset | ErrorCode::ConnectionBroken | ErrorCode::ConnectionAborted => {
            TransportError::ConnectionReset
        }
        ErrorCode::Timeout => TransportError::Timeout,
        error => TransportError::Io(error.to_string()),
    }
}

pub(crate) fn duration_to_wasi(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}
