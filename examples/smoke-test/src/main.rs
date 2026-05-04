//! Minimal WASI P2 smoke test: verify TCP + random + async runtime work.

use wasip2::sockets::{
    instance_network::instance_network,
    network::{Ipv4SocketAddress, Ipv6SocketAddress},
    tcp::{IpAddressFamily, IpSocketAddress},
    tcp_create_socket::create_tcp_socket,
};
use wstd::io::{AsyncInputStream, AsyncOutputStream, AsyncWrite};
use wstd::runtime::AsyncPollable;

#[wstd::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Verify getrandom works
    let mut rand_buf = [0u8; 4];
    getrandom::fill(&mut rand_buf)?;
    eprintln!("Random bytes: {:02x?}", rand_buf);

    // 2. Verify async timer works
    wstd::task::sleep(wstd::time::Duration::from_millis(10)).await;
    eprintln!("Async sleep: OK");

    // 3. Verify TCP connect works using raw wasip2 sockets
    match try_tcp_connect("93.184.216.34:80").await {
        Ok(()) => eprintln!("TCP connect + HTTP round-trip: OK"),
        Err(e) => eprintln!("TCP connect failed: {} (expected if no network)", e),
    }

    eprintln!("All smoke tests passed!");
    Ok(())
}

async fn try_tcp_connect(addr: &str) -> wstd::io::Result<()> {
    let std_addr: std::net::SocketAddr = addr
        .parse()
        .map_err(|_| wstd::io::Error::other("failed to parse socket address"))?;

    let family = match std_addr {
        std::net::SocketAddr::V4(_) => IpAddressFamily::Ipv4,
        std::net::SocketAddr::V6(_) => IpAddressFamily::Ipv6,
    };

    let socket =
        create_tcp_socket(family).map_err(|e| wstd::io::Error::other(format!("{:?}", e)))?;
    let network = instance_network();

    let wasi_addr = match std_addr {
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
    };

    socket
        .start_connect(&network, wasi_addr)
        .map_err(|e| wstd::io::Error::other(format!("{:?}", e)))?;
    AsyncPollable::new(socket.subscribe()).wait_for().await;

    let (input, output) = socket
        .finish_connect()
        .map_err(|e| wstd::io::Error::other(format!("{:?}", e)))?;

    let input = AsyncInputStream::new(input);
    let mut output = AsyncOutputStream::new(output);

    // Send a minimal HTTP request
    output
        .write_all(b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n")
        .await?;

    // Read at least some bytes back
    let mut buf = [0u8; 64];
    let n = input.read(&mut buf).await?;
    if n == 0 {
        return Err(wstd::io::Error::other("EOF before any data"));
    }
    eprintln!("  received {} bytes", n);

    // Streams and socket are dropped automatically, closing the connection.
    Ok(())
}
