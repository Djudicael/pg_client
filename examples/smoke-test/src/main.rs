//! Minimal WASI 0.3 smoke test built with Rust's `wasm32-wasip2` target.
//!
//! This checks randomness, the native P3 monotonic clock, and a P3 TCP
//! round-trip. It is intentionally lower-level than `wasi-pg-client`.

#[cfg(target_os = "wasi")]
wasip3::cli::command::export!(SmokeTest);

#[cfg(target_os = "wasi")]
struct SmokeTest;

#[cfg(target_os = "wasi")]
impl wasip3::exports::cli::run::Guest for SmokeTest {
    async fn run() -> Result<(), ()> {
        smoke_test().await.map_err(|error| {
            eprintln!("Smoke test failed: {error}");
        })
    }
}

#[cfg(target_os = "wasi")]
async fn smoke_test() -> Result<(), String> {
    let mut rand_buf = [0u8; 4];
    getrandom::fill(&mut rand_buf).map_err(|error| error.to_string())?;
    eprintln!("Random bytes: {rand_buf:02x?}");

    wasip3::clocks::monotonic_clock::wait_for(10_000_000).await;
    eprintln!("WASI 0.3 async sleep: OK");

    match try_tcp_connect("example.com", 80).await {
        Ok(()) => eprintln!("WASI 0.3 TCP connect + HTTP round-trip: OK"),
        Err(error) => eprintln!(
            "TCP connect failed: {error} (check Wasmtime DNS/TCP permissions and host networking)"
        ),
    }

    eprintln!("Smoke test completed.");
    Ok(())
}

#[cfg(target_os = "wasi")]
async fn try_tcp_connect(host: &str, port: u16) -> Result<(), String> {
    use wasip3::sockets::{
        ip_name_lookup,
        types::{
            IpAddress, IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress,
            TcpSocket,
        },
    };
    use wasip3::wit_bindgen::rt::async_support::StreamResult;

    let addresses = ip_name_lookup::resolve_addresses(host.to_string())
        .await
        .map_err(|error| format!("DNS resolution failed: {error}"))?;

    let mut last_error = "DNS returned no addresses".to_string();
    for address in addresses {
        let address = match address {
            IpAddress::Ipv4(address) => IpSocketAddress::Ipv4(Ipv4SocketAddress { address, port }),
            IpAddress::Ipv6(address) => IpSocketAddress::Ipv6(Ipv6SocketAddress {
                address,
                port,
                flow_info: 0,
                scope_id: 0,
            }),
        };
        let family = match address {
            IpSocketAddress::Ipv4(_) => IpAddressFamily::Ipv4,
            IpSocketAddress::Ipv6(_) => IpAddressFamily::Ipv6,
        };
        let socket = TcpSocket::create(family).map_err(|error| error.to_string())?;

        if let Err(error) = socket.connect(address).await {
            last_error = error.to_string();
            continue;
        }

        let (mut input, input_done) = socket.receive();
        let (mut output, outgoing) = wasip3::wit_stream::new::<u8>();
        let output_done = socket.send(outgoing);
        let request = b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n".to_vec();
        let remaining = output.write_all(request).await;
        if !remaining.is_empty() {
            return Err("send stream closed before writing the request".to_string());
        }
        drop(output);
        output_done.await.map_err(|error| error.to_string())?;

        loop {
            let (status, data) = input.read(Vec::with_capacity(64)).await;
            if !data.is_empty() {
                eprintln!("  received {} bytes", data.len());
                return Ok(());
            }
            match status {
                StreamResult::Complete(_) => continue,
                StreamResult::Dropped => {
                    input_done.await.map_err(|error| error.to_string())?;
                    return Err("EOF before any response data".to_string());
                }
                StreamResult::Cancelled => {
                    return Err("receive operation was cancelled".to_string());
                }
            }
        }
    }

    Err(last_error)
}
