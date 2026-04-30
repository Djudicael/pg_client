# Step 02 - Async TCP Transport Layer

## Goal
Implement the async TCP transport using raw `wasip2::sockets::tcp` bindings (wrapped in `wstd::io` async streams), providing a clean async abstraction that the PostgreSQL protocol layer will use. Includes buffered I/O, timeout support, DNS resolution, and a native fallback for testing.

## Context
`wstd` 0.5.x provides async I/O primitives (`AsyncInputStream`, `AsyncOutputStream`) and a TCP listener, but **does not expose `TcpStream::connect`** for client-side connections. We use raw `wasip2::sockets::tcp` bindings (re-exported by `wstd::wasip2`) to:
- `create_tcp_socket(family)` — create a socket
- `socket.start_connect(&network, addr)` — begin async TCP connect
- `socket.finish_connect()` — complete connect, obtain `(InputStream, OutputStream)`
- Wrap raw streams in `wstd::io::AsyncInputStream` / `AsyncOutputStream` for async read/write
- DNS resolution is not handled by `wstd` for client connect; use `std::net::ToSocketAddrs` (WASI P2 compatible) or direct IP addresses

Since WASI P2 is single-threaded, no `Send + Sync` bounds are needed on our async traits.

**Important: `async fn` in traits — generic dispatch only**. We use `async fn` in the `AsyncTransport` trait, which means we can only use generic dispatch (`impl AsyncTransport`), never dynamic dispatch (`dyn AsyncTransport`). This is a deliberate choice — it avoids `Pin<Box<dyn Future>>` overhead and works on WASI. Mock transports for testing are passed as generic parameters.

## Tasks

### 2.1 - Define the async Transport trait

```rust
use std::future::Future;

/// Async transport abstraction for PostgreSQL wire protocol I/O.
///
/// This trait uses `async fn` which means only generic dispatch is supported
/// (no `dyn AsyncTransport`). Use generic parameters in all functions that
/// need a transport:
///
/// ```rust
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
```

No `Send` bound needed on the returned futures since WASI P2 is single-threaded. We can use `async fn` in traits directly (Rust 1.75+).

This trait abstracts I/O so we can:
- Swap in TLS transparently (Step 03)
- Mock for testing (generic parameter, not dyn)
- Use native blocking I/O for integration tests (behind `test-native` feature)
- Potentially migrate to different WASI versions later

### 2.2 - Implement wstd-based TCP transport

**WASI P2 API surface for client TCP**:
- `wasip2::sockets::tcp_create_socket::create_tcp_socket(family)` — create socket
- `wasip2::sockets::instance_network::instance_network()` — get default network
- `socket.start_connect(&network, addr)` — begin connect
- `socket.subscribe()` → `Pollable` — wait for connect completion
- `socket.finish_connect()` → `(InputStream, OutputStream)` — complete connect
- `wstd::io::AsyncInputStream::new(input)` — async read wrapper
- `wstd::io::AsyncOutputStream::new(output)` — async write wrapper
- `wstd::runtime::AsyncPollable::new(pollable).wait_for().await` — async wait

> **Note**: `wstd`'s exact API may vary between versions. We pin `wstd = "0.5"` and wrap the raw WASI socket operations in our `WasiTcpTransport`. If `wstd` gains `TcpStream::connect` in a future version, only this file needs to change — the `AsyncTransport` trait insulates the rest of the codebase.

```rust
use wstd::io::{AsyncInputStream, AsyncOutputStream, AsyncRead, AsyncWrite};
use wstd::runtime::{AsyncPollable, WaitFor};
use wstd::wasip2::sockets::{
    instance_network::instance_network,
    network::{Ipv4SocketAddress, Ipv6SocketAddress},
    tcp::{IpAddressFamily, IpSocketAddress, TcpSocket},
    tcp_create_socket::create_tcp_socket,
};

pub struct WasiTcpTransport {
    input: AsyncInputStream,
    output: AsyncOutputStream,
    socket: TcpSocket,
}

impl WasiTcpTransport {
    pub async fn connect(host: &str, port: u16) -> Result<Self, TransportError> {
        let addr = format!("{}:{}", host, port);
        let std_addr: std::net::SocketAddr = addr.parse()
            .map_err(|_| TransportError::InvalidConfig("invalid address".into()))?;

        let family = match std_addr {
            std::net::SocketAddr::V4(_) => IpAddressFamily::Ipv4,
            std::net::SocketAddr::V6(_) => IpAddressFamily::Ipv6,
        };

        let socket = create_tcp_socket(family)
            .map_err(|e| TransportError::Io(format!("{:?}", e)))?;
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

        socket.start_connect(&network, wasi_addr)
            .map_err(|e| TransportError::Io(format!("{:?}", e)))?;
        AsyncPollable::new(socket.subscribe()).wait_for().await;

        let (input, output) = socket.finish_connect()
            .map_err(|e| TransportError::Io(format!("{:?}", e)))?;

        Ok(Self {
            input: AsyncInputStream::new(input),
            output: AsyncOutputStream::new(output),
            socket,
        })
    }
}

impl AsyncTransport for WasiTcpTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        self.input.read(buf).await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        self.output.write(buf).await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.output.write_all(buf).await
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
        self.output.flush().await
            .map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        self.socket.shutdown(wasip2::sockets::tcp::ShutdownType::Both)
            .map_err(|e| TransportError::Io(format!("{:?}", e)))?;
        Ok(())
    }
}
```

### 2.3 - Connection parameters

```rust
pub struct ConnectionParams {
    pub host: String,
    pub port: u16,          // default 5432
    pub connect_timeout: Option<Duration>,
}

impl ConnectionParams {
    pub fn validate(&self) -> Result<(), TransportError> {
        if self.host.is_empty() {
            return Err(TransportError::InvalidConfig("host is empty".into()));
        }
        if self.port == 0 {
            return Err(TransportError::InvalidConfig("port cannot be 0".into()));
        }
        Ok(())
    }
}
```

### 2.4 - Timeout support

Use `futures-concurrency` race patterns for timeouts. **Important**: the race pattern requires both futures to be polled; when one wins, the other is dropped. This means a timeout future that uses `sleep` is safe — dropping a `sleep` future just cancels the timer.

```rust
use futures_concurrency::future::Race;

/// Connect with an optional timeout.
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
                wstd::task::sleep(duration).await;
                Err(TransportError::Timeout)
            };
            // Race: first one to complete wins; the other is dropped.
            // If connect wins first, the sleep is cancelled.
            // If timeout wins first, the in-progress connect is dropped (socket closed).
            (connect_fut, timeout_fut).race().await
        }
        None => WasiTcpTransport::connect(host, port).await,
    }
}
```

**Why not `tokio::time::timeout`?** Because we don't depend on `tokio`. The `futures-concurrency` `Race` combinator is runtime-agnostic and works with `wstd`'s async model.

**Alternative: manual poll-based timeout**. If `futures-concurrency` causes issues on WASI, we can implement timeout manually using `wasi:io/poll`:

```rust
/// Fallback timeout implementation using wasi:io/poll directly.
/// This is more verbose but avoids any dependency on futures-concurrency for timeouts.
pub async fn connect_with_timeout_poll(
    host: &str,
    port: u16,
    timeout: Option<Duration>,
) -> Result<WasiTcpTransport, TransportError> {
    // Implementation would use wasi:io/poll::poll with a deadline
    // to wait on both the connect future and the timeout simultaneously.
    // This is the low-level WASI approach; prefer the Race combinator above.
    todo!("implement if futures-concurrency Race doesn't work on WASI P2")
}
```

### 2.5 - Buffered async I/O wrapper

The original `BufferedTransport` had several bugs. This is the corrected version:

**Bugs fixed**:
1. `read_exact` was not implemented — now properly reads until buffer is full
2. EOF handling: `read` returning 0 now correctly propagates as 0 (EOF), not as an error
3. `write` now auto-flushes when the write buffer exceeds a threshold (prevents unbounded memory growth)
4. `read` properly handles the case where the buffer is partially consumed
5. `compact()` is called automatically to avoid unbounded read buffer growth

```rust
/// Default write buffer threshold before auto-flush (8 KiB).
const WRITE_BUFFER_FLUSH_THRESHOLD: usize = 8192;

/// Default read buffer capacity (8 KiB).
const DEFAULT_READ_CAPACITY: usize = 8192;

/// Default write buffer capacity (8 KiB).
const DEFAULT_WRITE_CAPACITY: usize = 8192;

pub struct BufferedTransport<T: AsyncTransport> {
    inner: T,

    // Read buffer: data read from inner but not yet consumed by the caller.
    // Valid data is in read_buf[read_pos..read_len).
    read_buf: Vec<u8>,
    read_pos: usize,
    read_len: usize,

    // Write buffer: data written by the caller but not yet flushed to inner.
    write_buf: Vec<u8>,
}

impl<T: AsyncTransport> BufferedTransport<T> {
    pub fn new(inner: T) -> Self {
        Self::with_capacity(inner, DEFAULT_READ_CAPACITY, DEFAULT_WRITE_CAPACITY)
    }

    pub fn with_capacity(inner: T, read_cap: usize, write_cap: usize) -> Self {
        Self {
            inner,
            read_buf: vec![0; read_cap],
            read_pos: 0,
            read_len: 0,
            write_buf: Vec::with_capacity(write_cap),
        }
    }

    /// Compact the read buffer: move unconsumed bytes to the front.
    /// Called automatically when the read buffer is exhausted or fragmented.
    fn compact_read(&mut self) {
        if self.read_pos > 0 {
            self.read_buf.copy_within(self.read_pos..self.read_len, 0);
            self.read_len -= self.read_pos;
            self.read_pos = 0;
        }
    }

    /// Grow the read buffer if it's too small for the next read.
    fn ensure_read_capacity(&mut self, min_remaining: usize) {
        let remaining = self.read_buf.len() - self.read_len;
        if remaining < min_remaining {
            let new_len = (self.read_buf.len() * 2).max(self.read_len + min_remaining);
            self.read_buf.resize(new_len, 0);
        }
    }

    /// Flush the write buffer to the inner transport.
    async fn flush_write(&mut self) -> Result<(), TransportError> {
        if !self.write_buf.is_empty() {
            self.inner.write_all(&self.write_buf).await?;
            self.write_buf.clear();
        }
        Ok(())
    }
}

impl<T: AsyncTransport> AsyncTransport for BufferedTransport<T> {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        // If there's buffered data, return it immediately (no I/O)
        if self.read_pos < self.read_len {
            let available = &self.read_buf[self.read_pos..self.read_len];
            let n = std::cmp::min(buf.len(), available.len());
            buf[..n].copy_from_slice(&available[..n]);
            self.read_pos += n;

            // Compact if we've consumed more than half the buffer
            if self.read_pos > self.read_buf.len() / 2 {
                self.compact_read();
            }
            return Ok(n);
        }

        // Buffer is empty — refill from inner transport
        self.compact_read();
        self.ensure_read_capacity(DEFAULT_READ_CAPACITY);

        let n = self.inner.read(&mut self.read_buf[self.read_len..]).await?;
        if n == 0 {
            // EOF: connection closed
            return Ok(0);
        }
        self.read_len += n;

        // Copy from read buffer to caller's buffer
        let available = &self.read_buf[self.read_pos..self.read_len];
        let to_copy = std::cmp::min(buf.len(), available.len());
        buf[..to_copy].copy_from_slice(&available[..to_copy]);
        self.read_pos += to_copy;

        Ok(to_copy)
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        // Buffer the write data
        self.write_buf.extend_from_slice(buf);

        // Auto-flush if the write buffer exceeds the threshold.
        // This prevents unbounded memory growth if the caller writes
        // large amounts of data without calling flush().
        if self.write_buf.len() >= WRITE_BUFFER_FLUSH_THRESHOLD {
            self.flush_write().await?;
        }

        Ok(buf.len())
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.write_buf.extend_from_slice(buf);

        if self.write_buf.len() >= WRITE_BUFFER_FLUSH_THRESHOLD {
            self.flush_write().await?;
        }

        Ok(())
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        let mut filled = 0;
        while filled < buf.len() {
            // Try to consume from the read buffer first
            if self.read_pos < self.read_len {
                let available = &self.read_buf[self.read_pos..self.read_len];
                let needed = buf.len() - filled;
                let to_copy = std::cmp::min(needed, available.len());
                buf[filled..filled + to_copy].copy_from_slice(&available[..to_copy]);
                self.read_pos += to_copy;
                filled += to_copy;
                continue;
            }

            // Read buffer exhausted — refill from inner transport
            self.compact_read();
            self.ensure_read_capacity(buf.len() - filled);

            let n = self.inner.read(&mut self.read_buf[self.read_len..]).await?;
            if n == 0 {
                return Err(TransportError::UnexpectedEof);
            }
            self.read_len += n;
        }

        // Compact if fragmented
        if self.read_pos > self.read_buf.len() / 2 {
            self.compact_read();
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        // Flush our write buffer first, then the inner transport
        self.flush_write().await?;
        self.inner.flush().await
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        // Flush any pending writes before shutting down
        self.flush_write().await?;
        self.inner.shutdown().await
    }
}
```

**Key design decisions for `BufferedTransport`**:
- **Auto-flush threshold**: When the write buffer exceeds 8 KiB, we flush automatically. This prevents unbounded memory growth while still allowing small messages (like individual protocol messages) to be batched.
- **Read buffer compaction**: When more than half the read buffer has been consumed, we compact it. This prevents the buffer from growing indefinitely when reading small amounts at a time.
- **`read_exact` uses the read buffer**: We don't bypass the buffer for `read_exact`. This ensures that data already buffered is consumed first, and any leftover data from a refill is available for the next read.
- **EOF propagation**: `read` returning 0 means EOF. `read_exact` returns `UnexpectedEof` if EOF is hit before the buffer is full. This matches `std::io::Read::read_exact` semantics.

### 2.6 - DNS resolution

`wstd::net::TcpStream::connect` handles DNS resolution internally. However, we should document the behavior and provide a fallback:

```rust
/// DNS resolution is handled by wstd::net::TcpStream::connect internally.
/// It uses wasi:sockets/ip-name-lookup under the hood.
///
/// If DNS resolution fails, the error is mapped to TransportError::DnsResolutionFailed.
/// This is distinguished from a connection refused error.
impl WasiTcpTransport {
    pub async fn connect(host: &str, port: u16) -> Result<Self, TransportError> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr).await.map_err(|e| {
            let msg = e.to_string();
            // Heuristic: distinguish DNS errors from connection errors
            if msg.contains("resolve") || msg.contains("name") || msg.contains("dns") || msg.contains("NXDOMAIN") {
                TransportError::DnsResolutionFailed { host: host.to_string() }
            } else if msg.contains("refused") {
                TransportError::ConnectionRefused
            } else if msg.contains("timed out") {
                TransportError::Timeout
            } else {
                TransportError::Io(msg)
            }
        })?;
        Ok(Self { stream })
    }
}
```

> **Note**: The error classification above is heuristic-based because `wstd` (and the underlying WASI socket errors) may not provide structured error types. If `wstd` improves its error reporting, we can make this more precise.

### 2.7 - Native transport for testing (behind `test-native` feature)

For integration tests that run natively (not via WASI), we provide a blocking I/O transport. This is **only** for testing — it uses blocking I/O inside `async fn` bodies, which works because test executors poll futures to completion synchronously.

```rust
// pg-client/src/transport/native.rs
// Only compiled when the "test-native" feature is enabled.

#[cfg(feature = "test-native")]
pub struct NativeTcpTransport {
    stream: std::net::TcpStream,
}

#[cfg(feature = "test-native")]
impl NativeTcpTransport {
    pub fn connect(host: &str, port: u16) -> Result<Self, TransportError> {
        let addr = format!("{}:{}", host, port);
        let stream = std::net::TcpStream::connect(&addr)
            .map_err(|e| TransportError::Io(e.to_string()))?;
        Ok(Self { stream })
    }

    pub fn connect_with_timeout(
        host: &str,
        port: u16,
        timeout: Option<Duration>,
    ) -> Result<Self, TransportError> {
        let addr = format!("{}:{}", host, port);
        let stream = match timeout {
            Some(dur) => std::net::TcpStream::connect_timeout(
                &addr.parse().map_err(|e| TransportError::Io(e.to_string()))?,
                dur,
            ),
            None => std::net::TcpStream::connect(&addr),
        }.map_err(|e| TransportError::Io(e.to_string()))?;
        Ok(Self { stream })
    }
}

#[cfg(feature = "test-native")]
impl AsyncTransport for NativeTcpTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        use std::io::Read;
        self.stream.read(buf).map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        use std::io::Write;
        self.stream.write(buf).map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        use std::io::Write;
        self.stream.write_all(buf).map_err(|e| TransportError::Io(e.to_string()))
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
        self.stream.flush().map_err(|e| TransportError::Io(e.to_string()))
    }

    async fn shutdown(&mut self) -> Result<(), TransportError> {
        self.stream.shutdown(std::net::Shutdown::Both)
            .map_err(|e| TransportError::Io(e.to_string()))
    }
}
```

**Why blocking I/O inside `async fn` works for tests**: The `async fn` body compiles to a state machine. When the future is polled, the blocking I/O runs synchronously within the poll call. The future never yields (it completes in one poll), so there's no deadlock risk. This is fine for sequential test execution but would be catastrophic in a real async runtime (it would block the executor). That's why this is behind a feature flag and only used in tests.

### 2.8 - Transport error type

```rust
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("connection refused")]
    ConnectionRefused,

    #[error("connection reset by peer")]
    ConnectionReset,

    #[error("operation timed out")]
    Timeout,

    #[error("DNS resolution failed for host: {host}")]
    DnsResolutionFailed { host: String },

    #[error("TLS handshake failed: {0}")]
    TlsHandshake(String),

    #[error("TLS not supported by server")]
    TlsNotSupported,

    #[error("unexpected end of stream")]
    UnexpectedEof,

    #[error("I/O error: {0}")]
    Io(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

impl TransportError {
    /// Returns true if this error indicates the connection is broken
    /// and cannot be recovered (e.g., EOF, connection reset).
    pub fn is_connection_broken(&self) -> bool {
        matches!(
            self,
            TransportError::ConnectionReset
            | TransportError::UnexpectedEof
            | TransportError::ConnectionRefused
        )
    }

    /// Returns true if this error is potentially transient
    /// (e.g., timeout, DNS failure).
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            TransportError::Timeout
            | TransportError::DnsResolutionFailed { .. }
        )
    }
}
```

### 2.9 - Fallback: raw WASI sockets

If `wstd::net` proves insufficient (API bugs, missing features, version incompatibility), we can use `wasi:sockets/tcp` directly with `wasi:io/poll`. This is the fallback implementation:

```rust
// pg-client/src/transport/raw_wasi.rs
// Only used if wstd is unavailable or broken.

use wasi::sockets::tcp::{TcpSocket, InputStream, OutputStream};
use wasi::io::poll::poll;

pub struct RawWasiTransport {
    input: InputStream,
    output: OutputStream,
    socket: TcpSocket,
}

impl RawWasiTransport {
    pub async fn connect(host: &str, port: u16) -> Result<Self, TransportError> {
        // 1. Resolve hostname via wasi:sockets/ip-name-lookup
        // 2. Create TcpSocket via wasi:sockets/tcp/create-socket
        // 3. Start connect via socket.start_connect()
        // 4. Poll until connect is ready via wasi:io/poll
        // 5. Finish connect via socket.finish_connect()
        // 6. Get input/output streams via socket.subscribe() + streams
        todo!("implement only if wstd proves insufficient")
    }
}

// AsyncTransport impl would use wasi:io/poll to drive async reads/writes.
```

> **When to use this fallback**: Only if `wstd` has a blocking bug that prevents TCP connections from working. The `wstd` wrapper is preferred because it handles the complex WASI socket state machine (create → start-connect → poll → finish-connect → subscribe) internally. Reimplementing this is error-prone.

## File Layout
```
crates/pg-client/src/
├── transport/
│   ├── mod.rs          (AsyncTransport trait + re-exports)
│   ├── tcp.rs          (WasiTcpTransport using wstd::net::TcpStream)
│   ├── buffered.rs     (BufferedTransport — fixed version)
│   ├── native.rs       (NativeTcpTransport — test-native feature only)
│   ├── raw_wasi.rs     (RawWasiTransport — fallback, not compiled by default)
│   ├── error.rs        (TransportError)
│   └── params.rs       (ConnectionParams)
```

## Acceptance Criteria
- [ ] Can establish async TCP connection to a PostgreSQL server from a WASI component
- [ ] DNS resolution works for hostnames (via wstd)
- [ ] Async read/write operations work correctly
- [ ] `read_exact` properly handles partial reads and EOF
- [ ] `BufferedTransport` auto-flushes when write buffer exceeds threshold
- [ ] `BufferedTransport` read buffer compacts automatically to prevent unbounded growth
- [ ] Timeouts enforced via async race (connect with timeout works)
- [ ] Clean async shutdown of connections
- [ ] `AsyncTransport` trait allows generic mocking in tests (no dyn needed)
- [ ] `NativeTcpTransport` compiles behind `test-native` feature
- [ ] Transport errors are classified (broken vs transient)
- [ ] Connection parameters are validated before use
- [ ] Compiles for `wasm32-wasip2`

## Key Risks and Mitigations

| Risk | Severity | Mitigation |
|------|----------|------------|
| `wstd` API changes between versions | Medium | Pin `wstd = "0.5"`. All wstd usage is isolated to `tcp.rs`. Fallback: `raw_wasi.rs`. |
| `wstd` TCP connect doesn't handle DNS | Medium | Test DNS resolution early. If broken, use `wasi:sockets/ip-name-lookup` directly. |
| `futures-concurrency` Race doesn't work on WASI | Low | Fallback: implement manual poll-based timeout using `wasi:io/poll`. |
| BufferedTransport performance | Low | Auto-flush threshold prevents unbounded buffering. Users can call `flush()` explicitly for batching. |
| Native test transport blocks executor | Low | Behind `test-native` feature flag. Only used in tests with a trivial executor. Documented as test-only. |

## Testing
- **Unit tests**: Test `BufferedTransport` with a mock inner transport that returns partial reads/writes
- **Unit tests**: Test `read_exact` with various partial-read scenarios
- **Unit tests**: Test auto-flush threshold behavior
- **Unit tests**: Test read buffer compaction
- **Unit tests**: Test `TransportError` classification (`is_connection_broken`, `is_transient`)
- **Integration test**: Async connect to a local PostgreSQL, verify TCP handshake bytes
- **Integration test**: Connect with timeout (verify timeout fires on unreachable host)
- **Integration test**: Native transport connects to PostgreSQL (behind `test-native` feature)
