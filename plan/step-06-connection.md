# Step 06 - Connection Management (Async)

## Goal
Implement the async `Connection` type that ties together transport, protocol, and authentication into a usable database connection with full lifecycle management.

## Context
A PostgreSQL connection has a clear lifecycle:
1. TCP connect (+ optional TLS) - async
2. Startup message (protocol version + params) - async
3. Authentication exchange - async
4. Parameter collection → ReadyForQuery - async
5. Ready to accept queries
6. Terminate → close - async

All network I/O is async. The `Connection` is the main user-facing type.

### Connection State Machine

To prevent protocol violations (e.g., sending a query during authentication, or attempting a transaction while a stream is active), `Connection` must track its state explicitly:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    /// Initial state before any network activity.
    Disconnected,

    /// TCP/TLS handshake in progress.
    Connecting,

    /// Startup message sent, waiting for Authentication* or ReadyForQuery.
    StartingUp,

    /// Authentication challenge/response exchange in progress.
    Authenticating,

    /// ReadyForQuery received, connection is idle and can accept commands.
    Idle,

    /// A simple query is in flight (Query message sent).
    ActiveSimpleQuery,

    /// An extended query (Parse/Bind/Execute) is in flight.
    ActiveExtendedQuery,

    /// A COPY IN operation is in progress.
    CopyIn,

    /// A COPY OUT operation is in progress.
    CopyOut,

    /// A RowStream is active and borrowing the connection.
    Streaming,

    /// Connection is being closed gracefully.
    Closing,

    /// Connection is closed or unusable due to error.
    Closed,
}
```

State transitions are **monotonic** in normal operation but may jump to `Closed` on any error:

```
Disconnected → Connecting → StartingUp → Authenticating → Idle
                                                            ↓
Idle → ActiveSimpleQuery → Idle
Idle → ActiveExtendedQuery → Idle
Idle → Streaming → Idle
Idle → CopyIn → Idle
Idle → CopyOut → Idle
Idle → Closing → Closed
Any  → Closed   (on fatal error)
```

**Why a state machine matters**:

| Scenario | Without state machine | With state machine |
|----------|----------------------|-------------------|
| Double `query()` call | Corrupt protocol stream | `Err(InvalidState)` |
| `query()` during auth | Server rejects with protocol error | `Err(InvalidState)` |
| `commit()` during stream | Undefined behavior | `Err(InvalidState)` |
| Drop mid-stream | Connection left in bad state | `Drop` triggers rollback/cleanup via state |

The state is stored in `Connection.state` and checked at the entry of every public async method.

## Tasks

### 6.1 - Connection configuration
```rust
pub struct Config {
    // Connection
    pub host: String,
    pub port: u16,                        // default: 5432
    pub user: String,
    pub password: Option<String>,
    pub database: Option<String>,         // defaults to user
    pub application_name: Option<String>,

    // TLS
    pub ssl_mode: SslMode,
    pub ssl_ca_cert: Option<Vec<u8>>,
    pub ssl_client_cert: Option<Vec<u8>>,
    pub ssl_client_key: Option<Vec<u8>>,

    // Timeouts
    pub connect_timeout: Option<Duration>,
    pub statement_timeout: Option<Duration>,

    // Protocol
    pub target_session_attrs: TargetSessionAttrs,
    pub options: Vec<(String, String)>,    // extra startup params
}

pub enum TargetSessionAttrs {
    Any,
    ReadWrite,
    ReadOnly,
}
```

### 6.2 - Connection string parser
Support standard PostgreSQL connection URIs:
```
postgresql://user:password@host:port/database?sslmode=require&connect_timeout=10
```

Also support key-value format:
```
host=localhost port=5432 dbname=mydb user=myuser password=secret sslmode=require
```

```rust
impl Config {
    pub fn from_uri(uri: &str) -> Result<Config, ConfigError>;
    pub fn from_key_value(s: &str) -> Result<Config, ConfigError>;
    pub fn from_env() -> Result<Config, ConfigError>; // PGHOST, PGPORT, PGUSER, etc.

    pub fn builder() -> ConfigBuilder;
}

pub struct ConfigBuilder { /* fields */ }

impl ConfigBuilder {
    pub fn host(mut self, host: &str) -> Self;
    pub fn port(mut self, port: u16) -> Self;
    pub fn user(mut self, user: &str) -> Self;
    pub fn password(mut self, password: &str) -> Self;
    pub fn database(mut self, database: &str) -> Self;
    pub fn ssl_mode(mut self, mode: SslMode) -> Self;
    pub fn connect_timeout(mut self, timeout: Duration) -> Self;
    pub fn application_name(mut self, name: &str) -> Self;
    pub fn build(self) -> Result<Config, ConfigError>;
}
```

### 6.3 - Async connection establishment
```rust
impl Connection {
    pub async fn connect(config: &Config) -> Result<Connection, PgError> {
        // 1. Async TCP connect (with optional timeout)
        let tcp = connect_with_timeout(
            &config.host, config.port, config.connect_timeout,
        ).await?;

        // 2. Async TLS negotiation (if configured)
        let transport = if config.ssl_mode != SslMode::Disable {
            negotiate_tls(tcp, &config.tls_config()).await?
        } else {
            PgTransport::Plain(BufferedTransport::new(tcp))
        };

        // 3. Send StartupMessage
        let mut codec = Codec::new();
        codec.send(&mut transport, &FrontendMessage::StartupMessage {
            params: config.startup_params(),
        }).await?;

        // 4. Async authentication
        let server_params = authenticate(&mut transport, &mut codec, config).await?;

        // 5. Validate target_session_attrs if needed
        if config.target_session_attrs == TargetSessionAttrs::ReadWrite {
            validate_read_write(&mut transport, &mut codec).await?;
        }

        Ok(Connection {
            transport,
            codec,
            server_params,
            transaction_status: TransactionStatus::Idle,
            notification_queue: VecDeque::new(),
            statement_cache: StatementCache::new(256),
        })
    }

    /// Convenience: connect from a connection string
    pub async fn connect_str(s: &str) -> Result<Connection, PgError> {
        let config = Config::from_uri(s).or_else(|_| Config::from_key_value(s))?;
        Self::connect(&config).await
    }
}
```

### 6.4 - Connection struct
```rust
pub struct Connection {
    transport: PgTransport,
    codec: Codec,
    server_params: ServerParams,
    transaction_status: TransactionStatus,
    notification_queue: VecDeque<Notification>,
    statement_cache: StatementCache,
    config: Config,  // retained for cancel token
}
```

### 6.5 - Async Codec (protocol I/O bridge)
Bridge between the I/O-free protocol crate and the async transport:
```rust
pub struct Codec {
    encoder: MessageEncoder,
    read_buf: ReadBuffer,
}

impl Codec {
    pub async fn send(
        &mut self,
        transport: &mut impl AsyncTransport,
        msg: &FrontendMessage,
    ) -> Result<(), PgError> {
        let bytes = self.encoder.encode(msg);
        transport.write_all(bytes).await?;
        transport.flush().await?;
        Ok(())
    }

    /// Send without flushing (for pipelining multiple messages)
    pub async fn send_no_flush(
        &mut self,
        transport: &mut impl AsyncTransport,
        msg: &FrontendMessage,
    ) -> Result<(), PgError> {
        let bytes = self.encoder.encode(msg);
        transport.write_all(bytes).await?;
        Ok(())
    }

    pub async fn read_message(
        &mut self,
        transport: &mut impl AsyncTransport,
    ) -> Result<BackendMessage, PgError> {
        loop {
            if let Some(msg) = self.read_buf.next_message()? {
                return Ok(msg);
            }
            // Need more data from the network
            let mut buf = [0u8; 8192];
            let n = transport.read(&mut buf).await?;
            if n == 0 {
                return Err(PgError::ConnectionClosed);
            }
            self.read_buf.extend(&buf[..n]);
        }
    }
}
```

### 6.6 - Connection lifecycle (async)
```rust
impl Connection {
    pub fn is_closed(&self) -> bool { /* check transport state */ }
    pub fn transaction_status(&self) -> TransactionStatus { self.transaction_status }
    pub fn server_version(&self) -> &str { &self.server_params.server_version }
    pub fn server_params(&self) -> &ServerParams { &self.server_params }

    pub async fn close(mut self) -> Result<(), PgError> {
        self.codec.send(&mut self.transport, &FrontendMessage::Terminate).await?;
        self.transport.shutdown().await?;
        Ok(())
    }

    /// Internal: read messages until ReadyForQuery, discarding everything else
    /// Used after errors to resync the protocol state
    pub(crate) async fn read_until_ready(&mut self) -> Result<(), PgError> {
        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            match msg {
                BackendMessage::ReadyForQuery { transaction_status } => {
                    self.transaction_status = transaction_status;
                    return Ok(());
                }
                // Intercept async messages
                BackendMessage::NotificationResponse { process_id, channel, payload } => {
                    self.notification_queue.push_back(Notification {
                        process_id, channel, payload,
                    });
                }
                BackendMessage::ParameterStatus { name, value } => {
                    self.server_params.params.insert(name, value);
                }
                _ => {} // discard
            }
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Note: can't do async in Drop.
        // Best-effort: the transport's Drop will close the TCP socket.
        // For clean shutdown, users should call conn.close().await explicitly.
    }
}
```

### 6.7 - Environment variable support
Read standard PG environment variables via `wasi:cli/environment`:

| Variable | Mapping |
|----------|---------|
| `PGHOST` | host |
| `PGPORT` | port |
| `PGDATABASE` | database |
| `PGUSER` | user |
| `PGPASSWORD` | password |
| `PGSSLMODE` | ssl_mode |
| `PGCONNECT_TIMEOUT` | connect_timeout |
| `PGOPTIONS` | options |
| `PGAPPNAME` | application_name |

```rust
impl Config {
    pub fn from_env() -> Result<Config, ConfigError> {
        // std::env::var works on wasm32-wasip2 (delegates to wasi:cli/environment)
        let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("PGPORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5432);
        // ... etc
    }
}
```

## File Layout
```
crates/pg-client/src/
├── connection/
│   ├── mod.rs          (Connection struct + async connect)
│   ├── config.rs       (Config, ConfigBuilder, connection string parsing)
│   ├── codec.rs        (Codec - async protocol + transport bridge)
│   └── lifecycle.rs    (close, read_until_ready)
```

## Acceptance Criteria
- [ ] Can async connect to PostgreSQL using host/port/user/password
- [ ] Connection string parsing (URI and key-value formats)
- [ ] Config builder pattern works
- [ ] Environment variable fallback works
- [ ] Async TLS negotiation integrated
- [ ] Async auth integrated
- [ ] Async clean connection close (Terminate message sent)
- [ ] Drop closes TCP socket (best-effort, non-async)
- [ ] Connection validates server parameters
- [ ] Async message interception (notifications, parameter status, notices)

## Testing
- Unit test: config parsing (URI, key-value, env vars, builder)
- Async integration test: full connection lifecycle (connect → query → close)
- Test connection failure scenarios (wrong host, wrong password, timeout)
