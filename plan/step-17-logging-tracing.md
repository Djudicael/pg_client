# Step 17 - Structured Logging & Tracing

## Goal

Integrate the `tracing` crate throughout the library to provide structured, configurable observability for all internal operations. This enables users to debug connection issues, monitor query performance, and understand protocol behavior without modifying the library.

## Context

A production database client is a complex distributed system component. When things go wrong (connection drops, auth failures, slow queries, protocol errors), users need visibility into what the library is doing internally. `printf`-style debugging is insufficient because:

1. **No structure**: Raw log lines are hard to parse, filter, or aggregate.
2. **No levels**: You can't selectively enable/disable specific categories of logging.
3. **No context**: You can't correlate log lines with specific connections or queries.
4. **No timing**: You can't measure how long each operation takes.
5. **No redaction**: Sensitive data (passwords, query parameters) may be leaked.

The `tracing` crate solves all of these problems with structured spans, events, and levels. It's the de facto standard for Rust observability and works on `wasm32-wasip2` (it has no runtime dependency — it's a facade that the user's chosen subscriber drives).

### Why `tracing` over `log`?

| Feature | `log` | `tracing` |
|---------|-------|-----------|
| Structured data | No (string only) | Yes (key-value pairs) |
| Spans (scoped context) | No | Yes |
| Async-aware | No | Yes (span enters across `.await`) |
| Timing | Manual | Built-in (span duration) |
| Filtering | Level only | Level + target + fields |
| WASI compatible | Yes | Yes (no runtime dep) |
| Performance when disabled | Very low | Very low (similar to `log`) |

We use `tracing` as the primary observability layer. The `log` crate is not used directly, but `tracing` can emit `log` records via the `tracing-log` compatibility layer if users prefer.

### WASI P2 Considerations

- **No `tracing-subscriber` by default**: The `tracing` crate is just a facade. Users must install a subscriber (e.g., `tracing-subscriber`) in their application. The library only emits spans and events — it doesn't configure how they're handled.
- **No stdout by default on WASI**: WASI P2 has `wasi:cli/stdout` but it may not be connected. Users should configure their subscriber appropriately (e.g., write to stderr, a file, or a network endpoint).
- **Performance**: When no subscriber is installed, `tracing` events are no-ops (zero cost). This is important for WASI components that don't need logging.
- **Feature flag**: Tracing is behind the `tracing` feature flag (enabled by default). Users who don't want any tracing overhead can disable it.

## Tasks

### 17.1 - Feature flag and dependency

```toml
# In pg-client/Cargo.toml
[features]
default = ["tls", "scram", "tracing"]

# Structured logging via tracing crate.
# When disabled, all tracing macros become no-ops (zero overhead).
tracing = ["dep:tracing"]
```

```toml
# In workspace Cargo.toml
[workspace.dependencies]
tracing = "0.1"  # Structured logging facade (no runtime dependency)
```

### 17.2 - Tracing module structure

```rust
// pg-client/src/tracing_ext.rs
//
// Internal helpers for consistent tracing across the library.
// This module is NOT public — it provides internal macros and helpers.

/// Target prefix for all wasi-pg-client tracing events.
/// Users can filter to only our events with:
///   tracing_subscriber::filter::Targets::new().with_target("wasi_pg_client", tracing::Level::DEBUG)
pub const TARGET_PREFIX: &str = "wasi_pg_client";

/// Target for transport-layer events.
pub const TARGET_TRANSPORT: &str = "wasi_pg_client::transport";

/// Target for connection lifecycle events.
pub const TARGET_CONNECTION: &str = "wasi_pg_client::connection";

/// Target for authentication events.
pub const TARGET_AUTH: &str = "wasi_pg_client::auth";

/// Target for query execution events.
pub const TARGET_QUERY: &str = "wasi_pg_client::query";

/// Target for transaction events.
pub const TARGET_TRANSACTION: &str = "wasi_pg_client::transaction";

/// Target for COPY protocol events.
pub const TARGET_COPY: &str = "wasi_pg_client::copy";

/// Target for notification events.
pub const TARGET_NOTIFICATION: &str = "wasi_pg_client::notification";

/// Target for pool events.
pub const TARGET_POOL: &str = "wasi_pg_client::pool";

/// Target for reconnection events.
pub const TARGET_RECONNECT: &str = "wasi_pg_client::reconnect";

/// Target for wire protocol events.
pub const TARGET_PROTOCOL: &str = "wasi_pg_client::protocol";
```

### 17.3 - Transport layer tracing

```rust
// In pg-client/src/transport/tcp.rs

impl WasiTcpTransport {
    pub async fn connect(host: &str, port: u16) -> Result<Self, TransportError> {
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: TARGET_TRANSPORT,
            host = %host,
            port = port,
            "Connecting to PostgreSQL via TCP"
        );

        let start = Instant::now();
        let addr = format!("{}:{}", host, port);
        let result = TcpStream::connect(&addr).await;

        match result {
            Ok(stream) => {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    target: TARGET_TRANSPORT,
                    host = %host,
                    port = port,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "TCP connection established"
                );
                Ok(Self { stream })
            }
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: TARGET_TRANSPORT,
                    host = %host,
                    port = port,
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    error = %e,
                    "TCP connection failed"
                );
                Err(/* mapped error */)
            }
        }
    }
}

impl<T: AsyncTransport> AsyncTransport for BufferedTransport<T> {
    async fn flush(&mut self) -> Result<(), TransportError> {
        let write_buf_len = self.write_buf.len();
        if write_buf_len > 0 {
            #[cfg(feature = "tracing")]
            tracing::trace!(
                target: TARGET_TRANSPORT,
                write_buf_len = write_buf_len,
                "Flushing write buffer to transport"
            );
            // ... flush logic
        }
        Ok(())
    }
}
```

### 17.4 - Connection lifecycle spans

The most valuable tracing output is structured spans that capture the full lifecycle of a connection. Each major operation becomes a span with timing:

```rust
// In pg-client/src/connection/mod.rs

impl Connection {
    pub async fn connect(config: &Config) -> Result<Connection, PgError> {
        // Create a span for the entire connection establishment.
        // This span covers TCP connect + TLS + auth + parameter collection.
        #[cfg(feature = "tracing")]
        let span = tracing::info_span!(
            target: TARGET_CONNECTION,
            "connect",
            host = %config.host,
            port = config.port,
            database = ?config.database,
            user = %config.user,
            ssl_mode = %config.ssl_mode,
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_CONNECTION,
            "Starting connection establishment"
        );

        // 1. TCP connect
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_CONNECTION, "Step 1/4: TCP connect");
        let tcp = connect_with_timeout(&config.host, config.port, config.connect_timeout).await?;

        // 2. TLS negotiation
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: TARGET_CONNECTION,
            ssl_mode = %config.ssl_mode,
            "Step 2/4: TLS negotiation"
        );
        let transport = negotiate_tls(tcp, &config.tls_config()).await?;

        #[cfg(feature = "tracing")]
        if transport.is_tls() {
            tracing::info!(target: TARGET_CONNECTION, "TLS connection established");
        } else {
            tracing::warn!(target: TARGET_CONNECTION, "Connection is plaintext (no TLS)");
        }

        // 3. Startup message
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_CONNECTION, "Step 3/4: Sending startup message");
        // ... send startup

        // 4. Authentication
        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_CONNECTION, "Step 4/4: Authentication");
        let server_params = authenticate(&mut transport, &mut codec, config).await?;

        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_CONNECTION,
            server_version = %server_params.server_version,
            process_id = server_params.process_id,
            "Connection established successfully"
        );

        Ok(Connection { /* ... */ })
    }

    pub async fn close(mut self) -> Result<(), PgError> {
        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_CONNECTION,
            process_id = self.server_params.process_id,
            "Closing connection"
        );

        self.codec.send(&mut self.transport, &FrontendMessage::Terminate).await?;
        self.transport.shutdown().await?;

        #[cfg(feature = "tracing")]
        tracing::debug!(target: TARGET_CONNECTION, "Connection closed");
        Ok(())
    }
}
```

### 17.5 - Authentication tracing

Auth events are critical for debugging but must be careful about sensitive data:

```rust
// In pg-client/src/auth/mod.rs

pub async fn authenticate(
    transport: &mut impl AsyncTransport,
    codec: &mut Codec,
    params: &ConnectionParams,
) -> Result<ServerParams, AuthError> {
    loop {
        let msg = codec.read_message(transport).await?;
        match msg {
            BackendMessage::AuthenticationOk => {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    target: TARGET_AUTH,
                    "Authentication successful"
                );
                break;
            }
            BackendMessage::AuthenticationCleartextPassword => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    target: TARGET_AUTH,
                    method = "cleartext",
                    "Server requested cleartext password authentication"
                );
                auth_cleartext(transport, codec, params).await?;
            }
            BackendMessage::AuthenticationMD5Password { .. } => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    target: TARGET_AUTH,
                    method = "md5",
                    "Server requested MD5 password authentication"
                );
                auth_md5(transport, codec, params, &salt).await?;
            }
            BackendMessage::AuthenticationSASL { mechanisms } => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    target: TARGET_AUTH,
                    method = "scram-sha-256",
                    mechanisms = ?mechanisms,
                    "Server requested SASL authentication"
                );
                auth_sasl(transport, codec, params, &mechanisms).await?;
            }
            BackendMessage::ErrorResponse { fields } => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    target: TARGET_AUTH,
                    "Authentication failed: server returned error"
                );
                return Err(AuthError::ServerError(PgError::from_fields(fields)));
            }
            other => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    target: TARGET_AUTH,
                    message_type = ?other,
                    "Unexpected message during authentication"
                );
                return Err(AuthError::UnexpectedMessage(other));
            }
        }
    }
    // ...
}
```

**Critical: Sensitive data redaction**. Passwords, auth tokens, and SCRAM proofs must NEVER appear in tracing output:

```rust
// ❌ NEVER DO THIS:
tracing::debug!(password = %params.password, "Authenticating with password");

// ✅ DO THIS INSTEAD:
tracing::debug!("Authenticating with password (redacted)");

// ❌ NEVER DO THIS:
tracing::trace!(client_first = %client_first_bare, "SCRAM client-first");

// ✅ DO THIS INSTEAD:
tracing::trace!(
    nonce_len = nonce.len(),
    "SCRAM client-first message generated"
);
```

### 17.6 - Query execution tracing

Query tracing is the most commonly used observability feature. Users want to know:
- What SQL was executed
- How long it took
- How many rows were returned
- Whether it succeeded or failed

```rust
// In pg-client/src/query/mod.rs

impl Connection {
    pub async fn query_stream(&mut self, sql: &str) -> Result<RowStream<'_>, PgError> {
        #[cfg(feature = "tracing")]
        let span = tracing::info_span!(
            target: TARGET_QUERY,
            "query",
            // NOTE: sql is truncated to 200 chars to avoid flooding logs
            // with huge queries. Full SQL can be logged at TRACE level.
            sql_truncated = %truncate_str(sql, 200),
            protocol = "simple",
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: TARGET_QUERY,
            sql_len = sql.len(),
            "Executing simple query"
        );

        // At TRACE level, log the full SQL (users opt into this explicitly)
        #[cfg(feature = "tracing")]
        tracing::trace!(
            target: TARGET_QUERY,
            sql = %sql,
            "Full SQL text"
        );

        // ... send query
        Ok(stream)
    }

    pub async fn query_params_stream(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<RowStream<'_>, PgError> {
        #[cfg(feature = "tracing")]
        let span = tracing::info_span!(
            target: TARGET_QUERY,
            "query_params",
            sql_truncated = %truncate_str(sql, 200),
            param_count = params.len(),
            protocol = "extended",
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        // NOTE: Parameter values are NOT logged by default (may contain
        // sensitive data like emails, SSNs, etc.). At TRACE level, we log
        // parameter TYPE OIDs only, not values.
        #[cfg(feature = "tracing")]
        tracing::trace!(
            target: TARGET_QUERY,
            param_oids = ?params.iter().map(|p| p.type_oid()).collect::<Vec<_>>(),
            "Parameter type OIDs"
        );

        // ... execute
        Ok(stream)
    }
}

// In RowStream
impl<'a> RowStream<'a> {
    pub async fn next(&mut self) -> Result<Option<Row>, PgError> {
        // ... read next row
        // When stream ends, log summary
        if done {
            #[cfg(feature = "tracing")]
            tracing::info!(
                target: TARGET_QUERY,
                rows_fetched = self.rows_fetched,
                elapsed_ms = self.start_time.elapsed().as_millis() as u64,
                "Query completed"
            );
        }
    }
}
```

### 17.7 - Transaction tracing

```rust
// In pg-client/src/transaction/mod.rs

impl Connection {
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, PgError> {
        #[cfg(feature = "tracing")]
        tracing::info!(target: TARGET_TRANSACTION, "BEGIN transaction");

        self.execute("BEGIN").await?;
        Ok(Transaction { /* ... */ })
    }
}

impl<'a> Transaction<'a> {
    pub async fn commit(mut self) -> Result<(), PgError> {
        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_TRANSACTION,
            savepoint_depth = self.savepoint_depth,
            "COMMIT transaction"
        );

        self.conn.execute("COMMIT").await?;
        self.committed = true;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), PgError> {
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: TARGET_TRANSACTION,
            savepoint_depth = self.savepoint_depth,
            "ROLLBACK transaction"
        );

        self.conn.execute("ROLLBACK").await?;
        self.committed = true;
        Ok(())
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.committed {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: TARGET_TRANSACTION,
                "Transaction dropped without explicit commit/rollback. \
                 Best-effort cleanup will be attempted on next use."
            );
        }
    }
}
```

### 17.8 - Pool tracing

```rust
// In crates/pg-pool/src/pool.rs

impl Pool {
    pub async fn acquire(&self) -> Result<PoolGuard<'_>, PgError> {
        #[cfg(feature = "tracing")]
        let span = tracing::debug_span!(
            target: TARGET_POOL,
            "pool_acquire",
            max_size = self.inner.borrow().config.max_size,
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        // ... acquire logic with tracing at each step

        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: TARGET_POOL,
            source = "idle", // or "new" or "timeout"
            active = inner.active_count,
            idle = inner.idle.len(),
            "Acquired connection from pool"
        );
    }

    pub async fn release(&self, acquired: AcquiredConnection) {
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: TARGET_POOL,
            active = /* after decrement */,
            idle = /* after push */,
            "Returned connection to pool"
        );
    }
}
```

### 17.9 - Reconnection tracing

Reconnection events are critical for production debugging:

```rust
// In pg-client/src/reconnect/mod.rs

impl Connection {
    pub async fn reconnect(&mut self) -> Result<(), PgError> {
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: TARGET_RECONNECT,
            reconnect_count = self.state.reconnect_count,
            has_session_state = self.state.session_state.has_state(),
            "Attempting to reconnect broken connection"
        );

        // ... reconnection logic

        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_RECONNECT,
            reconnect_count = self.state.reconnect_count,
            "Reconnection successful"
        );

        Ok(())
    }

    pub async fn with_retry<T, F, Fut>(&mut self, f: F) -> Result<T, PgError>
    where
        F: Fn(&mut Connection) -> Fut,
        Fut: Future<Output = Result<T, PgError>>,
    {
        // ... on each retry attempt:
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: TARGET_RECONNECT,
            attempt = attempt,
            max_attempts = max_attempts,
            error_class = ?class,
            delay_ms = delay.as_millis() as u64,
            "Retrying operation after error"
        );
    }
}
```

### 17.10 - Wire protocol tracing (TRACE level)

At the TRACE level, we log individual protocol messages. This is extremely verbose but invaluable for debugging protocol issues:

```rust
// In pg-client/src/connection/codec.rs

impl Codec {
    pub async fn send(
        &mut self,
        transport: &mut impl AsyncTransport,
        msg: &FrontendMessage,
    ) -> Result<(), PgError> {
        #[cfg(feature = "tracing")]
        tracing::trace!(
            target: TARGET_PROTOCOL,
            direction = "send",
            message_type = %msg.message_type_name(),
            encoded_len = self.encoder.encode(msg).len(),
            "Sending frontend message"
        );

        let bytes = self.encoder.encode(msg);
        transport.write_all(bytes).await?;
        transport.flush().await?;
        Ok(())
    }

    pub async fn read_message(
        &mut self,
        transport: &mut impl AsyncTransport,
    ) -> Result<BackendMessage, PgError> {
        // ... read message

        #[cfg(feature = "tracing")]
        tracing::trace!(
            target: TARGET_PROTOCOL,
            direction = "recv",
            message_type = %msg.message_type_name(),
            "Received backend message"
        );

        Ok(msg)
    }
}

// Helper trait for message type names (for tracing only)
trait MessageTypeName {
    fn message_type_name(&self) -> &'static str;
}

impl MessageTypeName for FrontendMessage {
    fn message_type_name(&self) -> &'static str {
        match self {
            FrontendMessage::StartupMessage { .. } => "StartupMessage",
            FrontendMessage::SSLRequest => "SSLRequest",
            FrontendMessage::Query { .. } => "Query",
            FrontendMessage::Parse { .. } => "Parse",
            FrontendMessage::Bind { .. } => "Bind",
            FrontendMessage::Describe { .. } => "Describe",
            FrontendMessage::Execute { .. } => "Execute",
            FrontendMessage::Sync => "Sync",
            FrontendMessage::Flush => "Flush",
            FrontendMessage::Close { .. } => "Close",
            FrontendMessage::Terminate => "Terminate",
            FrontendMessage::PasswordMessage { .. } => "PasswordMessage",
            FrontendMessage::SASLInitialResponse { .. } => "SASLInitialResponse",
            FrontendMessage::SASLResponse { .. } => "SASLResponse",
            FrontendMessage::CopyData { .. } => "CopyData",
            FrontendMessage::CopyDone => "CopyDone",
            FrontendMessage::CopyFail { .. } => "CopyFail",
            FrontendMessage::CancelRequest { .. } => "CancelRequest",
        }
    }
}

impl MessageTypeName for BackendMessage {
    fn message_type_name(&self) -> &'static str {
        match self {
            BackendMessage::AuthenticationOk => "AuthenticationOk",
            BackendMessage::AuthenticationCleartextPassword => "AuthenticationCleartextPassword",
            BackendMessage::AuthenticationMD5Password { .. } => "AuthenticationMD5Password",
            BackendMessage::AuthenticationSASL { .. } => "AuthenticationSASL",
            BackendMessage::AuthenticationSASLContinue { .. } => "AuthenticationSASLContinue",
            BackendMessage::AuthenticationSASLFinal { .. } => "AuthenticationSASLFinal",
            BackendMessage::BackendKeyData { .. } => "BackendKeyData",
            BackendMessage::ParameterStatus { .. } => "ParameterStatus",
            BackendMessage::ReadyForQuery { .. } => "ReadyForQuery",
            BackendMessage::RowDescription { .. } => "RowDescription",
            BackendMessage::DataRow { .. } => "DataRow",
            BackendMessage::CommandComplete { .. } => "CommandComplete",
            BackendMessage::EmptyQueryResponse => "EmptyQueryResponse",
            BackendMessage::ParseComplete => "ParseComplete",
            BackendMessage::BindComplete => "BindComplete",
            BackendMessage::CloseComplete => "CloseComplete",
            BackendMessage::NoData => "NoData",
            BackendMessage::ParameterDescription { .. } => "ParameterDescription",
            BackendMessage::PortalSuspended => "PortalSuspended",
            BackendMessage::CopyInResponse { .. } => "CopyInResponse",
            BackendMessage::CopyOutResponse { .. } => "CopyOutResponse",
            BackendMessage::CopyData { .. } => "CopyData",
            BackendMessage::CopyDone => "CopyDone",
            BackendMessage::ErrorResponse { .. } => "ErrorResponse",
            BackendMessage::NoticeResponse { .. } => "NoticeResponse",
            BackendMessage::NotificationResponse { .. } => "NotificationResponse",
        }
    }
}
```

### 17.11 - COPY protocol tracing

```rust
// In pg-client/src/copy/mod.rs

impl Connection {
    pub async fn copy_in(&mut self, sql: &str) -> Result<CopyIn<'_>, PgError> {
        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_COPY,
            direction = "in",
            sql_truncated = %truncate_str(sql, 200),
            "Starting COPY IN operation"
        );
        // ...
    }
}

impl<'a> CopyIn<'a> {
    pub async fn write(&mut self, data: &[u8]) -> Result<(), PgError> {
        #[cfg(feature = "tracing")]
        tracing::trace!(
            target: TARGET_COPY,
            chunk_len = data.len(),
            total_bytes = self.bytes_written,
            "COPY IN: writing data chunk"
        );
        // ...
    }

    pub async fn finish(mut self) -> Result<u64, PgError> {
        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_COPY,
            total_bytes = self.bytes_written,
            chunks = self.chunks_written,
            "COPY IN: finishing"
        );
        // ...
    }
}
```

### 17.12 - Notification tracing

```rust
// In pg-client/src/notification.rs

impl Connection {
    pub async fn listen(&mut self, channel: &str) -> Result<(), PgError> {
        #[cfg(feature = "tracing")]
        tracing::info!(
            target: TARGET_NOTIFICATION,
            channel = %channel,
            "LISTEN: subscribing to channel"
        );
        // ...
    }

    pub async fn notify(&mut self, channel: &str, payload: &str) -> Result<(), PgError> {
        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: TARGET_NOTIFICATION,
            channel = %channel,
            payload_len = payload.len(),
            // NOTE: payload content is NOT logged (may be sensitive)
            "NOTIFY: sending notification"
        );
        // ...
    }
}

// When a notification is received from the server:
fn handle_notification(&mut self, process_id: i32, channel: String, payload: String) {
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: TARGET_NOTIFICATION,
        channel = %channel,
        process_id = process_id,
        payload_len = payload.len(),
        // NOTE: payload content is NOT logged at DEBUG level
        "Received notification"
    );

    // At TRACE level, log the payload (user explicitly opted into verbose logging)
    #[cfg(feature = "tracing")]
    tracing::trace!(
        target: TARGET_NOTIFICATION,
        channel = %channel,
        payload = %payload,
        "Received notification (with payload)"
    );
}
```

### 17.13 - Sensitive data redaction policy

This is a critical part of the tracing design. We must never leak sensitive data through tracing output.

```rust
// pg-client/src/tracing_ext.rs

/// Data categories and their redaction rules.
///
/// | Category | Examples | DEBUG | TRACE | Redaction |
/// |----------|----------|-------|-------|-----------|
/// | SQL text | SELECT, INSERT | Truncated (200 chars) | Full | None |
/// | SQL params | $1, $2 values | Count only | OID only | Values never logged |
/// | Passwords | auth passwords | Never | Never | Always redacted |
/// | SCRAM data | client-first, proofs | Never | Length only | Always redacted |
/// | TLS keys | client key, CA key | Never | Never | Always redacted |
/// | Connection strings | postgresql://user:pass@... | Host only | Host+port | Password redacted |
/// | Notification payload | LISTEN/NOTIFY data | Length only | Full | Depends on content |
/// | Row data | Column values | Never | Never | Always redacted |
/// | Error messages | Server errors | Full | Full | None (not sensitive) |
/// | Server params | server_version, etc. | Full | Full | None (not sensitive) |
///
/// **Rule of thumb**: If a value is user-provided or could contain PII, don't log it
/// at DEBUG or INFO level. Only log it at TRACE level with explicit documentation
/// that TRACE may expose sensitive data.

/// Truncate a string to `max_len` characters, appending "..." if truncated.
pub fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

/// Redact a connection string, replacing the password with "***".
/// Input:  "postgresql://user:secret@host:5432/db"
/// Output: "postgresql://user:***@host:5432/db"
pub fn redact_connection_string(s: &str) -> String {
    // Simple heuristic: replace text between : and @
    if let Some(start) = s.find("://") {
        let after_scheme = &s[start + 3..];
        if let Some(at_pos) = after_scheme.find('@') {
            let user_part = &after_scheme[..at_pos];
            if let Some(colon_pos) = user_part.find(':') {
                let before = &s[..start + 3 + colon_pos + 1];
                let after = &s[start + 3 + at_pos..];
                return format!("{}***{}", before, after);
            }
        }
    }
    s.to_string()
}
```

### 17.14 - Tracing level guide for users

Document the expected output at each tracing level so users can choose the right verbosity:

```
┌──────────┬──────────────────────────────────────────────────────────────┐
│ Level    │ What gets logged                                             │
├──────────┼──────────────────────────────────────────────────────────────┤
│ ERROR    │ Fatal errors only:                                           │
│          │   - Authentication failed                                    │
│          │   - Connection dropped mid-transaction                       │
│          │   - TLS handshake failed                                     │
│          │   - Reconnection failed after all attempts                   │
├──────────┼──────────────────────────────────────────────────────────────┤
│ WARN     │ Recoverable problems:                                        │
│          │   - Connection broken, attempting reconnection              │
│          │   - Transaction rolled back (explicit or Drop)              │
│          │   - TLS not supported, falling back to plaintext            │
│          │   - Pool guard dropped without async release                │
│          │   - RowStream dropped without full consumption              │
│          │   - Discarding expired/broken pool connection               │
│          │   - Session state rebuild partially failed                  │
├──────────┼──────────────────────────────────────────────────────────────┤
│ INFO     │ Normal operations (production-safe):                         │
│          │   - Connection established (host, port, server_version)     │
│          │   - Connection closed                                       │
│          │   - Query completed (truncated SQL, row count, duration)    │
│          │   - Transaction BEGIN/COMMIT/ROLLBACK                       │
│          │   - COPY started/finished (row count, bytes)                │
│          │   - Reconnection successful                                 │
│          │   - Pool created/closed                                     │
│          │   - Authentication successful (method only, no credentials) │
├──────────┼──────────────────────────────────────────────────────────────┤
│ DEBUG    │ Detailed operation info:                                     │
│          │   - TCP connect attempt (host, port)                        │
│          │   - TLS negotiation step                                     │
│          │   - Auth method requested by server                         │
│          │   - Pool acquire/release (active/idle counts)               │
│          │   - Statement prepare/close (name, SQL truncated)           │
│          │   - Cursor open/close                                       │
│          │   - Notification received (channel, payload length)         │
│          │   - Retry attempt (attempt number, delay, error class)      │
│          │   - Stale connection detected                               │
├──────────┼──────────────────────────────────────────────────────────────┤
│ TRACE    │ Wire-level protocol detail (very verbose):                   │
│          │   - Every frontend message sent (type, encoded length)      │
│          │   - Every backend message received (type)                   │
│          │   - Full SQL text (not truncated)                           │
│          │   - Parameter type OIDs                                     │
│          │   - Buffer flush operations (buffer sizes)                  │
│          │   - COPY data chunk sizes                                   │
│          │   - Notification payload content                            │
│          │                                                              │
│          │ ⚠️  TRACE may expose sensitive data. Use only in            │
│          │    development/debugging, never in production.              │
└──────────┴──────────────────────────────────────────────────────────────┘
```

### 17.15 - Conditional compilation pattern

All tracing calls use `#[cfg(feature = "tracing")]` to ensure zero overhead when the feature is disabled:

```rust
// Pattern 1: Simple event (no span)
#[cfg(feature = "tracing")]
tracing::info!(target: TARGET_CONNECTION, "Connection established");

// Pattern 2: Span with entry
#[cfg(feature = "tracing")]
let span = tracing::info_span!(target: TARGET_QUERY, "query", sql_truncated = %truncate_str(sql, 200));
#[cfg(feature = "tracing")]
let _enter = span.enter();

// When the "tracing" feature is disabled, these lines are compiled out entirely.
// The `tracing` crate itself uses a similar pattern internally — when no
// subscriber is active, the macros expand to no-ops.
```

**Alternative: always-depend on `tracing`**. Instead of the feature flag, we could always depend on `tracing` and rely on its built-in no-op behavior when no subscriber is installed. This simplifies the code (no `#[cfg]` annotations) at the cost of a always-present dependency. The overhead is negligible (the `tracing` facade is very lightweight).

**Recommendation**: For v0.1, use the feature flag approach for maximum control. If users report that the `#[cfg]` annotations are annoying or that they always enable `tracing`, we can make it non-optional in v0.2.

### 17.16 - Integration test helper

Provide a test helper that captures tracing output for assertions:

```rust
// In tests/common/mod.rs (not shipped with the library)

/// Install a test subscriber that captures tracing events for assertions.
/// Call this at the beginning of each test that needs tracing.
#[cfg(test)]
pub fn install_test_subscriber() -> TestTracing {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let (layer, guard) = tracing_capture::capture_layer();
    let subscriber = tracing_subscriber::registry().with(layer);
    subscriber.init();

    TestTracing { _guard: guard }
}

#[cfg(test)]
pub struct TestTracing {
    _guard: tracing_capture::CaptureGuard,
}

// Usage in tests:
#[test]
async fn test_connection_logs_establishment() {
    let _tracing = install_test_subscriber();

    let mut conn = Connection::connect(&test_config()).await.unwrap();

    // Assert that connection establishment was logged
    let events = tracing_capture::drain_events();
    assert!(events.iter().any(|e|
        e.target == "wasi_pg_client::connection" &&
        e.message.contains("Connection established")
    ));
}
```

### 17.17 - WASI P2 subscriber guidance

Document how users should set up a `tracing` subscriber in their WASI P2 component:

```rust
/// Example: Setting up tracing in a WASI P2 component.
///
/// Add to your Cargo.toml:
///   tracing-subscriber = "0.3"
///
/// Then in your main function:
#[wstd::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install a simple subscriber that writes to stderr.
    // stderr is available on WASI P2 via wasi:cli/stderr.
    use tracing_subscriber::{fmt, EnvFilter};

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("wasi_pg_client=info"))
        )
        .with_writer(std::io::stderr)  // Use stderr (available on WASI)
        .init();

    // Now use the library — all operations will be traced
    let mut conn = Connection::connect(&config).await?;
    // ...

    Ok(())
}
```

**Environment variable filtering**: Users can control tracing verbosity via the `RUST_LOG` environment variable (available on WASI P2 via `wasi:cli/environment`):

```bash
# Production: only info and above
wasmtime run --wasi inherit-env --env RUST_LOG=wasi_pg_client=info component.wasm

# Debugging: detailed operation info
wasmtime run --wasi inherit-env --env RUST_LOG=wasi_pg_client=debug component.wasm

# Protocol debugging: very verbose
wasmtime run --wasi inherit-env --env RUST_LOG=wasi_pg_client=trace component.wasm

# Only connection events
wasmtime run --wasi inherit-env --env RUST_LOG=wasi_pg_client::connection=debug component.wasm

# Only query events
wasmtime run --wasi inherit-env --env RUST_LOG=wasi_pg_client::query=debug component.wasm
```

## File Layout

```
crates/pg-client/src/
├── tracing_ext.rs     (internal: target constants, redaction helpers, truncate_str)
├── transport/
│   ├── tcp.rs         (tracing: TCP connect/disconnect, DNS resolution)
│   ├── tls.rs         (tracing: TLS negotiation, certificate validation)
│   └── buffered.rs    (tracing: buffer flush operations)
├── connection/
│   ├── mod.rs         (tracing: connection lifecycle spans)
│   └── codec.rs       (tracing: protocol message send/recv at TRACE level)
├── auth/
│   ├── mod.rs         (tracing: auth method selection, success/failure)
│   └── scram.rs       (tracing: SCRAM steps — NO sensitive data)
├── query/
│   ├── mod.rs         (tracing: query execution spans, row counts, timing)
│   └── stream.rs      (tracing: stream consumption, early termination)
├── transaction/
│   ├── mod.rs         (tracing: BEGIN/COMMIT/ROLLBACK, Drop warnings)
│   └── savepoint.rs   (tracing: savepoint create/release/rollback)
├── copy/
│   ├── mod.rs         (tracing: COPY start/finish, chunk sizes)
│   ├── copy_in.rs     (tracing: COPY IN data chunks)
│   └── copy_out.rs    (tracing: COPY OUT data chunks)
├── notification.rs    (tracing: LISTEN/UNLISTEN, notification received)
├── cancel.rs          (tracing: cancel request sent)
├── reconnect/
│   ├── mod.rs         (tracing: reconnection attempts, session rebuild)
│   └── retry.rs       (tracing: retry attempts, backoff delays)
└── error/
    └── mod.rs         (tracing: error classification for retry decisions)

crates/pg-pool/src/
├── pool.rs            (tracing: acquire/release, connection creation, maintenance)
└── guard.rs           (tracing: guard lifecycle, Drop warnings)
```

## Acceptance Criteria

- [ ] All tracing calls use `#[cfg(feature = "tracing")]` conditional compilation
- [ ] `tracing` feature is enabled by default, can be disabled for zero overhead
- [ ] Every major operation has a corresponding tracing event at the appropriate level
- [ ] Connection lifecycle has structured spans with timing
- [ ] Query execution logs SQL (truncated at DEBUG, full at TRACE), row count, and duration
- [ ] Authentication logs method and success/failure, NEVER passwords or tokens
- [ ] SCRAM auth logs step names and lengths, NEVER client-first/proof values
- [ ] Pool operations log active/idle counts
- [ ] Reconnection attempts and session rebuild are logged
- [ ] Transaction BEGIN/COMMIT/ROLLBACK are logged
- [ ] Drop-based cleanup emits WARN-level events
- [ ] Wire protocol messages are logged at TRACE level only
- [ ] Sensitive data is never logged at INFO/DEBUG/WARN/ERROR levels
- [ ] TRACE level documentation warns about potential sensitive data exposure
- [ ] `truncate_str()` helper truncates long SQL strings
- [ ] `redact_connection_string()` helper redacts passwords in URIs
- [ ] Target names are namespaced under `wasi_pg_client::*`
- [ ] Tracing level guide is documented for users
- [ ] WASI P2 subscriber setup is documented with examples
- [ ] `RUST_LOG` environment variable filtering works on WASI P2
- [ ] No performance impact when `tracing` feature is disabled
- [ ] No performance impact when `tracing` feature is enabled but no subscriber is installed
- [ ] Compiles for `wasm32-wasip2`

## Key Design Decisions

1. **`tracing` over `log`**: Structured spans and key-value pairs provide much better observability than flat log lines. The `tracing` crate is the Rust ecosystem standard and has no runtime dependency.

2. **Feature flag for zero-overhead opt-out**: When the `tracing` feature is disabled, all tracing calls are compiled out. This guarantees zero overhead for users who don't need observability.

3. **Sensitive data redaction by default**: Passwords, auth tokens, SCRAM proofs, query parameter values, and row data are NEVER logged at INFO/DEBUG/WARN/ERROR levels. Only at TRACE level is some potentially sensitive data exposed, and this is clearly documented.

4. **Target namespacing**: All targets are under `wasi_pg_client::*`, allowing users to filter to just our library's events. Sub-targets (transport, connection, query, etc.) allow fine-grained filtering.

5. **SQL truncation at DEBUG level**: SQL queries can be very long (e.g., bulk INSERT with thousands of rows). At DEBUG level, we truncate to 200 characters. At TRACE level, the full SQL is logged.

6. **Span-based timing**: Connection establishment and query execution use `tracing::info_span!` which automatically tracks duration. Users can see how long each operation takes without any manual timing code.

7. **No subscriber bundled**: The library only emits events. It does not configure how they're handled. Users choose their own subscriber (fmt, json, etc.) and filtering level.

## Testing

- **Unit test**: `truncate_str()` truncates correctly at boundary lengths
- **Unit test**: `redact_connection_string()` redacts passwords in various URI formats
- **Unit test**: Target constants are correct
- **Integration test**: Connection establishment emits INFO-level event
- **Integration test**: Query execution emits span with SQL, row count, duration
- **Integration test**: Authentication emits method name but NOT password
- **Integration test**: Transaction BEGIN/COMMIT/ROLLBACK are logged
- **Integration test**: Pool acquire/release logs active/idle counts
- **Integration test**: Reconnection attempt is logged at WARN level
- **Integration test**: Drop-based cleanup emits WARN-level event
- **Integration test**: TRACE-level protocol messages include message types
- **Integration test**: No tracing output when feature is disabled
- **Integration test**: `RUST_LOG` filtering works on WASI P2
- **Security test**: Search all tracing calls for accidental sensitive data logging (password, secret, token, key, proof in field names)
</arg_value>
