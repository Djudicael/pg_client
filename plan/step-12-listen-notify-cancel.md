# Step 12 - LISTEN/NOTIFY & Query Cancellation (Async)

## Goal
Implement PostgreSQL's asynchronous notification system (LISTEN/NOTIFY) and the out-of-band query cancellation protocol.

## Context

All network I/O is async. Notifications are collected asynchronously as they arrive between other messages. Cancellation opens a separate async TCP connection.

### LISTEN/NOTIFY
PostgreSQL supports pub/sub-style notifications between connections:
- `LISTEN channel` - subscribe to notifications on a channel
- `NOTIFY channel, 'payload'` - send a notification
- Notifications arrive as `NotificationResponse` backend messages, which can appear **at any time** between other messages (asynchronously interleaved)

### Cancellation
PostgreSQL allows cancelling a running query from a **separate connection**:
- During connection setup, server sends `BackendKeyData` (process_id + secret_key)
- To cancel: open a new TCP connection, send `CancelRequest` message, close
- This is out-of-band: doesn't use the primary connection

## Tasks

### 12.1 - Notification handling
```rust
#[derive(Debug, Clone)]
pub struct Notification {
    pub process_id: i32,    // PID of the notifying backend
    pub channel: String,
    pub payload: String,
}

impl Connection {
    /// Start listening for notifications on a channel
    pub async fn listen(&mut self, channel: &str) -> Result<(), PgError> {
        self.execute(&format!("LISTEN {}", quote_identifier(channel))).await?;
        Ok(())
    }

    /// Stop listening on a channel
    pub async fn unlisten(&mut self, channel: &str) -> Result<(), PgError> {
        self.execute(&format!("UNLISTEN {}", quote_identifier(channel))).await?;
        Ok(())
    }

    /// Stop listening on all channels
    pub async fn unlisten_all(&mut self) -> Result<(), PgError> {
        self.execute("UNLISTEN *").await?;
        Ok(())
    }

    /// Send a notification
    pub async fn notify(&mut self, channel: &str, payload: &str) -> Result<(), PgError> {
        self.execute_params(
            "SELECT pg_notify($1, $2)",
            &[&channel, &payload],
        ).await?;
        Ok(())
    }
}
```

### 12.2 - Notification collection
Notifications can arrive interleaved with other messages. The connection must buffer them.

```rust
pub struct Connection {
    // ... existing fields ...
    notification_queue: VecDeque<Notification>,
}

impl Connection {
    /// Collect notifications that arrived during the last operation.
    /// This is sync because it just drains the in-memory queue (no I/O).
    pub fn notifications(&mut self) -> Vec<Notification> {
        self.notification_queue.drain(..).collect()
    }

    /// Wait for the next notification (async).
    /// Sends an empty query to trigger server to flush pending notifications.
    pub async fn wait_for_notification(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<Option<Notification>, PgError> {
        // Check queue first
        if let Some(n) = self.notification_queue.pop_front() {
            return Ok(Some(n));
        }

        // Send empty query to trigger ReadyForQuery cycle
        // which will deliver any pending notifications
        self.codec.send(&mut self.transport, &FrontendMessage::Query {
            sql: String::new(),
        }).await?;

        // Read messages, collecting notifications
        loop {
            // If timeout, use poll with deadline on the transport
            let msg = self.codec.read_message(&mut self.transport).await?;
            match msg {
                BackendMessage::NotificationResponse { process_id, channel, payload } => {
                    return Ok(Some(Notification { process_id, channel, payload }));
                }
                BackendMessage::ReadyForQuery { transaction_status } => {
                    self.transaction_status = transaction_status;
                    break;
                }
                BackendMessage::EmptyQueryResponse => {}
                _ => {}
            }
        }

        Ok(None)
    }

    // Internal: called by message reading loop to intercept notifications
    fn handle_async_message(&mut self, msg: BackendMessage) -> Option<BackendMessage> {
        match msg {
            BackendMessage::NotificationResponse { process_id, channel, payload } => {
                self.notification_queue.push_back(Notification {
                    process_id, channel, payload,
                });
                None // consumed
            }
            BackendMessage::NoticeResponse { .. } => {
                // Handle notices (log or collect)
                None
            }
            BackendMessage::ParameterStatus { name, value } => {
                // Update server params
                self.server_params.params.insert(name, value);
                None
            }
            other => Some(other), // pass through
        }
    }
}
```

### 12.3 - Query cancellation
```rust
pub struct CancelToken {
    pub host: String,
    pub port: u16,
    pub process_id: i32,
    pub secret_key: i32,
    pub ssl_mode: SslMode,
}

impl Connection {
    /// Get a cancellation token for this connection.
    /// The token can be sent to another thread/task to cancel a running query.
    pub fn cancel_token(&self) -> CancelToken {
        CancelToken {
            host: self.config.host.clone(),
            port: self.config.port,
            process_id: self.server_params.process_id,
            secret_key: self.server_params.secret_key,
            ssl_mode: self.config.ssl_mode,
        }
    }
}

impl CancelToken {
    /// Send a cancellation request.
    /// This opens a NEW async TCP connection, sends CancelRequest, and closes it.
    pub async fn cancel(&self) -> Result<(), PgError> {
        // 1. Open TCP connection
        let mut tcp = WasiTcpTransport::connect(
            &self.host,
            self.port,
            Some(Duration::from_secs(10)),
        ).await?;

        // 2. Send CancelRequest message
        // Format: length(i32=16) + cancel_code(i32=80877102) + process_id(i32) + secret_key(i32)
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&16i32.to_be_bytes());
        buf.extend_from_slice(&80877102i32.to_be_bytes());
        buf.extend_from_slice(&self.process_id.to_be_bytes());
        buf.extend_from_slice(&self.secret_key.to_be_bytes());
        tcp.write_all(&buf).await?;

        // 3. Close connection (server processes the cancel and closes its end)
        tcp.shutdown().await?;
        Ok(())
    }
}
```

### 12.4 - Integrate async message handling into the read loop
Update the main message reading to intercept async messages:

```rust
impl Codec {
    /// Read the next synchronous message, handling async messages internally
    pub async fn read_sync_message(
        &mut self,
        transport: &mut impl Transport,
        conn: &mut ConnectionState,
    ) -> Result<BackendMessage, PgError> {
        loop {
            let msg = self.read_raw_message(transport).await?;
            match conn.handle_async_message(msg) {
                Some(sync_msg) => return Ok(sync_msg),
                None => continue, // was async message, read next
            }
        }
    }
}
```

## File Layout
```
crates/pg-client/src/
├── notification.rs     (Notification, listen/unlisten, wait_for_notification)
├── cancel.rs           (CancelToken, cancel)
```

## Acceptance Criteria
- [ ] LISTEN/UNLISTEN via SQL works
- [ ] Notifications received after queries
- [ ] `wait_for_notification` awaits asynchronously until notification arrives
- [ ] Notifications buffered when arriving between other messages
- [ ] `notify()` sends notifications
- [ ] Cancel token can be extracted from a connection
- [ ] `cancel()` actually cancels a running query
- [ ] Cancel opens separate async TCP connection (out-of-band)
- [ ] Async messages (notifications, notices, parameter status) handled transparently
- [ ] No notifications lost during normal query operations

## Testing
- LISTEN on connection A, NOTIFY from connection B, verify receipt
- Multiple channels, verify correct routing
- Notification with payload
- Cancel a long-running query (`SELECT pg_sleep(60)`)
- Verify cancel token works from a different scope
- Notifications interleaved with query results
- UNLISTEN stops delivery
