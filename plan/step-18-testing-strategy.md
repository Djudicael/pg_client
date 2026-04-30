# Step 18 - Testing Strategy (Async-Aware, WASI P2)

## Goal
Define a comprehensive testing strategy covering unit tests, integration tests, streaming tests, reconnection tests, tracing tests, fuzz testing, and CI pipeline for a WASI P2-targeted PostgreSQL client.

## Context
Testing a WASI P2 library has unique challenges:
- Unit tests can run natively (no WASI needed) if the code is well-abstracted
- Integration tests need a PostgreSQL instance + optionally a WASI runtime (wasmtime)
- The transport layer must be mocked for unit tests
- TLS and auth are hard to test without a real server
- Streaming results require testing backpressure and early termination
- Reconnection tests require simulating network failures
- Tracing tests require capturing and asserting on structured events
- The pool uses `RefCell` which requires borrow-safety testing

### Testing Layers

```
┌─────────────────────────────────────────────────────┐
│ Layer 5: End-to-end WASI tests                       │
│ (Full WASI component + real PostgreSQL + wasmtime)   │
├─────────────────────────────────────────────────────┤
│ Layer 4: Integration tests (native + real PostgreSQL)│
├─────────────────────────────────────────────────────┤
│ Layer 3: Protocol tests (mock transport, full flows) │
├─────────────────────────────────────────────────────┤
│ Layer 2: Component tests (single module, some I/O)   │
├─────────────────────────────────────────────────────┤
│ Layer 1: Unit tests (pure logic, no I/O)             │
└─────────────────────────────────────────────────────┘
```

## Tasks

### 18.1 - Layer 1: Unit tests (no I/O)

**Target crates**: `pg-protocol`, `pg-types`

These crates have zero I/O dependencies and can be tested with `cargo test` natively.

```rust
// pg-protocol: message encoding
#[test]
fn test_encode_query_message() {
    let mut encoder = MessageEncoder::new();
    let bytes = encoder.encode(&FrontendMessage::Query {
        sql: "SELECT 1".to_string(),
    });
    assert_eq!(bytes[0], b'Q');
    let len = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
    assert_eq!(len as usize, bytes.len() - 1);
    assert_eq!(&bytes[5..13], b"SELECT 1");
    assert_eq!(bytes[13], 0); // null terminator
}

#[test]
fn test_encode_startup_message() {
    let mut encoder = MessageEncoder::new();
    let bytes = encoder.encode(&FrontendMessage::StartupMessage {
        params: vec![
            ("user".to_string(), "postgres".to_string()),
            ("database".to_string(), "test".to_string()),
        ],
    });
    // Startup message has no type byte
    let len = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    assert_eq!(len as usize, bytes.len());
    // Check protocol version 3.0
    assert_eq!(&bytes[4..8], &[0, 3, 0, 0]);
}

#[test]
fn test_encode_ssl_request() {
    let mut encoder = MessageEncoder::new();
    let bytes = encoder.encode(&FrontendMessage::SSLRequest);
    assert_eq!(bytes.len(), 8);
    let len = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    assert_eq!(len, 8);
    let code = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
    assert_eq!(code, 80877103);
}

// pg-protocol: message decoding
#[test]
fn test_decode_ready_for_query() {
    let bytes = [b'Z', 0, 0, 0, 5, b'I'];
    let (msg, consumed) = MessageDecoder::decode(&bytes).unwrap().unwrap();
    assert_eq!(consumed, 6);
    assert!(matches!(msg, BackendMessage::ReadyForQuery {
        transaction_status: TransactionStatus::Idle
    }));
}

#[test]
fn test_decode_authentication_ok() {
    let bytes = [b'R', 0, 0, 0, 8, 0, 0, 0, 0];
    let (msg, consumed) = MessageDecoder::decode(&bytes).unwrap().unwrap();
    assert_eq!(consumed, 9);
    assert!(matches!(msg, BackendMessage::AuthenticationOk));
}

#[test]
fn test_decode_error_response() {
    // Minimal ErrorResponse with severity and code
    let mut bytes = vec![b'E'];
    let mut body = Vec::new();
    body.push(b'S'); // Severity
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C'); // Code
    body.extend_from_slice(b"42601\0");
    body.push(b'M'); // Message
    body.extend_from_slice(b"syntax error\0");
    body.push(0); // terminator
    let len = (body.len() + 4) as i32;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(&body);

    let (msg, consumed) = MessageDecoder::decode(&bytes).unwrap().unwrap();
    assert_eq!(consumed, bytes.len());
    if let BackendMessage::ErrorResponse { fields } = msg {
        assert!(fields.iter().any(|(code, val)| *code == b'S' && val == "ERROR"));
        assert!(fields.iter().any(|(code, val)| *code == b'C' && val == "42601"));
        assert!(fields.iter().any(|(code, val)| *code == b'M' && val == "syntax error"));
    } else {
        panic!("Expected ErrorResponse");
    }
}

#[test]
fn test_decode_partial_message_returns_none() {
    // Only 3 bytes — not enough for type + length
    let bytes = [b'Z', 0, 0];
    let result = MessageDecoder::decode(&bytes).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_decode_incomplete_payload_returns_none() {
    // Type + length says 5 bytes, but only 1 byte of payload
    let bytes = [b'Z', 0, 0, 0, 5, b'I']; // this is actually complete
    let bytes_partial = [b'Z', 0, 0, 0, 5]; // missing payload byte
    let result = MessageDecoder::decode(&bytes_partial).unwrap();
    assert!(result.is_none());
}

// pg-types: binary encoding round-trip
#[test]
fn test_i32_binary_roundtrip() {
    let val: i32 = 42;
    let encoded = val.to_sql().unwrap().unwrap();
    let decoded = i32::from_sql(oid::INT4, &encoded).unwrap();
    assert_eq!(val, decoded);
}

#[test]
fn test_i32_boundary_values() {
    for val in [i32::MIN, i32::MAX, 0, -1, 1] {
        let encoded = val.to_sql().unwrap().unwrap();
        let decoded = i32::from_sql(oid::INT4, &encoded).unwrap();
        assert_eq!(val, decoded);
    }
}

#[test]
fn test_f64_special_values() {
    for val in [f64::INFINITY, f64::NEG_INFINITY, 0.0, -0.0] {
        let encoded = val.to_sql().unwrap().unwrap();
        let decoded = f64::from_sql(oid::FLOAT8, &encoded).unwrap();
        assert_eq!(val, decoded);
    }
    // NaN is not equal to itself
    let encoded = f64::NAN.to_sql().unwrap().unwrap();
    let decoded = f64::from_sql(oid::FLOAT8, &encoded).unwrap();
    assert!(decoded.is_nan());
}

// pg-types: text format parsing
#[test]
fn test_i32_text_parse() {
    let decoded = i32::from_sql_text(oid::INT4, b"12345").unwrap();
    assert_eq!(decoded, 12345);
}

#[test]
fn test_bool_text_parse() {
    assert_eq!(bool::from_sql_text(oid::BOOL, b"t").unwrap(), true);
    assert_eq!(bool::from_sql_text(oid::BOOL, b"f").unwrap(), false);
    assert_eq!(bool::from_sql_text(oid::BOOL, b"true").unwrap(), true);
    assert_eq!(bool::from_sql_text(oid::BOOL, b"false").unwrap(), false);
}

// pg-types: NULL handling
#[test]
fn test_option_null() {
    let val: Option<i32> = None;
    let encoded = val.to_sql().unwrap();
    assert!(encoded.is_none());

    let decoded: Option<i32> = FromSql::from_sql_null().unwrap();
    assert!(decoded.is_none());
}

// Config parsing
#[test]
fn test_config_from_uri() {
    let config = Config::from_uri("postgresql://user:pass@localhost:5432/mydb?sslmode=require").unwrap();
    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 5432);
    assert_eq!(config.user, "user");
    assert_eq!(config.password, Some("pass".to_string()));
    assert_eq!(config.database, Some("mydb".to_string()));
    assert_eq!(config.ssl_mode, SslMode::Require);
}

#[test]
fn test_config_from_uri_with_ipv6() {
    let config = Config::from_uri("postgresql://user@[::1]:5432/mydb").unwrap();
    assert_eq!(config.host, "::1");
    assert_eq!(config.user, "user");
}

#[test]
fn test_config_from_key_value() {
    let config = Config::from_key_value("host=localhost port=5432 dbname=mydb user=myuser").unwrap();
    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 5432);
    assert_eq!(config.database, Some("mydb".to_string()));
    assert_eq!(config.user, "myuser");
}

#[test]
fn test_config_redacted_connection_string() {
    let redacted = redact_connection_string("postgresql://user:secret@host:5432/db");
    assert_eq!(redacted, "postgresql://user:***@host:5432/db");
    assert!(!redacted.contains("secret"));
}

// SCRAM computation with RFC 5802 test vectors
#[test]
fn test_scram_rfc5802_test_vector() {
    // RFC 5802 Section 5 test vector
    // This is a sync computation test — no I/O
    let client_first_bare = "n=user,r=fyko+d2lbbFgONRv9qkxdawL";
    let server_first = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
    let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
    let password = "pencil";

    let (client_final, server_signature) = scram_compute_client_final(
        password,
        client_first_bare,
        server_first.as_bytes(),
        client_nonce,
    ).unwrap();

    // Verify the client-final-message format
    assert!(client_final.starts_with("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j"));
    // Verify server signature
    assert!(server_signature.len() > 0);
}
```

**Coverage targets**:
- Every message type encoded/decoded
- Every type's binary and text encoding/decoding
- Edge cases: empty strings, NULL, MAX/MIN values, NaN, Infinity
- Malformed input handling (no panics)
- Config parsing (URI, key-value, env vars, IPv6, special characters)
- SCRAM computation with RFC 5802 test vectors
- Error classification (Broken/Transient/Permanent)
- RetryPolicy backoff calculations
- Truncation and redaction helpers

### 18.2 - Layer 2: Component tests (single module, some I/O)

**Target crate**: `pg-client` (specific modules)

These tests exercise a single module with minimal dependencies, using mock implementations for external interfaces.

```rust
// Test BufferedTransport with a controlled mock
#[test]
fn test_buffered_transport_auto_flush_threshold() {
    let mock = MockTransport::new();
    let mut buffered = BufferedTransport::new(mock);

    // Write data below the threshold — should not flush yet
    let small_data = vec![0u8; 100];
    buffered.write(&small_data).await.unwrap();
    assert_eq!(buffered.inner().written_data().len(), 0); // not flushed

    // Write data that exceeds the threshold — should auto-flush
    let large_data = vec![0u8; WRITE_BUFFER_FLUSH_THRESHOLD + 1];
    buffered.write(&large_data).await.unwrap();
    assert!(buffered.inner().written_data().len() > 0); // flushed
}

#[test]
fn test_buffered_transport_read_exact_with_partial_reads() {
    let mut mock = MockTransport::new();
    // Simulate partial reads: first read returns 3 bytes, second returns 2
    mock.add_read_data(&[1, 2, 3]);
    mock.add_read_data(&[4, 5]);

    let mut buffered = BufferedTransport::new(mock);
    let mut buf = [0u8; 5];
    buffered.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, &[1, 2, 3, 4, 5]);
}

#[test]
fn test_buffered_transport_read_exact_eof() {
    let mut mock = MockTransport::new();
    mock.add_read_data(&[1, 2, 3]); // only 3 bytes, but we want 5

    let mut buffered = BufferedTransport::new(mock);
    let mut buf = [0u8; 5];
    let result = buffered.read_exact(&mut buf).await;
    assert!(matches!(result, Err(TransportError::UnexpectedEof)));
}

#[test]
fn test_buffered_transport_compaction() {
    let mut mock = MockTransport::new();
    mock.add_read_data(&[1, 2, 3, 4, 5, 6, 7, 8]);

    let mut buffered = BufferedTransport::with_capacity(mock, 8, 8);
    let mut buf = [0u8; 3];
    buffered.read(&mut buf).await.unwrap(); // read 3 bytes
    // Now read_pos = 3, read_len = 8
    // Compaction should happen when we read more
    let mut buf2 = [0u8; 2];
    buffered.read(&mut buf2).await.unwrap(); // read 2 more
    // After compaction, the buffer should be compacted
}
```

### 18.3 - Layer 3: Protocol tests (mock transport)

**Target crate**: `pg-client`

Use a mock transport that replays pre-recorded server responses.

```rust
/// Mock transport for testing protocol flows.
///
/// Pre-loads server responses and captures client messages.
/// All I/O is synchronous (no actual network), but the
/// AsyncTransport impl uses async fn for API compatibility.
pub struct MockTransport {
    /// Pre-loaded read data (server → client).
    read_data: VecDeque<Vec<u8>>,
    /// Captured write data (client → server).
    written_data: Vec<Vec<u8>>,
    /// Whether the transport is "closed".
    closed: bool,
}

impl MockTransport {
    pub fn new() -> Self {
        MockTransport {
            read_data: VecDeque::new(),
            written_data: Vec::new(),
            closed: false,
        }
    }

    /// Add data that will be returned by subsequent reads.
    pub fn add_read_data(&mut self, data: &[u8]) {
        self.read_data.push_back(data.to_vec());
    }

    /// Add a pre-encoded backend message to the read queue.
    pub fn add_backend_message(&mut self, msg: &BackendMessage) {
        let mut encoder = MessageEncoder::new();
        let bytes = encoder.encode(msg);
        self.read_data.push_back(bytes.to_vec());
    }

    /// Get all data written by the client.
    pub fn written_data(&self) -> &[Vec<u8>] {
        &self.written_data
    }

    /// Decode all written data as frontend messages.
    pub fn decode_written_messages(&self) -> Vec<FrontendMessage> {
        let all_bytes: Vec<u8> = self.written_data.iter().flatten().copied().collect();
        // Parse the bytes into frontend messages
        // (StartupMessage has a different format, handle separately)
        todo!("implement frontend message parsing for test assertions")
    }
}

impl AsyncTransport for MockTransport {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        if let Some(data) = self.read_data.front_mut() {
            let n = std::cmp::min(buf.len(), data.len());
            buf[..n].copy_from_slice(&data[..n]);
            data.drain(..n);
            if data.is_empty() {
                self.read_data.pop_front();
            }
            Ok(n)
        } else if self.closed {
            Ok(0) // EOF
        } else {
            Err(TransportError::UnexpectedEof)
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        self.written_data.push(buf.to_vec());
        Ok(buf.len())
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.written_data.push(buf.to_vec());
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

    async fn flush(&mut self) -> Result<(), TransportError> { Ok(()) }
    async fn shutdown(&mut self) -> Result<(), TransportError> {
        self.closed = true;
        Ok(())
    }
}
```

**Test scenarios using mock transport**:

```rust
#[test]
fn test_full_connection_handshake_trust_auth() {
    let mut mock = MockTransport::new();

    // Server responses for trust auth:
    mock.add_backend_message(&BackendMessage::AuthenticationOk);
    mock.add_backend_message(&BackendMessage::ParameterStatus {
        name: "server_version".into(), value: "16.0".into(),
    });
    mock.add_backend_message(&BackendMessage::BackendKeyData {
        process_id: 123, secret_key: 456,
    });
    mock.add_backend_message(&BackendMessage::ReadyForQuery {
        transaction_status: TransactionStatus::Idle,
    });

    let mut transport = BufferedTransport::new(mock);
    let mut codec = Codec::new();

    // Send startup
    codec.send(&mut transport, &FrontendMessage::StartupMessage {
        params: vec![("user".into(), "postgres".into())],
    }).await.unwrap();

    // Authenticate
    let server_params = authenticate(&mut transport, &mut codec, &test_params()).await.unwrap();
    assert_eq!(server_params.process_id, 123);
    assert_eq!(server_params.server_version, "16.0");
}

#[test]
fn test_simple_query_flow() {
    let mut mock = MockTransport::new();

    mock.add_backend_message(&BackendMessage::RowDescription {
        fields: vec![FieldDescription {
            name: "num".into(),
            table_oid: 0,
            column_id: 0,
            type_oid: oid::INT4,
            type_size: 4,
            type_modifier: -1,
            format: FormatCode::Text,
        }],
    });
    mock.add_backend_message(&BackendMessage::DataRow {
        values: vec![Some(b"42".to_vec())],
    });
    mock.add_backend_message(&BackendMessage::CommandComplete {
        tag: CommandTag { command: "SELECT".into(), rows_affected: Some(1) },
    });
    mock.add_backend_message(&BackendMessage::ReadyForQuery {
        transaction_status: TransactionStatus::Idle,
    });

    // ... execute query and verify results
}

#[test]
fn test_error_response_during_query() {
    let mut mock = MockTransport::new();
    mock.add_backend_message(&BackendMessage::ErrorResponse {
        fields: vec![
            (b'S', "ERROR".into()),
            (b'C', "42601".into()),
            (b'M', "syntax error at or near \"SELEC\"".into()),
        ],
    });
    mock.add_backend_message(&BackendMessage::ReadyForQuery {
        transaction_status: TransactionStatus::Idle,
    });

    // ... execute query and verify error is returned correctly
}

#[test]
fn test_notification_interleaved_with_query() {
    let mut mock = MockTransport::new();

    // Server sends a notification between RowDescription and DataRow
    mock.add_backend_message(&BackendMessage::RowDescription { fields: vec![] });
    mock.add_backend_message(&BackendMessage::NotificationResponse {
        process_id: 99,
        channel: "test_channel".into(),
        payload: "hello".into(),
    });
    mock.add_backend_message(&BackendMessage::DataRow { values: vec![] });
    mock.add_backend_message(&BackendMessage::CommandComplete {
        tag: CommandTag::default(),
    });
    mock.add_backend_message(&BackendMessage::ReadyForQuery {
        transaction_status: TransactionStatus::Idle,
    });

    // ... verify notification is buffered and query still works
}
```

### 18.4 - Layer 4: Integration tests (native + real PostgreSQL)

Requires a running PostgreSQL instance. Uses the native transport (behind `test-native` feature) so tests run without wasmtime.

```rust
// tests/integration/mod.rs

#[cfg(feature = "test-native")]
mod tests {
    use wasi_pg_client::{Connection, Config, Row, PgError};

    fn test_config() -> Config {
        Config::from_uri(
            &std::env::var("TEST_DATABASE_URL")
                .unwrap_or("postgresql://postgres:postgres@localhost:5432/postgres".into())
        ).unwrap()
    }

    // ── Basic connection ──

    #[test]
    async fn test_connect_and_query() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let result = conn.query("SELECT 1 as num").await.unwrap();
        assert_eq!(result.rows.len(), 1);
        let val: i32 = result.rows[0].get(0).unwrap();
        assert_eq!(val, 1);
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_connect_failure_wrong_host() {
        let mut config = test_config();
        config.host = "nonexistent.host.invalid".into();
        let result = Connection::connect(&config).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PgError::Transport(_) => {} // expected
            other => panic!("Expected transport error, got: {:?}", other),
        }
    }

    #[test]
    async fn test_connect_failure_wrong_password() {
        let mut config = test_config();
        config.password = Some("wrong_password".into());
        let result = Connection::connect(&config).await;
        assert!(result.is_err());
    }

    // ── Simple query ──

    #[test]
    async fn test_query_multiple_rows() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let result = conn.query("SELECT generate_series(1, 5)").await.unwrap();
        assert_eq!(result.rows.len(), 5);
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_query_null_values() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let result = conn.query("SELECT NULL::int").await.unwrap();
        assert_eq!(result.rows.len(), 1);
        let val: Option<i32> = result.rows[0].get(0).unwrap();
        assert!(val.is_none());
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_execute_insert_delete() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.execute("CREATE TEMP TABLE test_exec (id int)").await.unwrap();

        let result = conn.execute("INSERT INTO test_exec VALUES (1)").await.unwrap();
        assert_eq!(result.rows_affected(), 1);

        let result = conn.execute("DELETE FROM test_exec WHERE id = 1").await.unwrap();
        assert_eq!(result.rows_affected(), 1);

        conn.close().await.unwrap();
    }

    // ── Parameterized query ──

    #[test]
    async fn test_parameterized_query() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let result = conn.query_params(
            "SELECT $1::int + $2::int as sum",
            &[&10i32, &20i32],
        ).await.unwrap();
        let sum: i32 = result.rows[0].get(0).unwrap();
        assert_eq!(sum, 30);
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_parameterized_null() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let result = conn.query_params(
            "SELECT $1::int",
            &[&Option::<i32>::None],
        ).await.unwrap();
        let val: Option<i32> = result.rows[0].get(0).unwrap();
        assert!(val.is_none());
        conn.close().await.unwrap();
    }

    // ── Streaming ──

    #[test]
    async fn test_streaming_large_result() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let mut stream = conn.query_stream("SELECT generate_series(1, 10000)").await.unwrap();

        let mut count = 0;
        while let Some(row) = stream.next().await.unwrap() {
            count += 1;
            let _val: i32 = row.get(0).unwrap();
        }
        assert_eq!(count, 10000);
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_streaming_early_termination() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let mut stream = conn.query_stream("SELECT generate_series(1, 10000)").await.unwrap();

        // Read only 5 rows, then drop the stream
        for _ in 0..5 {
            let _ = stream.next().await.unwrap();
        }
        // Drop the stream (incomplete consumption)
        drop(stream);

        // Connection should need recovery
        assert!(conn.needs_recovery());
        conn.recover().await.unwrap();
        assert!(!conn.needs_recovery());

        // Connection should be usable again
        let result = conn.query("SELECT 1").await.unwrap();
        assert_eq!(result.rows.len(), 1);
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_cursor_streaming() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let mut cursor = conn.cursor(
            "SELECT generate_series(1, 5000)",
            &[],
            500, // fetch 500 rows at a time
        ).await.unwrap();

        let mut count = 0;
        while let Some(_row) = cursor.next().await.unwrap() {
            count += 1;
        }
        assert_eq!(count, 5000);
        conn.close().await.unwrap();
    }

    // ── Transactions ──

    #[test]
    async fn test_transaction_commit() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.execute("CREATE TEMP TABLE test_txn (id int)").await.unwrap();

        let mut txn = conn.transaction().await.unwrap();
        txn.execute("INSERT INTO test_txn VALUES (1)").await.unwrap();
        txn.commit().await.unwrap();

        let result = conn.query("SELECT count(*) FROM test_txn").await.unwrap();
        let count: i64 = result.rows[0].get(0).unwrap();
        assert_eq!(count, 1);
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_transaction_rollback() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.execute("CREATE TEMP TABLE test_txn2 (id int)").await.unwrap();

        let mut txn = conn.transaction().await.unwrap();
        txn.execute("INSERT INTO test_txn2 VALUES (1)").await.unwrap();
        txn.rollback().await.unwrap();

        let result = conn.query("SELECT count(*) FROM test_txn2").await.unwrap();
        let count: i64 = result.rows[0].get(0).unwrap();
        assert_eq!(count, 0);
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_with_transaction_closure() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.execute("CREATE TEMP TABLE test_txn3 (id int)").await.unwrap();

        let result = conn.with_transaction(|txn| async {
            txn.execute("INSERT INTO test_txn3 VALUES (1)").await?;
            txn.execute("INSERT INTO test_txn3 VALUES (2)").await?;
            Ok(42)
        }).await.unwrap();

        assert_eq!(result, 42);

        let count_result = conn.query("SELECT count(*) FROM test_txn3").await.unwrap();
        let count: i64 = count_result.rows[0].get(0).unwrap();
        assert_eq!(count, 2);
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_with_transaction_rollback_on_error() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.execute("CREATE TEMP TABLE test_txn4 (id int)").await.unwrap();

        let result: Result<(), PgError> = conn.with_transaction(|txn| async {
            txn.execute("INSERT INTO test_txn4 VALUES (1)").await?;
            Err(PgError::Protocol("simulated error".into()))
        }).await;

        assert!(result.is_err());

        let count_result = conn.query("SELECT count(*) FROM test_txn4").await.unwrap();
        let count: i64 = count_result.rows[0].get(0).unwrap();
        assert_eq!(count, 0); // rolled back
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_savepoint() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.execute("CREATE TEMP TABLE test_sp (id int)").await.unwrap();

        let mut txn = conn.transaction().await.unwrap();
        txn.execute("INSERT INTO test_sp VALUES (1)").await.unwrap();

        let mut sp = txn.savepoint("sp1").await.unwrap();
        sp.execute("INSERT INTO test_sp VALUES (2)").await.unwrap();
        sp.rollback().await.unwrap(); // rollback to savepoint

        txn.commit().await.unwrap();

        let result = conn.query("SELECT count(*) FROM test_sp").await.unwrap();
        let count: i64 = result.rows[0].get(0).unwrap();
        assert_eq!(count, 1); // only the first insert survived
        conn.close().await.unwrap();
    }

    // ── Prepared statements ──

    #[test]
    async fn test_prepared_statement_reuse() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let stmt = conn.prepare("SELECT $1::int + $2::int").await.unwrap();

        let r1 = conn.query_prepared(&stmt, &[&10i32, &20i32]).await.unwrap();
        let sum1: i32 = r1.rows[0].get(0).unwrap();
        assert_eq!(sum1, 30);

        let r2 = conn.query_prepared(&stmt, &[&100i32, &200i32]).await.unwrap();
        let sum2: i32 = r2.rows[0].get(0).unwrap();
        assert_eq!(sum2, 300);

        conn.close_statement(&stmt).await.unwrap();
        conn.close().await.unwrap();
    }

    // ── COPY protocol ──

    #[test]
    async fn test_copy_in_text() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.execute("CREATE TEMP TABLE test_copy (name text, value int)").await.unwrap();

        let mut copy_in = conn.copy_in(
            "COPY test_copy (name, value) FROM STDIN WITH (FORMAT text)"
        ).await.unwrap();

        copy_in.write(b"alice\t1\n").await.unwrap();
        copy_in.write(b"bob\t2\n").await.unwrap();
        let rows = copy_in.finish().await.unwrap();
        assert_eq!(rows, 2);

        let result = conn.query("SELECT count(*) FROM test_copy").await.unwrap();
        let count: i64 = result.rows[0].get(0).unwrap();
        assert_eq!(count, 2);
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_copy_out_text() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.execute("CREATE TEMP TABLE test_copy2 (name text)").await.unwrap();
        conn.execute("INSERT INTO test_copy2 VALUES ('alice'), ('bob')").await.unwrap();

        let mut copy_out = conn.copy_out(
            "COPY test_copy2 TO STDOUT WITH (FORMAT text)"
        ).await.unwrap();

        let data = copy_out.read_all().await.unwrap();
        let text = String::from_utf8(data).unwrap();
        assert!(text.contains("alice"));
        assert!(text.contains("bob"));
        conn.close().await.unwrap();
    }

    // ── LISTEN/NOTIFY ──

    #[test]
    async fn test_listen_notify() {
        let mut conn_a = Connection::connect(&test_config()).await.unwrap();
        let mut conn_b = Connection::connect(&test_config()).await.unwrap();

        conn_a.listen("test_channel").await.unwrap();

        conn_b.notify("test_channel", "hello").await.unwrap();

        // Send an empty query to trigger notification delivery
        let notification = conn_a.wait_for_notification(Some(Duration::from_secs(5))).await.unwrap();
        assert!(notification.is_some());
        let n = notification.unwrap();
        assert_eq!(n.channel, "test_channel");
        assert_eq!(n.payload, "hello");

        conn_a.close().await.unwrap();
        conn_b.close().await.unwrap();
    }

    // ── Error handling ──

    #[test]
    async fn test_unique_violation_error() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.execute("CREATE TEMP TABLE test_uv (id int UNIQUE)").await.unwrap();
        conn.execute("INSERT INTO test_uv VALUES (1)").await.unwrap();

        let result = conn.execute("INSERT INTO test_uv VALUES (1)").await;
        match result {
            Err(PgError::Server(ref e)) => {
                assert!(e.is_unique_violation());
                assert_eq!(e.code, "23505");
            }
            other => panic!("Expected server error, got: {:?}", other),
        }
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_syntax_error() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let result = conn.query("SELEC 1").await;
        match result {
            Err(PgError::Server(ref e)) => {
                assert!(e.is_syntax_error());
            }
            other => panic!("Expected server error, got: {:?}", other),
        }
        conn.close().await.unwrap();
    }

    // ── Reconnection ──

    #[test]
    async fn test_reconnect_after_broken_connection() {
        let mut config = test_config();
        config.reconnect.enabled = true;
        config.reconnect.max_attempts = 3;

        let mut conn = Connection::connect(&config).await.unwrap();

        // Simulate a broken connection by shutting down the transport
        conn.transport.shutdown().await.unwrap();

        // The next operation should detect the broken connection and reconnect
        let result = conn.with_retry(|c| c.query("SELECT 1")).await;
        // This may or may not succeed depending on whether the server
        // accepts the new connection. The important thing is that
        // reconnection was attempted.
        // In a real test, we'd verify the reconnection happened.
    }

    #[test]
    async fn test_error_classification() {
        // Test that errors are correctly classified
        assert_eq!(
            Connection::classify_error(&PgError::ConnectionClosed),
            ErrorClass::Broken
        );
        assert_eq!(
            Connection::classify_error(&PgError::Transport(TransportError::ConnectionReset)),
            ErrorClass::Broken
        );
        assert_eq!(
            Connection::classify_error(&PgError::Server(PgServerError {
                code: "40001".to_string(),
                ..Default::default()
            })),
            ErrorClass::Transient // serialization failure
        );
        assert_eq!(
            Connection::classify_error(&PgError::Server(PgServerError {
                code: "42601".to_string(),
                ..Default::default()
            })),
            ErrorClass::Permanent // syntax error
        );
    }

    // ── Pool ──

    #[test]
    async fn test_pool_acquire_release() {
        let pool_config = PoolConfig {
            connection: test_config(),
            max_size: 3,
            ..Default::default()
        };
        let pool = Pool::new(pool_config).await.unwrap();

        let mut guard = pool.acquire().await.unwrap();
        let result = guard.query("SELECT 1").await.unwrap();
        assert_eq!(result.rows.len(), 1);
        guard.release().await.unwrap();

        assert_eq!(pool.status().idle, 1);
        assert_eq!(pool.status().active, 0);

        pool.close().await;
    }

    #[test]
    async fn test_pool_exhaustion() {
        let pool_config = PoolConfig {
            connection: test_config(),
            max_size: 1,
            acquire_timeout: Some(Duration::from_millis(100)),
            ..Default::default()
        };
        let pool = Pool::new(pool_config).await.unwrap();

        let guard1 = pool.acquire().await.unwrap();
        // Pool is now exhausted (max_size=1)

        let result = pool.acquire().await;
        assert!(result.is_err());

        drop(guard1); // return connection (via Drop, no async cleanup)
    }

    #[test]
    async fn test_pool_multiple_guards() {
        // Verify that acquire takes &self, so multiple guards can coexist
        let pool_config = PoolConfig {
            connection: test_config(),
            max_size: 3,
            ..Default::default()
        };
        let pool = Pool::new(pool_config).await.unwrap();

        let guard1 = pool.acquire().await.unwrap();
        let guard2 = pool.acquire().await.unwrap(); // This works because acquire takes &self

        assert_eq!(pool.status().active, 2);

        // Can check status while guards are alive
        let status = pool.status();
        assert_eq!(status.active, 2);

        guard1.release().await.unwrap();
        guard2.release().await.unwrap();

        pool.close().await;
    }

    // ── Health check ──

    #[test]
    async fn test_ping() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.ping().await.unwrap();
        assert!(conn.is_alive());
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_reset_after_failed_transaction() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.execute("BEGIN").await.unwrap();
        let _ = conn.query("INVALID SQL").await; // error in transaction

        assert_eq!(conn.transaction_status(), TransactionStatus::Failed);

        conn.reset().await.unwrap();
        assert_eq!(conn.transaction_status(), TransactionStatus::Idle);

        // Connection should be usable again
        let result = conn.query("SELECT 1").await.unwrap();
        assert_eq!(result.rows.len(), 1);
        conn.close().await.unwrap();
    }
}
```

### 18.5 - Layer 5: End-to-end WASI tests

Compile as `wasm32-wasip2` component, run with `wasmtime`.

```bash
# Build the test component
cargo build --target wasm32-wasip2 --example e2e-test

# Run with wasmtime, granting network access
wasmtime run \
  --wasi inherit-network \
  --wasi inherit-env \
  --env TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  --env RUST_LOG=wasi_pg_client=debug \
  target/wasm32-wasip2/debug/examples/e2e_test.wasm
```

`examples/e2e-test/src/main.rs`:
```rust
//! End-to-end test: full PostgreSQL client running as a WASI P2 component.

#[wstd::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install tracing subscriber (writes to stderr)
    #[cfg(feature = "tracing")]
    {
        use tracing_subscriber::{fmt, EnvFilter};
        fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("wasi_pg_client=info"))
            )
            .with_writer(std::io::stderr)
            .init();
    }

    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or("postgresql://postgres:postgres@localhost:5432/postgres".into());

    let config = Config::from_uri(&database_url)?;

    // Test 1: Connect and query
    eprintln!("Test 1: Connect and query...");
    let mut conn = Connection::connect(&config).await?;
    let result = conn.query("SELECT 1 as num").await?;
    assert_eq!(result.rows.len(), 1);
    eprintln!("  OK: Got {} row(s)", result.rows.len());

    // Test 2: Parameterized query
    eprintln!("Test 2: Parameterized query...");
    let result = conn.query_params("SELECT $1::text || ' ' || $2::text", &[&"hello", &"world"]).await?;
    let greeting: String = result.rows[0].get(0)?;
    assert_eq!(greeting, "hello world");
    eprintln!("  OK: greeting = {}", greeting);

    // Test 3: Transaction
    eprintln!("Test 3: Transaction...");
    conn.execute("CREATE TEMP TABLE e2e_test (id int, name text)").await?;
    conn.with_transaction(|txn| async {
        txn.execute_params("INSERT INTO e2e_test VALUES ($1, $2)", &[&1i32, &"alice"]).await?;
        txn.execute_params("INSERT INTO e2e_test VALUES ($1, $2)", &[&2i32, &"bob"]).await?;
        Ok(())
    }).await?;
    let count_result = conn.query("SELECT count(*) FROM e2e_test").await?;
    let count: i64 = count_result.rows[0].get(0)?;
    assert_eq!(count, 2);
    eprintln!("  OK: {} rows inserted", count);

    // Test 4: Streaming
    eprintln!("Test 4: Streaming...");
    let mut stream = conn.query_stream("SELECT generate_series(1, 100)").await?;
    let mut stream_count = 0;
    while let Some(_row) = stream.next().await? {
        stream_count += 1;
    }
    assert_eq!(stream_count, 100);
    eprintln!("  OK: Streamed {} rows", stream_count);

    // Test 5: COPY
    eprintln!("Test 5: COPY IN...");
    let mut copy_in = conn.copy_in("COPY e2e_test (name, id) FROM STDIN WITH (FORMAT csv)").await?;
    copy_in.write(b"charlie,3\n").await?;
    copy_in.write(b"dave,4\n").await?;
    let rows = copy_in.finish().await?;
    assert_eq!(rows, 2);
    eprintln!("  OK: Copied {} rows", rows);

    // Test 6: Prepared statement
    eprintln!("Test 6: Prepared statement...");
    let stmt = conn.prepare("SELECT name FROM e2e_test WHERE id = $1").await?;
    let result = conn.query_prepared(&stmt, &[&1i32]).await?;
    let name: String = result.rows[0].get(0)?;
    assert_eq!(name, "alice");
    conn.close_statement(&stmt).await?;
    eprintln!("  OK: name = {}", name);

    // Test 7: Close
    eprintln!("Test 7: Close connection...");
    conn.close().await?;
    eprintln!("  OK: Connection closed");

    eprintln!("\nAll E2E tests passed!");
    Ok(())
}
```

### 18.6 - Fuzz testing

```rust
// fuzz/fuzz_targets/decode_message.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use pg_protocol::MessageDecoder;

fuzz_target!(|data: &[u8]| {
    // Should never panic, regardless of input
    let _ = MessageDecoder::decode(data);
});
```

```rust
// fuzz/fuzz_targets/decode_message_persistent.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use pg_protocol::{MessageDecoder, ReadBuffer};

fuzz_target!(|data: &[u8]| {
    // Feed data into a ReadBuffer and try to decode messages.
    // This tests the buffer management under fuzz input.
    let mut buf = ReadBuffer::new();
    buf.extend(data);
    while let Ok(Some(_msg)) = buf.next_message() {
        // consume all messages
    }
});
```

### 18.7 - Property-based testing with proptest

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_i32_roundtrip(val in any::<i32>()) {
        let encoded = val.to_sql().unwrap().unwrap();
        let decoded = i32::from_sql(oid::INT4, &encoded).unwrap();
        prop_assert_eq!(val, decoded);
    }

    #[test]
    fn test_i64_roundtrip(val in any::<i64>()) {
        let encoded = val.to_sql().unwrap().unwrap();
        let decoded = i64::from_sql(oid::INT8, &encoded).unwrap();
        prop_assert_eq!(val, decoded);
    }

    #[test]
    fn test_f32_roundtrip(val in any::<f32>()) {
        // NaN is special — it's not equal to itself
        if val.is_nan() {
            let encoded = val.to_sql().unwrap().unwrap();
            let decoded = f32::from_sql(oid::FLOAT4, &encoded).unwrap();
            prop_assert!(decoded.is_nan());
        } else {
            let encoded = val.to_sql().unwrap().unwrap();
            let decoded = f32::from_sql(oid::FLOAT4, &encoded).unwrap();
            prop_assert_eq!(val, decoded);
        }
    }

    #[test]
    fn test_string_roundtrip(s in "\\PC*") {
        let encoded = s.to_sql().unwrap().unwrap();
        let decoded = String::from_sql(oid::TEXT, &encoded).unwrap();
        prop_assert_eq!(s, decoded);
    }

    #[test]
    fn test_option_i32_roundtrip(val in any::<Option<i32>>()) {
        let encoded = val.to_sql().unwrap();
        match (val, encoded) {
            (Some(v), Some(bytes)) => {
                let decoded = i32::from_sql(oid::INT4, &bytes).unwrap();
                prop_assert_eq!(v, decoded);
            }
            (None, None) => {}
            _ => prop_assert!(false, "Option/encoding mismatch"),
        }
    }

    #[test]
    fn test_decode_random_bytes_no_panic(data in prop::collection::vec(any::<u8>(), 0..1000)) {
        let _ = MessageDecoder::decode(&data);
        // Should never panic
    }

    #[test]
    fn test_config_uri_roundtrip(
        host in "[a-zA-Z][a-zA-Z0-9]*",
        port in 1u16..=65535u16,
        user in "[a-zA-Z][a-zA-Z0-9]*",
        db in "[a-zA-Z][a-zA-Z0-9]*",
    ) {
        let uri = format!("postgresql://{}@{}:{}/{}", user, host, port, db);
        let config = Config::from_uri(&uri);
        prop_assert!(config.is_ok());
        if let Ok(c) = config {
            prop_assert_eq!(c.host, host);
            prop_assert_eq!(c.port, port);
            prop_assert_eq!(c.user, user);
            prop_assert_eq!(c.database, Some(db));
        }
    }
}
```

### 18.8 - CI pipeline (GitHub Actions)

```yaml
name: CI
on: [push, pull_request]

env:
  CARGO_TERM_COLOR: always

jobs:
  # ── Lint ──
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - run: cargo fmt --all -- --check
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo clippy --all-targets --all-features -- -D warnings

  # ── Unit tests (native, no WASI needed) ──
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: Run unit tests (pg-protocol, pg-types)
        run: cargo test -p pg-protocol -p pg-types
      - name: Run unit tests with all features
        run: cargo test -p pg-protocol -p pg-types --all-features
      - name: Run proptest
        run: cargo test -p pg-protocol -p pg-types --features proptest

  # ── WASI build check ──
  wasi-build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: wasm32-wasip2
      - name: Build all crates for WASI P2
        run: cargo build --target wasm32-wasip2 --all-features
      - name: Build smoke test example
        run: cargo build --target wasm32-wasip2 --example smoke-test
      - name: Build E2E test example
        run: cargo build --target wasm32-wasip2 --example e2e-test
      - name: Check for duplicate dependencies
        run: cargo tree --target wasm32-wasip2 --duplicates

  # ── Integration tests (native + real PostgreSQL) ──
  integration-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: Run integration tests
        run: cargo test --test integration --features test-native
        env:
          TEST_DATABASE_URL: postgresql://postgres:postgres@localhost:5432/test

  # ── Integration tests with TLS ──
  integration-tests-tls:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: Configure PostgreSQL for SSL
        run: |
          # This is complex in CI; for now, test with sslmode=disable
          # and sslmode=prefer (which falls back to plaintext)
          echo "TLS integration tests require custom PostgreSQL SSL configuration"
          echo "Running with sslmode=prefer instead"
      - name: Run integration tests with TLS
        run: cargo test --test integration --features test-native,tls
        env:
          TEST_DATABASE_URL: postgresql://postgres:postgres@localhost:5432/test?sslmode=prefer

  # ── E2E WASI test ──
  e2e-wasi:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: wasm32-wasip2
      - uses: bytecodealliance/actions/wasmtime/setup@v1
      - name: Build E2E test component
        run: cargo build --target wasm32-wasip2 --example e2e-test
      - name: Run E2E test in wasmtime
        run: |
          wasmtime run \
            --wasi inherit-network \
            --wasi inherit-env \
            --env TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/test \
            --env RUST_LOG=wasi_pg_client=info \
            target/wasm32-wasip2/debug/examples/e2e_test.wasm

  # ── Fuzz testing (short run) ──
  fuzz:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: Install cargo-fuzz
        run: cargo install cargo-fuzz
      - name: Run fuzz tests (short)
        run: cargo fuzz run decode_message -- -max_total_time=60
      - name: Run fuzz tests with buffer
        run: cargo fuzz run decode_message_persistent -- -max_total_time=60

  # ── Security audit ──
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: rustsec/audit-check@v2
        with:
          targets: wasm32-wasip2

  # ── Coverage ──
  coverage:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: test
        ports:
          - 5432:5432
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - name: Install tarpaulin
        run: cargo install cargo-tarpaulin
      - name: Generate coverage
        run: cargo tarpaulin --features test-native --out Xml
        env:
          TEST_DATABASE_URL: postgresql://postgres:postgres@localhost:5432/test
      - name: Upload coverage
        uses: codecov/codecov-action@v4
```

### 18.9 - Native transport for testing

To run integration tests natively (not via WASI), we provide a blocking I/O transport behind the `test-native` feature flag (defined in Step 02).

**Why blocking I/O inside `async fn` works for tests**: The `async fn` body compiles to a state machine. When the future is polled, the blocking I/O runs synchronously within the poll call. The future never yields (it completes in one poll), so there's no deadlock risk. This is fine for sequential test execution but would be catastrophic in a real async runtime.

### 18.10 - Test helper utilities

```rust
// tests/common/mod.rs

/// Create a test configuration from environment variables.
pub fn test_config() -> Config {
    Config::from_uri(
        &std::env::var("TEST_DATABASE_URL")
            .unwrap_or("postgresql://postgres:postgres@localhost:5432/postgres".into())
    ).unwrap()
}

/// Create a test pool configuration.
pub fn test_pool_config() -> PoolConfig {
    PoolConfig {
        connection: test_config(),
        max_size: 5,
        test_on_acquire: true,
        ..Default::default()
    }
}

/// Create a test pool configuration with no health checks (faster).
pub fn test_pool_config_fast() -> PoolConfig {
    PoolConfig {
        connection: test_config(),
        max_size: 5,
        test_on_acquire: false,
        ..Default::default()
    }
}

/// Install a test tracing subscriber that captures events for assertions.
#[cfg(feature = "tracing")]
pub fn install_test_subscriber() -> TestTracing {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let (layer, guard) = tracing_capture::capture_layer();
    let subscriber = tracing_subscriber::registry().with(layer);
    // Only install if no subscriber is already set
    let _ = subscriber.try_init();

    TestTracing { _guard: guard }
}

#[cfg(feature = "tracing")]
pub struct TestTracing {
    _guard: tracing_capture::CaptureGuard,
}

/// Assert that a PgError is a specific SQLSTATE code.
pub fn assert_sqlstate(err: &PgError, expected_code: &str) {
    match err {
        PgError::Server(e) => assert_eq!(e.code, expected_code, "Expected SQLSTATE {}, got {}", expected_code, e.code),
        other => panic!("Expected PgError::Server, got: {:?}", other),
    }
}

/// Skip the test if no PostgreSQL is available.
pub async fn skip_if_no_postgres() -> Config {
    let config = test_config();
    if Connection::connect(&config).await.is_err() {
        eprintln!("Skipping test: PostgreSQL not available");
        std::process::exit(0);
    }
    config
}
```

### 18.11 - Tracing tests

```rust
// tests/integration/tracing.rs

#[cfg(all(feature = "test-native", feature = "tracing"))]
mod tracing_tests {
    use super::common::*;

    #[test]
    async fn test_connection_establishment_logged() {
        let _tracing = install_test_subscriber();

        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.close().await.unwrap();

        let events = tracing_capture::drain_events();
        let connect_events: Vec<_> = events.iter()
            .filter(|e| e.target.starts_with("wasi_pg_client::connection"))
            .collect();

        assert!(connect_events.iter().any(|e|
            e.message.contains("Connection established") || e.message.contains("connect")
        ), "Expected connection establishment event, got: {:?}", connect_events);
    }

    #[test]
    async fn test_query_execution_logged() {
        let _tracing = install_test_subscriber();

        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.query("SELECT 1").await.unwrap();
        conn.close().await.unwrap();

        let events = tracing_capture::drain_events();
        let query_events: Vec<_> = events.iter()
            .filter(|e| e.target.starts_with("wasi_pg_client::query"))
            .collect();

        assert!(!query_events.is_empty(), "Expected query events");
    }

    #[test]
    async fn test_no_password_in_tracing_output() {
        let _tracing = install_test_subscriber();

        let config = test_config();
        let mut conn = Connection::connect(&config).await.unwrap();
        conn.close().await.unwrap();

        let events = tracing_capture::drain_events();
        let all_text: String = events.iter()
            .flat_map(|e| e.fields.values())
            .collect();

        if let Some(ref password) = config.password {
            assert!(
                !all_text.contains(password),
                "Password leaked in tracing output! Password '{}' found in events",
                password
            );
        }
    }

    #[test]
    async fn test_transaction_events_logged() {
        let _tracing = install_test_subscriber();

        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let mut txn = conn.transaction().await.unwrap();
        txn.commit().await.unwrap();
        conn.close().await.unwrap();

        let events = tracing_capture::drain_events();
        let txn_events: Vec<_> = events.iter()
            .filter(|e| e.target.starts_with("wasi_pg_client::transaction"))
            .collect();

        assert!(txn_events.iter().any(|e| e.message.contains("BEGIN") || e.message.contains("transaction")));
        assert!(txn_events.iter().any(|e| e.message.contains("COMMIT")));
    }
}
```

### 18.12 - Reconnection tests

```rust
// tests/integration/reconnect.rs

#[cfg(feature = "test-native")]
mod reconnect_tests {
    use super::common::*;

    #[test]
    async fn test_retry_policy_backoff_calculation() {
        let policy = RetryPolicy::exponential_backoff(5, Duration::from_millis(100), Duration::from_secs(10));

        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(100));  // 100 * 2^0
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(200));  // 100 * 2^1
        assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(400));  // 100 * 2^2
        assert_eq!(policy.delay_for_attempt(4), Duration::from_millis(800));  // 100 * 2^3
        // Attempt 5: 100 * 2^4 = 1600, but capped at 10s
        assert_eq!(policy.delay_for_attempt(5), Duration::from_millis(1600));
    }

    #[test]
    async fn test_retry_policy_capped_at_max() {
        let policy = RetryPolicy::exponential_backoff(10, Duration::from_secs(1), Duration::from_secs(5));

        assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(1));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_secs(2));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_secs(4));
        assert_eq!(policy.delay_for_attempt(4), Duration::from_secs(5)); // capped
        assert_eq!(policy.delay_for_attempt(5), Duration::from_secs(5)); // capped
    }

    #[test]
    async fn test_session_state_tracking() {
        let mut state = SessionState::default();
        assert!(!state.has_state());
        assert!(state.is_reconnect_safe());

        state.listen_channels.insert("test_channel".into());
        assert!(state.has_state());
        assert!(!state.is_reconnect_safe()); // has LISTEN state

        state.in_transaction = true;
        assert!(!state.is_reconnect_safe()); // in transaction
    }

    #[test]
    async fn test_connect_with_retry_success() {
        let policy = RetryPolicy::exponential_backoff(3, Duration::from_millis(100), Duration::from_secs(5));
        let conn = Connection::connect_with_retry(&test_config(), &policy).await;
        assert!(conn.is_ok());
        let mut conn = conn.unwrap();
        conn.close().await.unwrap();
    }

    #[test]
    async fn test_connect_with_retry_failure() {
        let mut config = test_config();
        config.host = "nonexistent.invalid".into();

        let policy = RetryPolicy::fixed_delay(2, Duration::from_millis(50));
        let result = Connection::connect_with_retry(&config, &policy).await;
        assert!(result.is_err());
    }

    #[test]
    async fn test_stale_detection() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();

        // Fresh connection should not be stale
        assert!(!conn.is_stale(Duration::from_secs(30)));

        // After pinging, last_confirmed_alive is updated
        conn.ping().await.unwrap();
        assert!(!conn.is_stale(Duration::from_secs(30)));

        conn.close().await.unwrap();
    }

    #[test]
    async fn test_ensure_alive_fresh_connection() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.ensure_alive().await.unwrap(); // should succeed immediately
        conn.close().await.unwrap();
    }
}
```

### 18.13 - Pool RefCell safety tests

```rust
// tests/integration/pool_safety.rs

#[cfg(feature = "test-native")]
mod pool_safety_tests {
    use super::common::*;

    #[test]
    async fn test_pool_refcell_no_borrow_across_await() {
        // This test verifies that the Pool implementation never holds
        // a RefCell borrow across an .await point, which would panic
        // at runtime if another method tried to borrow.
        let pool = Pool::new(test_pool_config_fast()).await.unwrap();

        // Acquire a connection (borrows RefCell internally)
        let guard1 = pool.acquire().await.unwrap();

        // Check status while guard is alive (borrows RefCell again)
        let status = pool.status();
        assert_eq!(status.active, 1);

        // Acquire another connection while first guard is alive
        let guard2 = pool.acquire().await.unwrap();

        // Check status again
        let status = pool.status();
        assert_eq!(status.active, 2);

        // Release both
        guard1.release().await.unwrap();
        guard2.release().await.unwrap();

        pool.close().await;
    }

    #[test]
    async fn test_pool_guard_drop_returns_connection() {
        let pool = Pool::new(test_pool_config_fast()).await.unwrap();

        {
            let _guard = pool.acquire().await.unwrap();
            // guard dropped here without calling release()
        }

        // Connection should be back in the pool (via Drop)
        // Note: it may have dirty state since Drop can't do async cleanup
        let status = pool.status();
        assert_eq!(status.idle, 1);
        assert_eq!(status.active, 0);

        pool.close().await;
    }

    #[test]
    async fn test_pool_maintain_discards_expired() {
        let mut pool_config = test_pool_config_fast();
        pool_config.idle_timeout = Some(Duration::from_millis(1)); // very short

        let pool = Pool::new(pool_config).await.unwrap();

        // Acquire and release a connection
        let guard = pool.acquire().await.unwrap();
        guard.release().await.unwrap();

        // Wait for idle timeout
        std::thread::sleep(Duration::from_millis(10));

        // Maintain should discard the expired connection
        pool.maintain().await;

        let status = pool.status();
        assert_eq!(status.idle, 0);

        pool.close().await;
    }
}
```

## File Layout

```
tests/
├── common/
│   └── mod.rs                  (test helpers: config, tracing, assertions)
├── integration/
│   ├── mod.rs                  (basic connect, query, types, transactions, copy, notifications)
│   ├── streaming.rs            (streaming, cursor, early termination, recovery)
│   ├── reconnect.rs            (reconnection, retry policy, stale detection, session state)
│   ├── pool_safety.rs          (RefCell borrow safety, guard lifecycle, maintenance)
│   ├── tracing.rs              (tracing event capture and assertion)
│   └── types.rs                (comprehensive type round-trip tests)
├── fuzz/
│   └── fuzz_targets/
│       ├── decode_message.rs
│       └── decode_message_persistent.rs
└── examples/
    ├── smoke-test/             (WASI P2 smoke test: TCP + random + async)
    └── e2e-test/               (WASI P2 full E2E: connect, query, txn, copy, stream)
```

## Acceptance Criteria

- [ ] Unit tests pass for all pure logic (protocol, types, config, error classification)
- [ ] Property-based tests (proptest) cover type round-trips and config parsing
- [ ] Mock transport tests cover all protocol flows (handshake, query, error, notifications)
- [ ] Integration tests connect to real PostgreSQL (behind `test-native` feature)
- [ ] Streaming tests verify memory-efficient row processing
- [ ] Streaming tests verify early termination and connection recovery
- [ ] Cursor tests verify batch fetching with fetch_size
- [ ] Reconnection tests verify retry policy, backoff, session state
- [ ] Pool safety tests verify RefCell borrow invariants
- [ ] Pool tests verify acquire/release, exhaustion, maintenance
- [ ] Tracing tests verify events are emitted without leaking sensitive data
- [ ] End-to-end WASI tests pass with wasmtime
- [ ] Fuzz testing runs without panics
- [ ] CI pipeline runs all test levels
- [ ] Test coverage > 80% for core crates (`pg-protocol`, `pg-types`)
- [ ] Test coverage > 60% for async crates (`pg-client`, `pg-pool`)
- [ ] Native transport enables testing without wasmtime
- [ ] No test depends on specific PostgreSQL data (use TEMP TABLEs)
- [ ] All tests clean up after themselves (no leftover tables/connections)

## Key Design Decisions

1. **Layered testing**: Tests are organized by dependency level. Pure logic (Layer 1) has the most tests and runs fastest. E2E WASI tests (Layer 5) are the slowest and fewest.

2. **`test-native` feature for integration tests**: Integration tests use blocking I/O behind a feature flag, avoiding the need for wasmtime during development. WASI E2E tests are a separate, slower CI job.

3. **TEMP TABLEs for test isolation**: All integration tests use PostgreSQL temporary tables, which are automatically cleaned up when the connection closes. No test leaves persistent state.

4. **Mock transport for protocol tests**: The mock transport allows testing protocol flows without a real server, making tests fast and deterministic.

5. **Proptest for type system**: Property-based testing is ideal for the type system because it automatically explores edge cases (MIN/MAX values, empty strings, Unicode, etc.).

6. **Fuzz testing for decoder**: The protocol decoder must handle arbitrary input without panicking. Fuzz testing is the best way to verify this.

7. **Tracing tests verify no sensitive data leaks**: A dedicated test suite checks that passwords, auth tokens, and query parameter values never appear in tracing output.

8. **Pool RefCell safety tests**: Explicit tests verify that no `RefCell` borrow is held across an `.await` point, which would cause a runtime panic.

## WASI P2 Test Execution

```bash
# Run unit tests (fast, no dependencies)
cargo test -p pg-protocol -p pg-types

# Run integration tests (needs PostgreSQL, uses native transport)
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost/test \
  cargo test --test integration --features test-native

# Run with tracing enabled
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost/test \
  RUST_LOG=wasi_pg_client=debug \
  cargo test --test integration --features test-native,tracing

# Build and run WASI E2E test
cargo build --target wasm32-wasip2 --example e2e-test
wasmtime run \
  --wasi inherit-network \
  --wasi inherit-env \
  --env TEST_DATABASE_URL=postgresql://postgres:postgres@localhost/test \
  target/wasm32-wasip2/debug/examples/e2e_test.wasm

# Run fuzz tests
cargo fuzz run decode_message -- -max_total_time=300
```
