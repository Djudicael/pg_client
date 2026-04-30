# Step 04 - PostgreSQL Wire Protocol (Message Codec)

## Goal
Implement the PostgreSQL v3 Frontend/Backend wire protocol as a standalone, I/O-free codec in the `pg-protocol` crate.

## Context
The PostgreSQL wire protocol (v3, since PG 7.4) is a binary, message-based protocol. Every message (except startup) has the format:
```
| type: u8 | length: i32 (includes self, excludes type) | payload: [u8] |
```
The startup message is special: no type byte, just `length + payload`.

Keeping the codec I/O-free means it operates on byte buffers only, making it testable, portable, and reusable.

## Tasks

### 4.1 - Frontend messages (client → server)

```rust
pub enum FrontendMessage {
    // Startup
    StartupMessage { params: Vec<(String, String)> },  // user, database, etc.
    SSLRequest,
    CancelRequest { process_id: i32, secret_key: i32 },

    // Simple query
    Query { sql: String },

    // Extended query
    Parse { name: String, sql: String, param_types: Vec<Oid> },
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<FormatCode>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<FormatCode>,
    },
    Describe { variant: DescribeVariant, name: String },
    Execute { portal: String, max_rows: i32 },
    Close { variant: CloseVariant, name: String },
    Sync,
    Flush,

    // COPY
    CopyData { data: Vec<u8> },
    CopyDone,
    CopyFail { message: String },

    // Auth
    PasswordMessage { password: String },
    SASLInitialResponse { mechanism: String, data: Vec<u8> },
    SASLResponse { data: Vec<u8> },

    // Control
    Terminate,
}
```

### 4.2 - Backend messages (server → client)

```rust
pub enum BackendMessage {
    // Auth
    AuthenticationOk,
    AuthenticationCleartextPassword,
    AuthenticationMD5Password { salt: [u8; 4] },
    AuthenticationSASL { mechanisms: Vec<String> },
    AuthenticationSASLContinue { data: Vec<u8> },
    AuthenticationSASLFinal { data: Vec<u8> },

    // Startup phase
    BackendKeyData { process_id: i32, secret_key: i32 },
    ParameterStatus { name: String, value: String },
    ReadyForQuery { transaction_status: TransactionStatus },

    // Query results
    RowDescription { fields: Vec<FieldDescription> },
    DataRow { columns: Vec<Option<Vec<u8>>> },
    CommandComplete { tag: CommandTag },
    EmptyQueryResponse,

    // Extended query
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    ParameterDescription { param_types: Vec<Oid> },
    PortalSuspended,

    // COPY
    CopyInResponse { format: FormatCode, column_formats: Vec<FormatCode> },
    CopyOutResponse { format: FormatCode, column_formats: Vec<FormatCode> },
    CopyData { data: Vec<u8> },
    CopyDone,

    // Error/Notice
    ErrorResponse { fields: Vec<(u8, String)> },
    NoticeResponse { fields: Vec<(u8, String)> },

    // Notification
    NotificationResponse { process_id: i32, channel: String, payload: String },
}
```

### 4.3 - Message encoding (serialize)
```rust
pub struct MessageEncoder {
    buf: Vec<u8>,
}

impl MessageEncoder {
    pub fn encode(&mut self, msg: &FrontendMessage) -> &[u8] {
        self.buf.clear();
        match msg {
            FrontendMessage::Query { sql } => {
                self.buf.push(b'Q');
                let len_pos = self.buf.len();
                self.buf.extend_from_slice(&[0; 4]); // placeholder
                self.write_cstring(sql);
                self.set_length(len_pos);
            }
            // ... other messages
        }
        &self.buf
    }
}
```

### 4.4 - Message decoding (deserialize)
```rust
pub struct MessageDecoder;

impl MessageDecoder {
    /// Attempt to decode a backend message from the buffer.
    /// Returns Ok(Some((message, bytes_consumed))) or Ok(None) if buffer incomplete.
    pub fn decode(buf: &[u8]) -> Result<Option<(BackendMessage, usize)>, ProtocolError> {
        if buf.len() < 5 {
            return Ok(None); // need at least type + length
        }
        let msg_type = buf[0];
        let length = i32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
        let total = 1 + length; // type byte + length (includes itself)

        if buf.len() < total {
            return Ok(None); // incomplete message
        }

        let payload = &buf[5..total];
        let msg = match msg_type {
            b'R' => Self::decode_auth(payload)?,
            b'K' => Self::decode_backend_key_data(payload)?,
            b'S' => Self::decode_parameter_status(payload)?,
            b'Z' => Self::decode_ready_for_query(payload)?,
            b'T' => Self::decode_row_description(payload)?,
            b'D' => Self::decode_data_row(payload)?,
            b'C' => Self::decode_command_complete(payload)?,
            b'E' => Self::decode_error_response(payload)?,
            b'N' => Self::decode_notice_response(payload)?,
            b'1' => BackendMessage::ParseComplete,
            b'2' => BackendMessage::BindComplete,
            b'3' => BackendMessage::CloseComplete,
            b'n' => BackendMessage::NoData,
            b's' => BackendMessage::PortalSuspended,
            b'I' => BackendMessage::EmptyQueryResponse,
            b'A' => Self::decode_notification(payload)?,
            b'G' => Self::decode_copy_in_response(payload)?,
            b'H' => Self::decode_copy_out_response(payload)?,
            b'd' => BackendMessage::CopyData { data: payload.to_vec() },
            b'c' => BackendMessage::CopyDone,
            b't' => Self::decode_parameter_description(payload)?,
            other => return Err(ProtocolError::UnknownMessageType(other)),
        };
        Ok(Some((msg, total)))
    }
}
```

### 4.5 - Supporting types
```rust
pub type Oid = u32;

#[derive(Debug, Clone, Copy)]
pub enum FormatCode {
    Text = 0,
    Binary = 1,
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionStatus {
    Idle,          // 'I'
    InTransaction, // 'T'
    Failed,        // 'E'
}

#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: Oid,
    pub column_id: i16,
    pub type_oid: Oid,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: FormatCode,
}

#[derive(Debug, Clone)]
pub struct CommandTag {
    pub command: String,   // INSERT, UPDATE, DELETE, SELECT, etc.
    pub rows_affected: Option<u64>,
}
```

### 4.6 - Read buffer management
```rust
pub struct ReadBuffer {
    buf: Vec<u8>,
    cursor: usize,
}

impl ReadBuffer {
    /// Append new data from the transport
    pub fn extend(&mut self, data: &[u8]);

    /// Try to decode the next message
    pub fn next_message(&mut self) -> Result<Option<BackendMessage>, ProtocolError>;

    /// Compact: move unconsumed bytes to the front
    pub fn compact(&mut self);
}
```

## File Layout
```
crates/pg-protocol/src/
├── lib.rs
├── frontend.rs      (FrontendMessage, MessageEncoder)
├── backend.rs       (BackendMessage, MessageDecoder)
├── types.rs         (Oid, FormatCode, TransactionStatus, FieldDescription, etc.)
├── buffer.rs        (ReadBuffer)
└── error.rs         (ProtocolError)
```

## Acceptance Criteria
- [ ] All frontend messages can be encoded correctly
- [ ] All backend messages can be decoded correctly
- [ ] Round-trip encoding/decoding is correct (for messages that exist both ways)
- [ ] Handles partial messages (returns None, waits for more data)
- [ ] Handles unknown message types gracefully
- [ ] Zero I/O - purely operates on `&[u8]` and `Vec<u8>`
- [ ] Fuzz-tested against malformed input

## Testing
- **Unit tests**: Encode each frontend message, verify bytes match expected format
- **Unit tests**: Decode known byte sequences into correct backend messages
- **Property tests**: Encode → decode round-trip (for shared message types like CopyData)
- **Fuzz tests**: Feed random bytes to decoder, ensure no panics
- **Reference tests**: Capture real PG protocol traffic with `tcpdump` / Wireshark, replay through decoder
