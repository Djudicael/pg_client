# Step 11 - COPY Protocol (Async)

## Goal
Implement PostgreSQL's COPY protocol for high-performance bulk data import and export.

## Context
The COPY protocol is PostgreSQL's most efficient way to transfer large amounts of data:
- **COPY IN** (client -> server): Bulk insert data
- **COPY OUT** (server -> client): Bulk export data

It uses a streaming approach where data flows as `CopyData` messages, much more efficient than individual INSERT statements.

All network I/O is async. COPY streams are consumed with async read/write operations.

## Tasks

### 11.1 - COPY IN (bulk import)
```rust
impl Connection {
    /// Start a COPY IN operation. Returns a CopyIn writer.
    pub async fn copy_in(&mut self, sql: &str) -> Result<CopyIn<'_>, PgError> {
        // sql should be like: COPY table FROM STDIN [WITH (FORMAT csv, ...)]
        self.codec.send(&mut self.transport, &FrontendMessage::Query {
            sql: sql.to_string(),
        }).await?;

        let msg = self.codec.read_message(&mut self.transport).await?;
        match msg {
            BackendMessage::CopyInResponse { format, column_formats } => {
                Ok(CopyIn {
                    conn: self,
                    format,
                    column_formats,
                    done: false,
                })
            }
            BackendMessage::ErrorResponse { fields } => {
                self.read_until_ready().await?;
                Err(PgError::Server(PgServerError::from_fields(fields)))
            }
            _ => Err(PgError::Protocol("Expected CopyInResponse".into())),
        }
    }
}

pub struct CopyIn<'a> {
    conn: &'a mut Connection,
    format: FormatCode,
    column_formats: Vec<FormatCode>,
    done: bool,
}

impl<'a> CopyIn<'a> {
    /// Send a chunk of COPY data
    pub async fn write(&mut self, data: &[u8]) -> Result<(), PgError> {
        self.conn.codec.send(&mut self.conn.transport, &FrontendMessage::CopyData {
            data: data.to_vec(),
        }).await
    }

    /// Send a single row in text format (tab-separated, newline-terminated)
    pub async fn write_row(&mut self, columns: &[&str]) -> Result<(), PgError> {
        let line = columns.join("\t") + "\n";
        self.write(line.as_bytes()).await
    }

    /// Finish the COPY operation successfully
    pub async fn finish(mut self) -> Result<u64, PgError> {
        self.conn.codec.send(&mut self.conn.transport, &FrontendMessage::CopyDone).await?;

        let mut rows = 0;
        loop {
            let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
            match msg {
                BackendMessage::CommandComplete { tag } => {
                    rows = tag.rows_affected.unwrap_or(0);
                }
                BackendMessage::ReadyForQuery { transaction_status } => {
                    self.conn.transaction_status = transaction_status;
                    self.done = true;
                    break;
                }
                BackendMessage::ErrorResponse { fields } => {
                    self.conn.read_until_ready().await?;
                    self.done = true;
                    return Err(PgError::Server(PgServerError::from_fields(fields)));
                }
                _ => {}
            }
        }
        Ok(rows)
    }

    /// Cancel the COPY operation
    pub async fn cancel(mut self, reason: &str) -> Result<(), PgError> {
        self.conn.codec.send(&mut self.conn.transport, &FrontendMessage::CopyFail {
            message: reason.to_string(),
        }).await?;
        self.conn.read_until_ready().await?;
        self.done = true;
        Ok(())
    }
}

impl<'a> Drop for CopyIn<'a> {
    fn drop(&mut self) {
        if !self.done {
            // NOTE: Drop cannot be async. Best-effort: write CopyFail message
            // synchronously. Users should call .finish().await or .cancel().await.
        }
    }
}
```

### 11.2 - COPY OUT (bulk export)
```rust
impl Connection {
    /// Start a COPY OUT operation. Returns a CopyOut reader.
    pub async fn copy_out(&mut self, sql: &str) -> Result<CopyOut<'_>, PgError> {
        // sql should be like: COPY table TO STDOUT [WITH (FORMAT csv, ...)]
        self.codec.send(&mut self.transport, &FrontendMessage::Query {
            sql: sql.to_string(),
        }).await?;

        let msg = self.codec.read_message(&mut self.transport).await?;
        match msg {
            BackendMessage::CopyOutResponse { format, column_formats } => {
                Ok(CopyOut {
                    conn: self,
                    format,
                    column_formats,
                    done: false,
                })
            }
            BackendMessage::ErrorResponse { fields } => {
                self.read_until_ready().await?;
                Err(PgError::Server(PgServerError::from_fields(fields)))
            }
            _ => Err(PgError::Protocol("Expected CopyOutResponse".into())),
        }
    }
}

pub struct CopyOut<'a> {
    conn: &'a mut Connection,
    format: FormatCode,
    column_formats: Vec<FormatCode>,
    done: bool,
}

impl<'a> CopyOut<'a> {
    /// Read the next chunk of COPY data. Returns None when complete.
    pub async fn read_next(&mut self) -> Result<Option<Vec<u8>>, PgError> {
        loop {
            let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
            match msg {
                BackendMessage::CopyData { data } => {
                    return Ok(Some(data));
                }
                BackendMessage::CopyDone => {
                    // Read CommandComplete + ReadyForQuery
                }
                BackendMessage::CommandComplete { .. } => {}
                BackendMessage::ReadyForQuery { transaction_status } => {
                    self.conn.transaction_status = transaction_status;
                    self.done = true;
                    return Ok(None);
                }
                BackendMessage::ErrorResponse { fields } => {
                    self.conn.read_until_ready().await?;
                    self.done = true;
                    return Err(PgError::Server(PgServerError::from_fields(fields)));
                }
                _ => {}
            }
        }
    }

    /// Read all COPY data into a single buffer
    pub async fn read_all(&mut self) -> Result<Vec<u8>, PgError> {
        let mut result = Vec::new();
        while let Some(chunk) = self.read_next().await? {
            result.extend_from_slice(&chunk);
        }
        Ok(result)
    }

    /// Process each chunk with a callback (streaming)
    pub async fn for_each<F>(&mut self, mut f: F) -> Result<(), PgError>
    where
        F: FnMut(&[u8]) -> Result<(), PgError>,
    {
        while let Some(chunk) = self.read_next().await? {
            f(&chunk)?;
        }
        Ok(())
    }
}
```

### 11.3 - COPY format helpers
```rust
pub enum CopyFormat {
    Text,
    Csv {
        delimiter: char,
        null: String,
        header: bool,
        quote: char,
        escape: char,
    },
    Binary,
}

impl CopyFormat {
    pub fn to_sql_options(&self) -> String {
        match self {
            CopyFormat::Text => String::new(),
            CopyFormat::Csv { delimiter, null, header, quote, escape } => {
                format!(
                    "WITH (FORMAT csv, DELIMITER '{}', NULL '{}', HEADER {}, QUOTE '{}', ESCAPE '{}')",
                    delimiter, null, header, quote, escape
                )
            }
            CopyFormat::Binary => "WITH (FORMAT binary)".to_string(),
        }
    }
}
```

### 11.4 - Binary COPY format
PostgreSQL binary COPY has a specific format:
```
Header: 11-byte signature + flags(4) + header extension(4)
Tuple:  field_count(i16) + [field_length(i32) + field_data(bytes)]*
Trailer: field_count = -1 (i16)
```

```rust
pub struct BinaryCopyWriter {
    buf: Vec<u8>,
    column_count: i16,
}

impl BinaryCopyWriter {
    pub fn new(column_count: i16) -> Self;
    pub fn header(&mut self) -> &[u8];         // generate binary header
    pub fn write_row(&mut self, values: &[Option<&[u8]>]) -> &[u8];
    pub fn trailer(&mut self) -> &[u8];        // generate -1 trailer
}
```

## File Layout
```
crates/pg-client/src/
├── copy/
│   ├── mod.rs          (copy_in, copy_out entry points)
│   ├── copy_in.rs      (CopyIn writer)
│   ├── copy_out.rs     (CopyOut reader)
│   ├── format.rs       (CopyFormat, options)
│   └── binary.rs       (BinaryCopyWriter, binary format handling)
```

## Acceptance Criteria
- [ ] COPY IN with text format works
- [ ] COPY IN with CSV format works
- [ ] COPY IN with binary format works
- [ ] COPY OUT with text format works
- [ ] COPY OUT with CSV format works
- [ ] COPY OUT with binary format works
- [ ] Streaming (chunk-by-chunk) processing
- [ ] CopyIn auto-cancels on Drop if not finished
- [ ] Error handling (server rejects data)
- [ ] Works within transactions
- [ ] Performance: significantly faster than individual INSERTs

## Testing
- Bulk insert 10k rows via COPY IN, verify count
- COPY OUT and verify data matches
- CSV format with special characters (quotes, newlines, delimiters)
- Binary format round-trip
- Error: malformed data rejected by server
- COPY within a transaction (rollback test)
- Drop without finish (auto-cancel)
