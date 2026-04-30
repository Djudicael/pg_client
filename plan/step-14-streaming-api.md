# Step 14 - Streaming API for Query Results

## Goal
Implement an async streaming API for query results that processes rows one at a time without buffering the entire result set in memory. This is essential for large result sets, memory-constrained WASI environments, and backpressure-aware processing.

## Context
The original query API (Steps 07/08) collects all rows into a `Vec<Row>`, which is simple but has critical limitations:

1. **Memory**: A query returning 1 million rows buffers all of them in memory. In WASI P2 environments with limited memory, this can cause OOM crashes.
2. **Latency**: The caller must wait for all rows to arrive before processing any. With streaming, the first row is available as soon as it arrives from the server.
3. **Backpressure**: With `Vec<Row>`, the server sends rows as fast as the network allows. With streaming, the client controls the pace — the server only sends more rows when the client asks for them (via TCP flow control / read buffering).

The PostgreSQL wire protocol naturally supports streaming: `DataRow` messages arrive one at a time, and the client reads them sequentially. We just need to expose this as an async iterator instead of collecting into a `Vec`.

**Design principle**: Streaming is the **primary** API. The convenience methods (`query()` → `Vec<Row>`) are built on top of the stream by collecting all rows.

## Tasks

### 14.1 - `RowStream` type

```rust
use std::pin::Pin;
use std::task::{Context, Poll};

/// An async stream of rows from a query result.
///
/// Rows are fetched from the server one at a time as the consumer calls `next()`.
/// This provides natural backpressure: the server only sends rows when the client
/// reads them, and memory usage is O(1) regardless of result set size.
///
/// # Lifetime
///
/// `RowStream` borrows the connection mutably. You cannot use the connection
/// while iterating the stream. When the stream is dropped (or fully consumed),
/// the connection is available again.
///
/// # Error handling
///
/// If an error occurs mid-stream (e.g., connection drops), the stream returns
/// `Some(Err(...))` and then `None`. The connection may be in an inconsistent
/// state after an error — call `read_until_ready()` to recover.
///
/// # Example
///
/// ```rust
/// let mut stream = conn.query_stream("SELECT id, name FROM users").await?;
/// while let Some(row) = stream.next().await? {
///     let id: i32 = row.get(0)?;
///     let name: String = row.get(1)?;
///     println!("{}: {}", id, name);
/// }
/// // Connection is available again here
/// ```
pub struct RowStream<'a> {
    /// Mutable borrow of the connection. Released when the stream is dropped.
    conn: &'a mut Connection,

    /// Column metadata for the current result set.
    /// Set when RowDescription is received; None before that or after error.
    columns: Option<Arc<Vec<FieldDescription>>>,

    /// State of the stream.
    state: RowStreamState,

    /// Whether this stream was created from the extended query protocol.
    /// Extended query streams read until Sync's ReadyForQuery.
    /// Simple query streams read until the natural ReadyForQuery.
    extended_protocol: bool,
}

/// Internal state of the row stream.
enum RowStreamState {
    /// Waiting for RowDescription (first message from server).
    WaitingForDescription,

    /// Receiving DataRow messages. Columns are known.
    ReceivingRows,

    /// Stream is complete (CommandComplete received, waiting for ReadyForQuery).
    Finishing { command_tag: CommandTag },

    /// Stream is done (ReadyForQuery received).
    Done,

    /// Stream encountered an error. Connection may need recovery.
    Error,
}
```

### 14.2 - `RowStream` async iteration

```rust
impl<'a> RowStream<'a> {
    /// Fetch the next row from the stream.
    ///
    /// Returns:
    /// - `Ok(Some(row))` — a row was received
    /// - `Ok(None)` — the stream is complete (no more rows)
    /// - `Err(e)` — an error occurred
    ///
    /// After returning `None` or `Err`, subsequent calls return `None`.
    pub async fn next(&mut self) -> Result<Option<Row>, PgError> {
        loop {
            match self.state {
                RowStreamState::Done | RowStreamState::Error => {
                    return Ok(None);
                }

                RowStreamState::WaitingForDescription => {
                    let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
                    match msg {
                        BackendMessage::RowDescription { fields } => {
                            self.columns = Some(Arc::new(fields));
                            self.state = RowStreamState::ReceivingRows;
                            // Continue loop to read first DataRow
                        }
                        BackendMessage::CommandComplete { tag } => {
                            // Query returned no rows (e.g., INSERT)
                            self.state = RowStreamState::Finishing { command_tag: tag };
                            // Continue loop to read ReadyForQuery
                        }
                        BackendMessage::EmptyQueryResponse => {
                            self.state = RowStreamState::Finishing {
                                command_tag: CommandTag::default(),
                            };
                        }
                        BackendMessage::ErrorResponse { fields } => {
                            self.conn.read_until_ready().await?;
                            self.state = RowStreamState::Error;
                            return Err(PgError::Server(PgServerError::from_fields(fields)));
                        }
                        // Intercept async messages
                        BackendMessage::NotificationResponse { process_id, channel, payload } => {
                            self.conn.notification_queue.push_back(Notification {
                                process_id, channel, payload,
                            });
                            continue; // read next message
                        }
                        BackendMessage::NoticeResponse { fields } => {
                            // TODO: dispatch to notice handler
                            continue;
                        }
                        BackendMessage::ParameterStatus { name, value } => {
                            self.conn.server_params.params.insert(name, value);
                            continue;
                        }
                        _ => {
                            // Unexpected message during this state
                            continue;
                        }
                    }
                }

                RowStreamState::ReceivingRows => {
                    let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
                    match msg {
                        BackendMessage::DataRow { values } => {
                            let cols = self.columns.as_ref().unwrap();
                            return Ok(Some(Row {
                                columns: cols.clone(),
                                values,
                            }));
                        }
                        BackendMessage::CommandComplete { tag } => {
                            // All rows received; transition to finishing
                            self.state = RowStreamState::Finishing { command_tag: tag };
                            // Continue loop to read ReadyForQuery
                        }
                        BackendMessage::ErrorResponse { fields } => {
                            let err = PgServerError::from_fields(fields);
                            self.conn.read_until_ready().await?;
                            self.state = RowStreamState::Error;
                            return Err(PgError::Server(err));
                        }
                        // Intercept async messages
                        BackendMessage::NotificationResponse { process_id, channel, payload } => {
                            self.conn.notification_queue.push_back(Notification {
                                process_id, channel, payload,
                            });
                            continue;
                        }
                        BackendMessage::NoticeResponse { .. } => {
                            continue;
                        }
                        BackendMessage::ParameterStatus { name, value } => {
                            self.conn.server_params.params.insert(name, value);
                            continue;
                        }
                        _ => continue,
                    }
                }

                RowStreamState::Finishing { .. } => {
                    let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
                    match msg {
                        BackendMessage::ReadyForQuery { transaction_status } => {
                            self.conn.transaction_status = transaction_status;
                            self.state = RowStreamState::Done;
                            return Ok(None);
                        }
                        // Intercept async messages before ReadyForQuery
                        BackendMessage::NotificationResponse { process_id, channel, payload } => {
                            self.conn.notification_queue.push_back(Notification {
                                process_id, channel, payload,
                            });
                            continue;
                        }
                        BackendMessage::ParameterStatus { name, value } => {
                            self.conn.server_params.params.insert(name, value);
                            continue;
                        }
                        _ => continue,
                    }
                }
            }
        }
    }

    /// Get the column metadata for the current result set.
    ///
    /// Returns `None` before the first row is received (RowDescription not yet read).
    /// Returns `Some(&[FieldDescription])` after RowDescription is received.
    pub fn columns(&self) -> Option<&[FieldDescription]> {
        self.columns.as_ref().map(|c| c.as_slice())
    }

    /// Get the command tag (e.g., "SELECT 100", "INSERT 0 1") after the stream ends.
    ///
    /// Returns `None` while rows are still being received.
    /// Returns `Some` after `next()` returns `None`.
    pub fn command_tag(&self) -> Option<&CommandTag> {
        match &self.state {
            RowStreamState::Finishing { command_tag } => Some(command_tag),
            RowStreamState::Done => None, // tag was already consumed
            _ => None,
        }
    }

    /// Returns true if the stream has been fully consumed or encountered an error.
    pub fn is_done(&self) -> bool {
        matches!(self.state, RowStreamState::Done | RowStreamState::Error)
    }

    /// Consume the remaining rows in the stream, discarding them.
    ///
    /// This is useful when you want to stop processing early but need to
    /// leave the connection in a clean state (ReadyForQuery consumed).
    /// Without calling this, dropping the stream mid-iteration may leave
    /// the connection in an inconsistent state.
    pub async fn consume(mut self) -> Result<CommandTag, PgError> {
        let mut tag = CommandTag::default();
        while let Some(_) = self.next().await? {
            // discard
        }
        // Extract the command tag from the finishing state
        if let RowStreamState::Finishing { command_tag } = &self.state {
            tag = command_tag.clone();
        }
        Ok(tag)
    }
}
```

### 14.3 - Drop behavior for `RowStream`

```rust
impl<'a> Drop for RowStream<'a> {
    fn drop(&mut self) {
        if !self.is_done() {
            // The stream was dropped before being fully consumed.
            // The connection may have unread DataRow / CommandComplete / ReadyForQuery
            // messages in its buffer. This leaves the connection in an inconsistent
            // state — the next operation will likely fail.
            //
            // We cannot fix this in Drop because Drop is not async.
            // Options for the user:
            //   1. Always consume the stream fully (call .next() until None)
            //   2. Call .consume().await before dropping
            //   3. After an incomplete drop, call conn.read_until_ready().await
            //      to recover the connection state
            //
            // We set a flag so the connection knows it needs recovery.
            self.conn.needs_recovery = true;

            #[cfg(feature = "tracing")]
            tracing::warn!(
                "RowStream dropped before being fully consumed. \
                 Connection may be in an inconsistent state. \
                 Call conn.recover() or conn.read_until_ready() to fix."
            );
        }
    }
}
```

### 14.4 - Connection recovery after incomplete stream consumption

```rust
impl Connection {
    /// Whether the connection needs recovery (e.g., a RowStream was dropped
    /// before being fully consumed).
    pub fn needs_recovery(&self) -> bool {
        self.needs_recovery
    }

    /// Recover the connection after an incomplete stream consumption.
    /// Reads messages until ReadyForQuery is received, discarding everything.
    pub async fn recover(&mut self) -> Result<(), PgError> {
        if self.needs_recovery {
            self.read_until_ready().await?;
            self.needs_recovery = false;
        }
        Ok(())
    }
}
```

### 14.5 - Connection methods that return `RowStream`

```rust
impl Connection {
    /// Execute a simple query and return a stream of rows.
    ///
    /// This is the primary streaming API. Rows are fetched from the server
    /// one at a time as the consumer calls `next()` on the returned stream.
    ///
    /// # Example
    ///
    /// ```rust
    /// let mut stream = conn.query_stream("SELECT id, name FROM users").await?;
    /// while let Some(row) = stream.next().await? {
    ///     let id: i32 = row.get(0)?;
    ///     let name: String = row.get(1)?;
    /// }
    /// ```
    pub async fn query_stream(&mut self, sql: &str) -> Result<RowStream<'_>, PgError> {
        // Ensure connection is in a clean state
        if self.needs_recovery {
            self.recover().await?;
        }

        self.codec.send(&mut self.transport, &FrontendMessage::Query {
            sql: sql.to_string(),
        }).await?;

        Ok(RowStream {
            conn: self,
            columns: None,
            state: RowStreamState::WaitingForDescription,
            extended_protocol: false,
        })
    }

    /// Execute a parameterized query and return a stream of rows.
    ///
    /// Uses the extended query protocol (Parse + Bind + Execute + Sync).
    /// Parameters are binary-encoded, preventing SQL injection.
    ///
    /// # Example
    ///
    /// ```rust
    /// let mut stream = conn.query_params_stream(
    ///     "SELECT id, name FROM users WHERE age > $1",
    ///     &[&18i32],
    /// ).await?;
    /// while let Some(row) = stream.next().await? {
    ///     let id: i32 = row.get(0)?;
    ///     let name: String = row.get(1)?;
    /// }
    /// ```
    pub async fn query_params_stream(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<RowStream<'_>, PgError> {
        if self.needs_recovery {
            self.recover().await?;
        }

        let param_values = encode_params(params, &[])?;

        // Parse (unnamed statement)
        self.codec.send_no_flush(&mut self.transport, &FrontendMessage::Parse {
            name: String::new(),
            sql: sql.to_string(),
            param_types: vec![],
        }).await?;

        // Bind (unnamed portal)
        self.codec.send_no_flush(&mut self.transport, &FrontendMessage::Bind {
            portal: String::new(),
            statement: String::new(),
            param_formats: vec![FormatCode::Binary],
            params: param_values,
            result_formats: vec![FormatCode::Binary],
        }).await?;

        // Describe portal (to get column metadata)
        self.codec.send_no_flush(&mut self.transport, &FrontendMessage::Describe {
            variant: DescribeVariant::Portal,
            name: String::new(),
        }).await?;

        // Execute
        self.codec.send_no_flush(&mut self.transport, &FrontendMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        }).await?;

        // Sync (flush + get ReadyForQuery at the end)
        self.codec.send(&mut self.transport, &FrontendMessage::Sync).await?;

        Ok(RowStream {
            conn: self,
            columns: None,
            state: RowStreamState::WaitingForDescription,
            extended_protocol: true,
        })
    }

    /// Execute a prepared statement and return a stream of rows.
    pub async fn query_prepared_stream(
        &mut self,
        stmt: &PreparedStatement,
        params: &[&dyn ToSql],
    ) -> Result<RowStream<'_>, PgError> {
        if self.needs_recovery {
            self.recover().await?;
        }

        let param_values = encode_params(params, &stmt.param_types)?;

        // Bind
        self.codec.send_no_flush(&mut self.transport, &FrontendMessage::Bind {
            portal: String::new(),
            statement: stmt.name.clone(),
            param_formats: vec![FormatCode::Binary],
            params: param_values,
            result_formats: vec![FormatCode::Binary],
        }).await?;

        // Execute
        self.codec.send_no_flush(&mut self.transport, &FrontendMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        }).await?;

        // Sync
        self.codec.send(&mut self.transport, &FrontendMessage::Sync).await?;

        // Use the prepared statement's column metadata
        Ok(RowStream {
            conn: self,
            columns: Some(stmt.columns.clone()),
            state: RowStreamState::WaitingForDescription,
            extended_protocol: true,
        })
    }
}
```

### 14.6 - Cursor-based streaming (fetch-size support)

For very large result sets, the extended query protocol supports portal-based cursors
with a `max_rows` limit. This allows fetching rows in batches:

```rust
/// A cursor that fetches rows in batches from a portal.
///
/// Unlike `RowStream` which fetches all rows at once (max_rows=0),
/// `CursorStream` fetches `fetch_size` rows at a time, issuing new
/// Execute messages as needed. This provides better backpressure control
/// and lower memory usage for very large results.
///
/// # Example
///
/// ```rust
/// let mut cursor = conn.cursor(
///     "SELECT * FROM large_table",
///     &[],
///     1000,  // fetch 1000 rows at a time
/// ).await?;
/// while let Some(row) = cursor.next().await? {
///     // Process each row; only 1000 rows are buffered server-side at a time
/// }
/// ```
pub struct CursorStream<'a> {
    conn: &'a mut Connection,
    portal_name: String,
    columns: Arc<Vec<FieldDescription>>,
    fetch_size: i32,
    rows_remaining_in_batch: i32,
    done: bool,
}

impl<'a> CursorStream<'a> {
    /// Fetch the next row from the cursor.
    ///
    /// When the current batch is exhausted, automatically issues another
    /// Execute message to fetch the next batch.
    pub async fn next(&mut self) -> Result<Option<Row>, PgError> {
        if self.done {
            return Ok(None);
        }

        loop {
            // If we've consumed all rows in the current batch, fetch more
            if self.rows_remaining_in_batch == 0 {
                // Check if the previous Execute ended with PortalSuspended or CommandComplete
                let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
                match msg {
                    BackendMessage::PortalSuspended => {
                        // More rows available — issue another Execute
                        self.conn.codec.send_no_flush(
                            &mut self.conn.transport,
                            &FrontendMessage::Execute {
                                portal: self.portal_name.clone(),
                                max_rows: self.fetch_size,
                            },
                        ).await?;
                        self.conn.codec.send(
                            &mut self.conn.transport,
                            &FrontendMessage::Sync,
                        ).await?;
                        self.rows_remaining_in_batch = self.fetch_size;
                        continue;
                    }
                    BackendMessage::CommandComplete { .. } => {
                        // All rows received — read ReadyForQuery
                        self.read_until_ready().await?;
                        self.done = true;
                        return Ok(None);
                    }
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
                    // Intercept async messages
                    BackendMessage::NotificationResponse { process_id, channel, payload } => {
                        self.conn.notification_queue.push_back(Notification {
                            process_id, channel, payload,
                        });
                        continue;
                    }
                    _ => continue,
                }
            }

            // Read the next message (should be a DataRow)
            let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
            match msg {
                BackendMessage::DataRow { values } => {
                    self.rows_remaining_in_batch -= 1;
                    return Ok(Some(Row {
                        columns: self.columns.clone(),
                        values,
                    }));
                }
                BackendMessage::CommandComplete { .. } => {
                    // All rows received (fewer than fetch_size remaining)
                    self.rows_remaining_in_batch = 0;
                    continue; // Will be handled in the next iteration
                }
                BackendMessage::PortalSuspended => {
                    // Batch exhausted, more rows available
                    self.rows_remaining_in_batch = 0;
                    continue;
                }
                BackendMessage::ErrorResponse { fields } => {
                    self.conn.read_until_ready().await?;
                    self.done = true;
                    return Err(PgError::Server(PgServerError::from_fields(fields)));
                }
                BackendMessage::NotificationResponse { process_id, channel, payload } => {
                    self.conn.notification_queue.push_back(Notification {
                        process_id, channel, payload,
                    });
                    continue;
                }
                BackendMessage::NoticeResponse { .. } => continue,
                BackendMessage::ParameterStatus { name, value } => {
                    self.conn.server_params.params.insert(name, value);
                    continue;
                }
                _ => continue,
            }
        }
    }

    /// Close the cursor, releasing the portal.
    pub async fn close(mut self) -> Result<(), PgError> {
        if !self.done {
            // Close the portal
            self.conn.codec.send_no_flush(
                &mut self.conn.transport,
                &FrontendMessage::Close {
                    variant: CloseVariant::Portal,
                    name: self.portal_name.clone(),
                },
            ).await?;
            self.conn.codec.send(
                &mut self.conn.transport,
                &FrontendMessage::Sync,
            ).await?;
            self.conn.read_until_ready().await?;
        }
        self.done = true;
        Ok(())
    }

    async fn read_until_ready(&mut self) -> Result<(), PgError> {
        loop {
            let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
            match msg {
                BackendMessage::ReadyForQuery { transaction_status } => {
                    self.conn.transaction_status = transaction_status;
                    return Ok(());
                }
                BackendMessage::NotificationResponse { process_id, channel, payload } => {
                    self.conn.notification_queue.push_back(Notification {
                        process_id, channel, payload,
                    });
                }
                _ => {}
            }
        }
    }
}

impl<'a> Drop for CursorStream<'a> {
    fn drop(&mut self) {
        if !self.done {
            self.conn.needs_recovery = true;
            #[cfg(feature = "tracing")]
            tracing::warn!(
                "CursorStream dropped without being fully consumed or closed. \
                 Connection may need recovery."
            );
        }
    }
}
```

### 14.7 - Connection method for cursor streaming

```rust
impl Connection {
    /// Execute a query with cursor-based streaming.
    ///
    /// Unlike `query_stream()` which fetches all rows at once, this creates
    /// a named portal with the given `fetch_size` and fetches rows in batches.
    ///
    /// The `fetch_size` parameter controls how many rows are fetched per
    /// network round-trip. A larger value means fewer round-trips but more
    /// memory usage on the server side. A typical value is 100–10000.
    ///
    /// # Example
    ///
    /// ```rust
    /// let mut cursor = conn.cursor(
    ///     "SELECT * FROM large_table WHERE category = $1",
    ///     &[&"electronics"],
    ///     1000,
    /// ).await?;
    /// while let Some(row) = cursor.next().await? {
    ///     // Process row
    /// }
    /// ```
    pub async fn cursor(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
        fetch_size: i32,
    ) -> Result<CursorStream<'_>, PgError> {
        if self.needs_recovery {
            self.recover().await?;
        }

        let portal_name = self.next_portal_name();
        let param_values = encode_params(params, &[])?;

        // Parse (named statement for potential reuse)
        let stmt_name = self.next_statement_name();
        self.codec.send_no_flush(&mut self.transport, &FrontendMessage::Parse {
            name: stmt_name.clone(),
            sql: sql.to_string(),
            param_types: vec![],
        }).await?;

        // Bind (named portal)
        self.codec.send_no_flush(&mut self.transport, &FrontendMessage::Bind {
            portal: portal_name.clone(),
            statement: stmt_name,
            param_formats: vec![FormatCode::Binary],
            params: param_values,
            result_formats: vec![FormatCode::Binary],
        }).await?;

        // Describe portal (to get column metadata)
        self.codec.send_no_flush(&mut self.transport, &FrontendMessage::Describe {
            variant: DescribeVariant::Portal,
            name: portal_name.clone(),
        }).await?;

        // Execute with fetch_size
        self.codec.send_no_flush(&mut self.transport, &FrontendMessage::Execute {
            portal: portal_name.clone(),
            max_rows: fetch_size,
        }).await?;

        // Sync
        self.codec.send(&mut self.transport, &FrontendMessage::Sync).await?;

        // Read ParseComplete, BindComplete, RowDescription (or NoData)
        let mut columns = None;
        loop {
            let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription { fields } => {
                    columns = Some(Arc::new(fields));
                    break;
                }
                BackendMessage::NoData => {
                    // Statement doesn't return rows
                    break;
                }
                BackendMessage::ErrorResponse { fields } => {
                    self.conn.read_until_ready().await?;
                    return Err(PgError::Server(PgServerError::from_fields(fields)));
                }
                BackendMessage::ReadyForQuery { .. } => break,
                _ => {}
            }
        }

        let columns = columns.unwrap_or_default();

        Ok(CursorStream {
            conn: self,
            portal_name,
            columns,
            fetch_size,
            rows_remaining_in_batch: fetch_size,
            done: false,
        })
    }

    /// Generate a unique portal name.
    fn next_portal_name(&mut self) -> String {
        self.portal_counter += 1;
        format!("_pg_portal_{}", self.portal_counter)
    }

    /// Generate a unique statement name.
    fn next_statement_name(&mut self) -> String {
        self.statement_counter += 1;
        format!("_pg_stmt_{}", self.statement_counter)
    }
}
```

### 14.8 - Rebuild convenience methods on top of `RowStream`

The existing `query()`, `query_one()`, `query_each()` methods should be reimplemented
using `RowStream` internally. This ensures consistent behavior and reduces code duplication:

```rust
impl Connection {
    /// Execute a query that returns rows, collecting all results into a Vec.
    ///
    /// This is a convenience method built on top of `query_stream()`.
    /// For large result sets, prefer `query_stream()` to avoid buffering
    /// all rows in memory.
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult, PgError> {
        let mut stream = self.query_stream(sql).await?;
        let mut rows = Vec::new();

        while let Some(row) = stream.next().await? {
            rows.push(row);
        }

        Ok(QueryResult {
            columns: stream.columns.unwrap_or_default(),
            rows,
            command_tag: CommandTag::default(), // extracted from stream state
        })
    }

    /// Execute a parameterized query, collecting all results into a Vec.
    ///
    /// Convenience method built on top of `query_params_stream()`.
    pub async fn query_params(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<QueryResult, PgError> {
        let mut stream = self.query_params_stream(sql, params).await?;
        let mut rows = Vec::new();

        while let Some(row) = stream.next().await? {
            rows.push(row);
        }

        Ok(QueryResult {
            columns: stream.columns.unwrap_or_default(),
            rows,
            command_tag: CommandTag::default(),
        })
    }

    /// Execute a query and return the first row, or None.
    ///
    /// This only reads the first row from the stream, then consumes
    /// the rest (discarding them) to leave the connection in a clean state.
    pub async fn query_one(&mut self, sql: &str) -> Result<Option<Row>, PgError> {
        let mut stream = self.query_stream(sql).await?;
        match stream.next().await? {
            Some(row) => {
                // Consume the rest of the stream to clean up the connection
                stream.consume().await?;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    /// Execute a query and process rows with a callback (streaming).
    ///
    /// This is more memory-efficient than `query()` for large result sets
    /// because rows are processed one at a time without buffering.
    pub async fn query_each<F>(&mut self, sql: &str, mut f: F) -> Result<CommandTag, PgError>
    where
        F: FnMut(Row) -> Result<(), PgError>,
    {
        let mut stream = self.query_stream(sql).await?;
        while let Some(row) = stream.next().await? {
            f(row)?;
        }
        // CommandTag is embedded in the stream state
        Ok(CommandTag::default())
    }

    /// Execute a query and process rows with an async callback (streaming).
    ///
    /// Like `query_each()` but the callback is async, allowing async operations
    /// (e.g., writing to another connection) per row.
    pub async fn query_each_async<F, Fut>(&mut self, sql: &str, mut f: F) -> Result<CommandTag, PgError>
    where
        F: FnMut(Row) -> Fut,
        Fut: Future<Output = Result<(), PgError>>,
    {
        let mut stream = self.query_stream(sql).await?;
        while let Some(row) = stream.next().await? {
            f(row).await?;
        }
        Ok(CommandTag::default())
    }
}
```

### 14.9 - Optional `futures::Stream` implementation

For users who want to compose with `futures::Stream` combinators (`map`, `filter`, `take`, etc.),
we provide an optional `Stream` implementation behind a feature flag:

```toml
[features]
stream = ["dep:futures-core"]
```

```rust
#[cfg(feature = "stream")]
impl<'a> futures_core::Stream for RowStream<'a> {
    type Item = Result<Row, PgError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // This requires a custom executor that can poll the underlying async read.
        // Since we're using wstd (not tokio), we need to bridge the polling model.
        //
        // Implementation approach:
        // 1. Try to read a message from the codec's read buffer (sync, no I/O)
        // 2. If buffer is empty, we need to do async I/O — return Pending
        //    and schedule a wakeup via wasi:io/poll
        //
        // This is complex because our `next()` method is `async fn` which
        // doesn't directly expose a `poll` interface. We have two options:
        //
        // Option A: Store the `next()` future and poll it.
        //   - Requires `unsafe` Pin projection
        //   - The future borrows `self` mutably, creating a self-referential struct
        //   - This is the standard approach but requires careful unsafe code
        //
        // Option B: Refactor `next()` into a `poll_next()` method.
        //   - More code but no unsafe
        //   - The `async fn next()` becomes a wrapper around `poll_next()`
        //
        // We choose Option B for safety and WASI compatibility.

        match self.poll_next_inner(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => {
                // Register wakeup via wasi:io/poll when data is available
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
```

> **Note**: The `futures::Stream` implementation is complex because it requires bridging
> between `wstd`'s async model (driven by `wasi:io/poll`) and `futures`' polling model.
> For v0.1, the `async fn next()` API is the primary interface. The `Stream` implementation
> is a nice-to-have that can be added later once the core streaming works reliably.

### 14.10 - Streaming for batch queries (multiple result sets)

The simple query protocol can return multiple result sets (from multiple statements
separated by `;`). The streaming API should handle this:

```rust
/// A single result set within a batch query.
pub struct ResultSet<'a> {
    /// The stream for this result set.
    stream: RowStream<'a>,
}

/// Batch query result that yields multiple result sets.
pub struct BatchStream<'a> {
    conn: &'a mut Connection,
    done: bool,
}

impl<'a> BatchStream<'a> {
    /// Get the next result set in the batch.
    ///
    /// Returns `Some(RowStream)` for each statement in the batch,
    /// or `None` when all result sets have been consumed.
    pub async fn next_result_set(&mut self) -> Result<Option<RowStream<'_>>, PgError> {
        if self.done {
            return Ok(None);
        }

        let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
        match msg {
            BackendMessage::RowDescription { .. } => {
                // New result set starting
                // Put the message back... or handle differently
                todo!("implement batch result set streaming")
            }
            BackendMessage::ReadyForQuery { transaction_status } => {
                self.conn.transaction_status = transaction_status;
                self.done = true;
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}
```

> **Design note**: Batch query streaming is more complex because each `RowStream` borrows
> the connection, but we need to transition between result sets. A simpler approach is to
> provide `batch_execute()` (which collects everything) and discourage batch queries with
> streaming. Users who need multiple queries should use separate `query_stream()` calls.

## File Layout

```
crates/pg-client/src/
├── query/
│   ├── mod.rs              (query, execute, query_one — built on stream)
│   ├── row.rs              (Row, column access)
│   ├── result.rs           (QueryResult, ExecuteResult)
│   ├── stream.rs           (RowStream, RowStreamState)
│   ├── cursor_stream.rs    (CursorStream — fetch-size based)
│   ├── prepared.rs         (PreparedStatement, prepare/close)
│   ├── params.rs           (parameter encoding, query_params)
│   ├── pipeline.rs         (Pipeline)
│   └── cache.rs            (StatementCache)
```

## Acceptance Criteria

- [ ] `query_stream()` returns a `RowStream` that yields rows one at a time
- [ ] `query_params_stream()` returns a `RowStream` for parameterized queries
- [ ] `query_prepared_stream()` returns a `RowStream` for prepared statements
- [ ] `RowStream::next()` returns `Ok(Some(row))`, `Ok(None)`, or `Err`
- [ ] `RowStream::consume()` discards remaining rows and cleans up connection
- [ ] `RowStream` Drop sets `needs_recovery` flag if not fully consumed
- [ ] `Connection::recover()` reads until ReadyForQuery to fix inconsistent state
- [ ] `cursor()` returns a `CursorStream` with fetch-size-based batching
- [ ] `CursorStream::next()` automatically fetches next batch when current batch is exhausted
- [ ] `CursorStream::close()` releases the portal
- [ ] Convenience methods (`query`, `query_one`, `query_each`) are built on top of `RowStream`
- [ ] `query_each_async()` supports async callbacks per row
- [ ] Async messages (notifications, notices, parameter status) are intercepted during streaming
- [ ] Memory usage is O(1) per row (no buffering of all rows)
- [ ] Backpressure: server only sends rows when client reads them
- [ ] Compiles for `wasm32-wasip2`

## Key Design Decisions

1. **Streaming is primary, collecting is convenience**: The `RowStream` API is the foundation. `query()` → `Vec<Row>` is sugar on top. This ensures the library is memory-efficient by default.

2. **`RowStream` borrows `Connection` mutably**: This prevents using the connection while iterating, which is correct — the connection is busy reading query results. When the stream is dropped/consumed, the borrow is released.

3. **`needs_recovery` flag**: Since `Drop` can't be async, we can't clean up the connection when a `RowStream` is dropped early. Instead, we set a flag and require the user to call `recover()`. This is explicit and avoids silent state corruption.

4. **No `futures::Stream` by default**: The `async fn next()` API is simpler and works reliably on WASI. The `Stream` trait implementation is optional and can be added later.

5. **Cursor streaming for very large results**: For results that don't fit in server memory either, `CursorStream` uses `max_rows` to fetch in batches. This is the PostgreSQL-native way to do cursor-based pagination.

## Testing

- **Unit test**: `RowStream` with mock transport — verify rows are yielded one at a time
- **Unit test**: `RowStream` Drop sets `needs_recovery` flag
- **Unit test**: `Connection::recover()` reads until ReadyForQuery
- **Unit test**: `RowStream::consume()` discards remaining rows
- **Integration test**: Stream 10k rows, verify memory doesn't grow linearly
- **Integration test**: Stream with early termination (drop after 100 rows), then recover
- **Integration test**: `cursor()` with fetch_size=100, verify batch fetching
- **Integration test**: `CursorStream::close()` releases portal
- **Integration test**: `query_each_async()` with async callback per row
- **Integration test**: Notifications arrive during streaming and are buffered
- **Integration test**: Error mid-stream (connection drop), verify error propagation
- **WASI E2E test**: Stream rows from a WASI component
