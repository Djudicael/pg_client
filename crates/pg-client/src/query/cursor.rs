//! Cursor support for fetching large result sets in batches.
//!
//! A [`Cursor`] executes a portal with a limited `max_rows` count and
//! provides `fetch_next()` to retrieve subsequent batches until the result
//! set is exhausted.
//!
//! A [`CursorStream`] wraps a cursor and yields rows one at a time,
//! automatically fetching the next batch when the current batch is exhausted.

use std::sync::Arc;

use pg_protocol::{BackendMessage, FrontendMessage, TransactionStatus};

use crate::connection::{Connection, ConnectionState};
use crate::error::{PgError, PgServerError, Result};
use crate::query::params::encode_params_text;
use crate::query::result::CommandTag;
use crate::query::row::{FieldDescription, Row};
use crate::query::{read_data_row, read_row_description};
use crate::transport::AsyncTransport;

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

/// A cursor for fetching a large result set in batches.
///
/// Created via [`Connection::query_cursor`]. Each call to [`Cursor::fetch_next`]
/// returns the next batch of rows. The cursor is automatically closed when
/// dropped, but explicit [`Cursor::close`] is recommended for clean shutdown.
pub struct Cursor<'a> {
    conn: &'a mut Connection,
    portal_name: String,
    columns: Arc<Vec<FieldDescription>>,
    fetch_size: i32,
    done: bool,
    /// Whether this cursor started the transaction and should commit on close.
    owns_transaction: bool,
}

impl<'a> Cursor<'a> {
    /// Fetch the next batch of rows.
    ///
    /// Returns an empty vector when all rows have been consumed.
    pub async fn fetch_next(&mut self) -> Result<Vec<Row>> {
        if self.done {
            return Ok(Vec::new());
        }

        self.conn.transition(ConnectionState::ActiveExtendedQuery)?;

        // Execute portal with limited max_rows
        self.conn
            .codec
            .encode_and_write(
                &mut self.conn.transport,
                &FrontendMessage::Execute {
                    portal: self.portal_name.clone(),
                    max_rows: self.fetch_size,
                },
            )
            .await?;

        self.conn
            .codec
            .encode_and_write(&mut self.conn.transport, &FrontendMessage::Sync)
            .await?;

        // Flush the batch
        self.conn
            .transport
            .flush()
            .await
            .map_err(PgError::Transport)?;

        let mut rows = Vec::new();

        loop {
            let msg = self
                .conn
                .codec
                .read_message(&mut self.conn.transport)
                .await?;
            if self.conn.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::RowDescription(body) => {
                    self.columns = Arc::new(read_row_description(body)?);
                }
                BackendMessage::DataRow(body) => {
                    let values = read_data_row(body)?;
                    rows.push(Row::new(self.columns.clone(), values));
                }
                BackendMessage::CommandComplete(_body) => {
                    self.done = true;
                }
                BackendMessage::PortalSuspended => {
                    // More rows available; portal remains open
                }
                BackendMessage::ReadyForQuery(body) => {
                    self.conn.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    self.conn.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.conn.read_until_ready().await?;
                    self.conn.state = ConnectionState::Idle;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                _ => {}
            }
        }

        Ok(rows)
    }

    /// Close the cursor, releasing the portal on the server.
    ///
    /// If the cursor automatically started a transaction (because no
    /// transaction was active when the cursor was created), the transaction
    /// is committed.
    pub async fn close(mut self) -> Result<()> {
        self.conn.transition(ConnectionState::ActiveExtendedQuery)?;

        self.conn
            .codec
            .encode_and_write(
                &mut self.conn.transport,
                &FrontendMessage::Close {
                    variant: b'P',
                    name: self.portal_name.clone(),
                },
            )
            .await?;

        self.conn
            .codec
            .encode_and_write(&mut self.conn.transport, &FrontendMessage::Sync)
            .await?;

        // Flush the batch
        self.conn
            .transport
            .flush()
            .await
            .map_err(PgError::Transport)?;

        loop {
            let msg = self
                .conn
                .codec
                .read_message(&mut self.conn.transport)
                .await?;
            if self.conn.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::CloseComplete => {}
                BackendMessage::ReadyForQuery(body) => {
                    self.conn.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    self.conn.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.conn.read_until_ready().await?;
                    self.conn.state = ConnectionState::Idle;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                _ => {}
            }
        }

        // Commit the transaction if we started it
        if self.owns_transaction {
            self.conn.execute("COMMIT").await?;
        }

        self.done = true;
        Ok(())
    }

    /// Returns true if all rows have been fetched.
    pub fn is_done(&self) -> bool {
        self.done
    }
}

// ---------------------------------------------------------------------------
// CursorStream
// ---------------------------------------------------------------------------

/// Internal state of the cursor stream.
#[derive(Debug)]
enum CursorStreamState {
    /// Rows are available in the buffer or more can be fetched.
    Active,
    /// All rows have been consumed and the cursor is closed.
    Done { command_tag: CommandTag },
    /// An error occurred.
    Error,
}

/// A streaming cursor that yields rows one at a time from a portal.
///
/// Unlike [`Cursor`] which returns batches of rows, `CursorStream` provides
/// a row-by-row iterator interface. When the current batch is exhausted,
/// it automatically fetches the next batch from the server.
///
/// `CursorStream` borrows the connection mutably. You cannot use the
/// connection while iterating. When the stream is dropped (or fully consumed),
/// the connection is available again.
///
/// If the stream is dropped before being fully consumed, the connection is
/// left in an inconsistent state. The [`Connection::needs_recovery`] flag is
/// set, and you must call [`Connection::recover`] before using the connection
/// again.
pub struct CursorStream<'a> {
    conn: &'a mut Connection,
    portal_name: String,
    columns: Arc<Vec<FieldDescription>>,
    fetch_size: i32,
    state: CursorStreamState,
    /// Rows buffered from the current batch, waiting to be yielded.
    buffered_rows: Vec<Row>,
    /// Whether this cursor started the transaction and should commit on close.
    owns_transaction: bool,
}

impl<'a> CursorStream<'a> {
    /// Create a new `CursorStream` from an already-set-up portal.
    pub(crate) fn new(
        conn: &'a mut Connection,
        portal_name: String,
        columns: Arc<Vec<FieldDescription>>,
        fetch_size: i32,
        owns_transaction: bool,
    ) -> Self {
        CursorStream {
            conn,
            portal_name,
            columns,
            fetch_size,
            state: CursorStreamState::Active,
            buffered_rows: Vec::new(),
            owns_transaction,
        }
    }

    /// Fetch the next row from the stream.
    ///
    /// Returns `Ok(Some(row))` when a row is available, `Ok(None)` when the
    /// stream is exhausted, and `Err(...)` on a server or protocol error.
    /// After returning `None` or `Err`, subsequent calls return `None`.
    pub async fn next(&mut self) -> Result<Option<Row>> {
        loop {
            match self.state {
                CursorStreamState::Done { .. } | CursorStreamState::Error => {
                    return Ok(None);
                }

                CursorStreamState::Active => {
                    // First, yield from the buffer if we have rows from a previous fetch
                    if let Some(row) = self.buffered_rows.pop() {
                        return Ok(Some(row));
                    }

                    // Buffer is empty — fetch the next batch from the server
                    self.conn.transition(ConnectionState::ActiveExtendedQuery)?;

                    self.conn
                        .codec
                        .encode_and_write(
                            &mut self.conn.transport,
                            &FrontendMessage::Execute {
                                portal: self.portal_name.clone(),
                                max_rows: self.fetch_size,
                            },
                        )
                        .await?;

                    self.conn
                        .codec
                        .encode_and_write(&mut self.conn.transport, &FrontendMessage::Sync)
                        .await?;

                    self.conn
                        .transport
                        .flush()
                        .await
                        .map_err(PgError::Transport)?;

                    // Read the batch response
                    let mut command_tag: Option<CommandTag> = None;
                    let mut rows: Vec<Row> = Vec::new();

                    loop {
                        let msg = self
                            .conn
                            .codec
                            .read_message(&mut self.conn.transport)
                            .await?;
                        if self.conn.handle_async_message(&msg) {
                            continue;
                        }
                        match msg {
                            BackendMessage::RowDescription(body) => {
                                self.columns = Arc::new(read_row_description(body)?);
                            }
                            BackendMessage::DataRow(body) => {
                                let values = read_data_row(body)?;
                                rows.push(Row::new(self.columns.clone(), values));
                            }
                            BackendMessage::CommandComplete(body) => {
                                command_tag =
                                    Some(CommandTag::new(body.tag().unwrap_or("").into()));
                            }
                            BackendMessage::PortalSuspended => {
                                // More rows available; portal remains open
                            }
                            BackendMessage::ReadyForQuery(body) => {
                                self.conn.transaction_status =
                                    TransactionStatus::from_u8(body.status())
                                        .unwrap_or(TransactionStatus::Idle);
                                self.conn.state = ConnectionState::Idle;
                                break;
                            }
                            BackendMessage::ErrorResponse(body) => {
                                let server_err =
                                    PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                                self.conn.read_until_ready().await?;
                                self.conn.state = ConnectionState::Idle;
                                self.state = CursorStreamState::Error;
                                return Err(PgError::Server(Box::new(server_err)));
                            }
                            _ => {}
                        }
                    }

                    // If CommandComplete was received, all rows are done
                    if let Some(tag) = command_tag {
                        self.state = CursorStreamState::Done { command_tag: tag };
                    }

                    // If no rows were returned and we're not done, loop to fetch again
                    // (this can happen with PortalSuspended when fetch_size rows were
                    // already consumed in a previous batch)
                    if rows.is_empty() && !self.is_done() {
                        continue;
                    }

                    // Reverse the buffer so we can pop() from the front efficiently
                    rows.reverse();
                    self.buffered_rows = rows;

                    // Yield the first row from the buffer
                    if let Some(row) = self.buffered_rows.pop() {
                        return Ok(Some(row));
                    }

                    // No rows and done
                    return Ok(None);
                }
            }
        }
    }

    /// Get the column metadata for the current result set.
    pub fn columns(&self) -> &[FieldDescription] {
        &self.columns
    }

    /// Returns true if the stream has been fully consumed or encountered an error.
    pub fn is_done(&self) -> bool {
        matches!(
            self.state,
            CursorStreamState::Done { .. } | CursorStreamState::Error
        )
    }

    /// Get the command tag after the stream ends.
    pub fn command_tag(&self) -> Option<&CommandTag> {
        match &self.state {
            CursorStreamState::Done { command_tag } => Some(command_tag),
            _ => None,
        }
    }

    /// Consume the remaining rows in the stream, discarding them, and close
    /// the cursor portal.
    pub async fn consume(mut self) -> Result<CommandTag> {
        while self.next().await?.is_some() {}
        self.close_portal().await?;
        match &self.state {
            CursorStreamState::Done { command_tag } => Ok(command_tag.clone()),
            _ => Ok(CommandTag::default()),
        }
    }

    /// Close the portal on the server and commit the transaction if we own it.
    async fn close_portal(&mut self) -> Result<()> {
        if matches!(self.state, CursorStreamState::Done { .. }) {
            // Already done — just commit if needed
            if self.owns_transaction {
                self.conn.execute("COMMIT").await?;
            }
            return Ok(());
        }

        self.conn.transition(ConnectionState::ActiveExtendedQuery)?;

        self.conn
            .codec
            .encode_and_write(
                &mut self.conn.transport,
                &FrontendMessage::Close {
                    variant: b'P',
                    name: self.portal_name.clone(),
                },
            )
            .await?;

        self.conn
            .codec
            .encode_and_write(&mut self.conn.transport, &FrontendMessage::Sync)
            .await?;

        self.conn
            .transport
            .flush()
            .await
            .map_err(PgError::Transport)?;

        loop {
            let msg = self
                .conn
                .codec
                .read_message(&mut self.conn.transport)
                .await?;
            if self.conn.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::CloseComplete => {}
                BackendMessage::ReadyForQuery(body) => {
                    self.conn.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    self.conn.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.conn.read_until_ready().await?;
                    self.conn.state = ConnectionState::Idle;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                _ => {}
            }
        }

        // Commit the transaction if we started it
        if self.owns_transaction {
            self.conn.execute("COMMIT").await?;
        }

        self.state = CursorStreamState::Done {
            command_tag: CommandTag::default(),
        };
        Ok(())
    }
}

impl<'a> Drop for CursorStream<'a> {
    fn drop(&mut self) {
        if !self.is_done() {
            self.conn.needs_recovery = true;
        }
    }
}

// ---------------------------------------------------------------------------
// Connection method
// ---------------------------------------------------------------------------

impl Connection {
    /// Open a cursor for a parameterized query.
    ///
    /// The query is parsed and bound to a named portal. The first batch of
    /// rows is fetched via [`Cursor::fetch_next`].
    ///
    /// **Important:** Named portals only survive within a transaction
    /// block. If no transaction is active, this method automatically
    /// begins one so the portal remains valid across `fetch_next` calls.
    /// The transaction is committed when the cursor is closed.
    pub async fn query_cursor(
        &mut self,
        sql: &str,
        params: &[&dyn pg_types::ToSql],
        fetch_size: i32,
    ) -> Result<Cursor<'_>> {
        // Named portals only survive within a transaction. Start one if
        // we're not already in a transaction.
        let need_transaction = self.transaction_status == pg_protocol::TransactionStatus::Idle;
        if need_transaction {
            // Use simple query for BEGIN — it's a single statement with no params
            self.query("BEGIN").await?;
        }

        self.transition(ConnectionState::ActiveExtendedQuery)?;

        let param_values = encode_params_text(params)?;
        let portal_name = format!("__pg_portal_{}", self.statement_counter);
        self.statement_counter += 1;

        // Parse (unnamed statement)
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Parse {
                    name: String::new(),
                    sql: sql.to_string(),
                    param_types: vec![],
                },
            )
            .await?;

        // Bind (named portal)
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Bind {
                    portal: portal_name.clone(),
                    statement: String::new(),
                    param_formats: vec![pg_protocol::FormatCode::Text],
                    params: param_values,
                    result_formats: vec![pg_protocol::FormatCode::Binary],
                },
            )
            .await?;

        // Describe the named portal so the server sends RowDescription
        // (or NoData for non-SELECT statements).
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Describe {
                    variant: b'P',
                    name: portal_name.clone(),
                },
            )
            .await?;

        // Sync to complete the sub-protocol
        self.codec
            .encode_and_write(&mut self.transport, &FrontendMessage::Sync)
            .await?;

        // Flush the batch
        self.transport.flush().await.map_err(PgError::Transport)?;

        let mut columns: Option<Arc<Vec<FieldDescription>>> = None;

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            if self.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::NoData => {
                    // Non-SELECT query opened as cursor
                }
                BackendMessage::RowDescription(body) => {
                    columns = Some(Arc::new(read_row_description(body)?));
                }
                BackendMessage::ReadyForQuery(body) => {
                    self.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    self.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.read_until_ready().await?;
                    self.state = ConnectionState::Idle;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                _ => {}
            }
        }

        Ok(Cursor {
            conn: self,
            portal_name,
            columns: columns.unwrap_or_default(),
            fetch_size,
            done: false,
            owns_transaction: need_transaction,
        })
    }

    /// Open a streaming cursor for a parameterized query.
    ///
    /// Like [`Connection::query_cursor`], but returns a [`CursorStream`] that
    /// yields rows one at a time instead of in batches. When the current batch
    /// is exhausted, the next batch is automatically fetched from the server.
    ///
    /// **Important:** Named portals only survive within a transaction
    /// block. If no transaction is active, this method automatically
    /// begins one so the portal remains valid. The transaction is committed
    /// when the stream is fully consumed or closed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut stream = conn.query_cursor_stream(
    ///     "SELECT id, name FROM users WHERE active = $1",
    ///     &[&true],
    ///     100, // fetch 100 rows at a time
    /// ).await?;
    /// while let Some(row) = stream.next().await? {
    ///     let id: i32 = row.get(0)?;
    /// }
    /// ```
    pub async fn query_cursor_stream(
        &mut self,
        sql: &str,
        params: &[&dyn pg_types::ToSql],
        fetch_size: i32,
    ) -> Result<CursorStream<'_>> {
        // Named portals only survive within a transaction. Start one if
        // we're not already in a transaction.
        let need_transaction = self.transaction_status == pg_protocol::TransactionStatus::Idle;
        if need_transaction {
            self.query("BEGIN").await?;
        }

        self.transition(ConnectionState::ActiveExtendedQuery)?;

        let param_values = encode_params_text(params)?;
        let portal_name = format!("__pg_portal_{}", self.statement_counter);
        self.statement_counter += 1;

        // Parse (unnamed statement)
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Parse {
                    name: String::new(),
                    sql: sql.to_string(),
                    param_types: vec![],
                },
            )
            .await?;

        // Bind (named portal)
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Bind {
                    portal: portal_name.clone(),
                    statement: String::new(),
                    param_formats: vec![pg_protocol::FormatCode::Text],
                    params: param_values,
                    result_formats: vec![pg_protocol::FormatCode::Binary],
                },
            )
            .await?;

        // Describe the named portal so the server sends RowDescription
        // (or NoData for non-SELECT statements).
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Describe {
                    variant: b'P',
                    name: portal_name.clone(),
                },
            )
            .await?;

        // Sync to complete the sub-protocol
        self.codec
            .encode_and_write(&mut self.transport, &FrontendMessage::Sync)
            .await?;

        // Flush the batch
        self.transport.flush().await.map_err(PgError::Transport)?;

        let mut columns: Option<Arc<Vec<FieldDescription>>> = None;

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            if self.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::NoData => {
                    // Non-SELECT query opened as cursor
                }
                BackendMessage::RowDescription(body) => {
                    columns = Some(Arc::new(read_row_description(body)?));
                }
                BackendMessage::ReadyForQuery(body) => {
                    self.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    self.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.read_until_ready().await?;
                    self.state = ConnectionState::Idle;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                _ => {}
            }
        }

        Ok(CursorStream::new(
            self,
            portal_name,
            columns.unwrap_or_default(),
            fetch_size,
            need_transaction,
        ))
    }
}
