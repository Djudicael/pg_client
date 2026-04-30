//! Cursor support for fetching large result sets in batches.
//!
//! A [`Cursor`] executes a portal with a limited `max_rows` count and
//! provides `fetch_next()` to retrieve subsequent batches until the result
//! set is exhausted.

use std::sync::Arc;

use pg_protocol::{BackendMessage, FrontendMessage, TransactionStatus};

use crate::connection::{Connection, ConnectionState};
use crate::error::{Error, Result};
use crate::query::params::encode_params_text;
use crate::query::row::{FieldDescription, Row};
use crate::query::{format_error_fields, read_data_row, read_row_description};

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
}

impl<'a> Cursor<'a> {
    /// Fetch the next batch of rows.
    ///
    /// Returns an empty vector when all rows have been consumed.
    pub async fn fetch_next(&mut self) -> Result<Vec<Row>> {
        if self.done {
            return Ok(Vec::new());
        }

        self.conn
            .transition(ConnectionState::ActiveExtendedQuery)?;

        // Execute portal with limited max_rows
        self.conn
            .codec
            .send(
                &mut self.conn.transport,
                &FrontendMessage::Execute {
                    portal: self.portal_name.clone(),
                    max_rows: self.fetch_size,
                },
            )
            .await?;

        self.conn
            .codec
            .send(&mut self.conn.transport, &FrontendMessage::Sync)
            .await?;

        let mut rows = Vec::new();

        loop {
            let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
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
                    self.conn.transaction_status =
                        TransactionStatus::from_u8(body.status())
                            .unwrap_or(TransactionStatus::Idle);
                    self.conn.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let msg = format_error_fields(&body)?;
                    self.conn.read_until_ready().await?;
                    self.conn.state = ConnectionState::Idle;
                    return Err(Error::Server(msg));
                }
                BackendMessage::NoticeResponse(body) => {
                    if let Ok(notice) = crate::query::Notice::from_fields(&body) {
                        self.conn.handle_notice(&notice);
                    }
                }
                _ => {}
            }
        }

        Ok(rows)
    }

    /// Close the cursor, releasing the portal on the server.
    pub async fn close(mut self) -> Result<()> {
        self.conn
            .transition(ConnectionState::ActiveExtendedQuery)?;

        self.conn
            .codec
            .send(
                &mut self.conn.transport,
                &FrontendMessage::Close {
                    variant: b'P',
                    name: self.portal_name.clone(),
                },
            )
            .await?;

        self.conn
            .codec
            .send(&mut self.conn.transport, &FrontendMessage::Sync)
            .await?;

        loop {
            let msg = self.conn.codec.read_message(&mut self.conn.transport).await?;
            match msg {
                BackendMessage::CloseComplete => {}
                BackendMessage::ReadyForQuery(body) => {
                    self.conn.transaction_status =
                        TransactionStatus::from_u8(body.status())
                            .unwrap_or(TransactionStatus::Idle);
                    self.conn.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let msg = format_error_fields(&body)?;
                    self.conn.read_until_ready().await?;
                    self.conn.state = ConnectionState::Idle;
                    return Err(Error::Server(msg));
                }
                BackendMessage::NoticeResponse(body) => {
                    if let Ok(notice) = crate::query::Notice::from_fields(&body) {
                        self.conn.handle_notice(&notice);
                    }
                }
                _ => {}
            }
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
// Connection method
// ---------------------------------------------------------------------------

impl Connection {
    /// Open a cursor for a parameterized query.
    ///
    /// The query is parsed, bound to a named portal, and executed with
    /// `max_rows = 0` initially. Subsequent batches are fetched via
    /// [`Cursor::fetch_next`].
    pub async fn query_cursor(
        &mut self,
        sql: &str,
        params: &[&dyn pg_types::ToSql],
        fetch_size: i32,
    ) -> Result<Cursor<'_>> {
        self.transition(ConnectionState::ActiveExtendedQuery)?;

        let param_values = encode_params_text(params)?;
        let portal_name = format!("__pg_portal_{}", self.statement_counter);
        self.statement_counter += 1;

        // Parse (unnamed)
        self.codec
            .send(
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
            .send(
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

        // Sync to get BindComplete + RowDescription
        self.codec
            .send(&mut self.transport, &FrontendMessage::Sync)
            .await?;

        let mut columns: Option<Arc<Vec<FieldDescription>>> = None;

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(body) => {
                    columns = Some(Arc::new(read_row_description(body)?));
                }
                BackendMessage::NoData => {
                    // Non-SELECT query opened as cursor
                }
                BackendMessage::ReadyForQuery(body) => {
                    self.transaction_status =
                        TransactionStatus::from_u8(body.status())
                            .unwrap_or(TransactionStatus::Idle);
                    self.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let msg = format_error_fields(&body)?;
                    self.read_until_ready().await?;
                    self.state = ConnectionState::Idle;
                    return Err(Error::Server(msg));
                }
                BackendMessage::NoticeResponse(body) => {
                    if let Ok(notice) = crate::query::Notice::from_fields(&body) {
                        self.handle_notice(&notice);
                    }
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
        })
    }
}
