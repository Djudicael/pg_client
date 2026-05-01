//! Query protocol implementation — simple and extended.
//!
//! This module provides [`Connection`] methods for executing queries via both
//! the Simple Query Protocol (text-only, no parameters) and the Extended Query
//! Protocol (parameterized, prepared statements, binary data).

use std::sync::Arc;

use fallible_iterator::FallibleIterator;
use pg_protocol::{BackendMessage, FrontendMessage, TransactionStatus};

use crate::connection::Connection;
use crate::error::{PgError, PgServerError, Result};
use crate::query::result::{CommandTag, ExecuteResult, QueryResult};
use crate::query::row::{FieldDescription, Row};
use crate::transport::AsyncTransport;

pub mod cache;
pub mod cursor;
pub mod params;
pub mod pipeline;
pub mod prepared;
pub mod result;
pub mod row;
pub mod stream;

// Re-export commonly used types at the `query` level.
pub use cache::StatementCache;
pub use cursor::Cursor;
pub use cursor::CursorStream;
pub use pipeline::{Pipeline, PipelineResult};
pub use prepared::PreparedStatement;

// ---------------------------------------------------------------------------
// Notice
// ---------------------------------------------------------------------------

/// A notice (non-fatal warning) sent by the PostgreSQL server.
///
/// Wraps a [`PgServerError`] which contains all fields from the PostgreSQL
/// `NoticeResponse` message. Convenience accessor methods are provided for
/// the most commonly used fields.
#[derive(Debug, Clone)]
pub struct Notice {
    /// The underlying server error/notice with all fields.
    inner: PgServerError,
}

/// A callback that is invoked whenever the server sends a [`Notice`].
pub type NoticeHandler = Box<dyn Fn(&Notice) + Send + Sync>;

impl Notice {
    /// Parse a [`Notice`] from a [`NoticeResponseBody`](pg_protocol::backend::NoticeResponseBody).
    pub fn from_fields(fields: &pg_protocol::backend::NoticeResponseBody) -> Result<Self> {
        let inner = PgServerError::from_notice_body(fields).map_err(PgError::Io)?;
        Ok(Self { inner })
    }

    /// Returns the severity level.
    ///
    /// One of: `ERROR`, `FATAL`, `PANIC`, `WARNING`, `NOTICE`, `DEBUG`, `INFO`, `LOG`.
    pub fn severity(&self) -> &str {
        &self.inner.severity
    }

    /// Returns the SQLSTATE error code.
    pub fn code(&self) -> &str {
        &self.inner.code
    }

    /// Returns the primary human-readable message.
    pub fn message(&self) -> &str {
        &self.inner.message
    }

    /// Returns the detailed secondary message, if any.
    pub fn detail(&self) -> Option<&str> {
        self.inner.detail.as_deref()
    }

    /// Returns the suggestion for resolution, if any.
    pub fn hint(&self) -> Option<&str> {
        self.inner.hint.as_deref()
    }

    /// Returns a reference to the underlying [`PgServerError`].
    ///
    /// Use this to access all fields (position, schema, table, column,
    /// constraint, etc.) that are not exposed by the convenience methods.
    pub fn as_server_error(&self) -> &PgServerError {
        &self.inner
    }
}

impl std::fmt::Display for Notice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (SQLSTATE {})",
            self.inner.severity, self.inner.message, self.inner.code
        )?;
        if let Some(detail) = &self.inner.detail {
            write!(f, "\nDETAIL: {}", detail)?;
        }
        if let Some(hint) = &self.inner.hint {
            write!(f, "\nHINT: {}", hint)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Connection query methods
// ---------------------------------------------------------------------------

impl Connection {
    /// Execute a SQL query that returns rows.
    ///
    /// This is a convenience method that collects all rows into a
    /// [`QueryResult`]. For streaming results one row at a time, use
    /// [`Connection::query_stream`] instead.
    ///
    /// # Example
    /// ```ignore
    /// let result = conn.query("SELECT id, name FROM users").await?;
    /// for row in result.iter() {
    ///     let id: i32 = row.get(0)?;
    ///     let name: String = row.get(1)?;
    /// }
    /// ```
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let mut stream = self.query_stream(sql).await?;
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await? {
            rows.push(row);
        }
        let columns = stream.columns().map(|c| c.to_vec()).unwrap_or_default();
        let command_tag = stream.command_tag().cloned().unwrap_or_default();
        Ok(QueryResult::new(rows, command_tag, Arc::new(columns)))
    }

    /// Execute a SQL statement that does not return rows.
    ///
    /// Returns the number of rows affected where applicable.
    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResult> {
        let result = self.query(sql).await?;
        Ok(ExecuteResult::new(result.command_tag().clone()))
    }

    /// Execute a query and return at most one row.
    ///
    /// Returns `None` if the query returns zero rows.
    pub async fn query_one(&mut self, sql: &str) -> Result<Option<Row>> {
        let result = self.query(sql).await?;
        Ok(result.into_rows().into_iter().next())
    }

    /// Execute a query, invoking `f` for each row as it arrives.
    ///
    /// This avoids buffering all rows in memory, which is useful for large
    /// result sets.
    pub async fn query_each<F>(&mut self, sql: &str, mut f: F) -> Result<CommandTag>
    where
        F: FnMut(Row) -> Result<()>,
    {
        let mut stream = self.query_stream(sql).await?;
        while let Some(row) = stream.next().await? {
            f(row)?;
        }
        stream
            .command_tag()
            .cloned()
            .ok_or_else(|| PgError::InvalidState("stream ended without command tag".into()))
    }

    /// Execute multiple statements separated by semicolons.
    ///
    /// Returns a [`QueryResult`] for each statement that produces one.
    pub async fn batch_execute(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        self.transition(ConnectionState::ActiveSimpleQuery)?;

        self.codec
            .send(
                &mut self.transport,
                &FrontendMessage::Query { sql: sql.into() },
            )
            .await?;

        let mut results = Vec::new();
        let mut current_columns: Option<Arc<Vec<FieldDescription>>> = None;
        let mut current_rows: Vec<Row> = Vec::new();

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            if self.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::RowDescription(body) => {
                    current_columns = Some(Arc::new(read_row_description(body)?));
                    current_rows.clear();
                }
                BackendMessage::DataRow(body) => {
                    let values = read_data_row(body)?;
                    current_rows.push(Row::new(
                        current_columns.clone().unwrap_or_default(),
                        values,
                    ));
                }
                BackendMessage::CommandComplete(body) => {
                    let tag = CommandTag::new(body.tag().unwrap_or("").into());
                    results.push(QueryResult::new(
                        std::mem::take(&mut current_rows),
                        tag,
                        current_columns.take().unwrap_or_default(),
                    ));
                }
                BackendMessage::EmptyQueryResponse => {
                    results.push(QueryResult::new(
                        Vec::new(),
                        CommandTag::new("".into()),
                        Arc::new(Vec::new()),
                    ));
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.read_until_ready().await?;
                    self.state = ConnectionState::Idle;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                BackendMessage::ReadyForQuery(body) => {
                    self.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    self.state = ConnectionState::Idle;
                    break;
                }
                _ => {}
            }
        }

        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// Streaming query methods
// ---------------------------------------------------------------------------

impl Connection {
    /// Execute a simple query and return a stream of rows.
    ///
    /// This is the primary streaming API. Rows are fetched from the server
    /// one at a time as the consumer calls `next()` on the returned stream.
    /// Memory usage is O(1) per row regardless of result set size.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut stream = conn.query_stream("SELECT id, name FROM users").await?;
    /// while let Some(row) = stream.next().await? {
    ///     let id: i32 = row.get(0)?;
    ///     let name: String = row.get(1)?;
    /// }
    /// ```
    pub async fn query_stream(&mut self, sql: &str) -> Result<stream::RowStream<'_>> {
        // Ensure connection is in a clean state
        if self.needs_recovery {
            self.recover().await?;
        }

        self.transition(ConnectionState::Streaming)?;

        self.codec
            .send(
                &mut self.transport,
                &FrontendMessage::Query { sql: sql.into() },
            )
            .await?;

        Ok(stream::RowStream::new_simple(self))
    }

    /// Execute a parameterized query and return a stream of rows.
    ///
    /// Uses the extended query protocol (Parse + Bind + Describe + Execute + Sync).
    /// Parameters are text-encoded, preventing SQL injection.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut stream = conn.query_params_stream(
    ///     "SELECT id, name FROM users WHERE age > $1",
    ///     &[&18i32],
    /// ).await?;
    /// while let Some(row) = stream.next().await? {
    ///     let id: i32 = row.get(0)?;
    /// }
    /// ```
    pub async fn query_params_stream(
        &mut self,
        sql: &str,
        params: &[&dyn pg_types::ToSql],
    ) -> Result<stream::RowStream<'_>> {
        if self.needs_recovery {
            self.recover().await?;
        }

        self.transition(ConnectionState::Streaming)?;

        let param_values = params::encode_params_text(params)?;

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

        // Bind (unnamed portal, unnamed statement)
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Bind {
                    portal: String::new(),
                    statement: String::new(),
                    param_formats: vec![pg_protocol::FormatCode::Text],
                    params: param_values,
                    result_formats: vec![pg_protocol::FormatCode::Binary],
                },
            )
            .await?;

        // Describe portal (to get column metadata)
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Describe {
                    variant: b'P',
                    name: String::new(),
                },
            )
            .await?;

        // Execute
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Execute {
                    portal: String::new(),
                    max_rows: 0,
                },
            )
            .await?;

        // Sync
        self.codec
            .encode_and_write(&mut self.transport, &FrontendMessage::Sync)
            .await?;

        // Flush the entire batch
        self.transport.flush().await.map_err(PgError::Transport)?;

        Ok(stream::RowStream::new_extended(self))
    }

    /// Execute a prepared statement and return a stream of rows.
    pub async fn query_prepared_stream(
        &mut self,
        stmt: &PreparedStatement,
        params: &[&dyn pg_types::ToSql],
    ) -> Result<stream::RowStream<'_>> {
        if self.needs_recovery {
            self.recover().await?;
        }

        self.transition(ConnectionState::Streaming)?;

        let param_values = params::encode_params_binary(params, &stmt.param_types)?;

        // Bind (unnamed portal, named statement)
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Bind {
                    portal: String::new(),
                    statement: stmt.name.clone(),
                    param_formats: vec![pg_protocol::FormatCode::Binary],
                    params: param_values,
                    result_formats: vec![pg_protocol::FormatCode::Binary],
                },
            )
            .await?;

        // Describe portal
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Describe {
                    variant: b'P',
                    name: String::new(),
                },
            )
            .await?;

        // Execute
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Execute {
                    portal: String::new(),
                    max_rows: 0,
                },
            )
            .await?;

        // Sync
        self.codec
            .encode_and_write(&mut self.transport, &FrontendMessage::Sync)
            .await?;

        // Flush the entire batch
        self.transport.flush().await.map_err(PgError::Transport)?;

        // Use the prepared statement's column metadata
        Ok(stream::RowStream::new_extended_with_columns(
            self,
            stmt.columns.clone(),
        ))
    }

    /// Execute a query and process rows with an async callback (streaming).
    ///
    /// Like `query_each()` but the callback is async, allowing async operations
    /// (e.g., writing to another connection) per row.
    pub async fn query_each_async<F, Fut>(&mut self, sql: &str, mut f: F) -> Result<CommandTag>
    where
        F: FnMut(Row) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut stream = self.query_stream(sql).await?;
        while let Some(row) = stream.next().await? {
            f(row).await?;
        }
        stream
            .command_tag()
            .cloned()
            .ok_or_else(|| PgError::InvalidState("stream ended without command tag".into()))
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

use crate::connection::ConnectionState;

/// Convert a `RowDescriptionBody` into our `Vec<FieldDescription>`.
pub(crate) fn read_row_description(
    body: pg_protocol::backend::RowDescriptionBody,
) -> Result<Vec<FieldDescription>> {
    let mut fields = Vec::new();
    let mut iter = body.fields();
    while let Some(field) = iter.next()? {
        fields.push(FieldDescription::new(
            field.name().into(),
            field.table_oid(),
            field.column_id(),
            field.type_oid(),
            field.type_size(),
            field.type_modifier(),
            field.format(),
        ));
    }
    Ok(fields)
}

/// Convert a `DataRowBody` into a `Vec<Option<Vec<u8>>>`.
pub(crate) fn read_data_row(
    body: pg_protocol::backend::DataRowBody,
) -> Result<Vec<Option<Vec<u8>>>> {
    let buf = body.buffer();
    let mut values = Vec::new();
    let mut iter = body.ranges();
    while let Some(range) = iter.next()? {
        values.push(range.map(|r| buf[r].to_vec()));
    }
    Ok(values)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Codec, ServerParams};
    use crate::config::Config;
    use crate::connection::ConnectionState;
    use crate::transport::{BufferedTransport, ClientTransport, MockTransport, PgTransport};
    use std::collections::VecDeque;

    fn make_connection(read_data: Vec<u8>) -> Connection {
        let transport = PgTransport::Plain(BufferedTransport::new(ClientTransport::Mock(
            MockTransport::new(read_data),
        )));
        Connection {
            transport,
            codec: Codec::new(),
            server_params: ServerParams::default(),
            state: ConnectionState::Idle,
            config: Config::new(),
            transaction_status: TransactionStatus::Idle,
            notification_queue: VecDeque::new(),
            notice_handler: None,
            statement_counter: 0,
            needs_recovery: false,
        }
    }

    pub(crate) fn build_row_description_msg(fields: &[(&str, u32)]) -> Vec<u8> {
        let mut buf = vec![b'T'];
        let mut body = Vec::new();
        // field count
        body.extend_from_slice(&(fields.len() as i16).to_be_bytes());
        for (name, type_oid) in fields {
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            body.extend_from_slice(&0u32.to_be_bytes()); // table_oid
            body.extend_from_slice(&0i16.to_be_bytes()); // column_id
            body.extend_from_slice(&type_oid.to_be_bytes()); // type_oid
            body.extend_from_slice(&(-1i16).to_be_bytes()); // type_size
            body.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier
            body.extend_from_slice(&0i16.to_be_bytes()); // format
        }
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    fn build_data_row_msg(values: &[Option<&str>]) -> Vec<u8> {
        let mut buf = vec![b'D'];
        let mut body = Vec::new();
        // column count
        body.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for val in values {
            match val {
                Some(v) => {
                    let bytes = v.as_bytes();
                    body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    body.extend_from_slice(bytes);
                }
                None => {
                    body.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    fn build_command_complete_msg(tag: &str) -> Vec<u8> {
        let mut buf = vec![b'C'];
        let mut body = Vec::new();
        body.extend_from_slice(tag.as_bytes());
        body.push(0);
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    fn build_ready_for_query(status: u8) -> Vec<u8> {
        vec![b'Z', 0, 0, 0, 5, status]
    }

    #[tokio::test]
    async fn test_query_basic() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[
            ("id", pg_types::INT4_OID),
            ("name", pg_types::TEXT_OID),
        ]));
        data.extend_from_slice(&build_data_row_msg(&[Some("1"), Some("alice")]));
        data.extend_from_slice(&build_data_row_msg(&[Some("2"), Some("bob")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 2"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn.query("SELECT id, name FROM users").await.unwrap();
        assert_eq!(result.len(), 2);
        let id: i32 = result.rows()[0].get(0).unwrap();
        assert_eq!(id, 1);
        let name: String = result.rows()[0].get(1).unwrap();
        assert_eq!(name, "alice");
    }

    #[tokio::test]
    async fn test_execute_no_rows() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("INSERT 0 3"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn
            .execute("INSERT INTO users (name) VALUES ('alice')")
            .await
            .unwrap();
        assert_eq!(result.rows_affected(), Some(3));
    }

    #[tokio::test]
    async fn test_query_one() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[("id", pg_types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("42")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let row = conn.query_one("SELECT 42").await.unwrap();
        assert!(row.is_some());
        let id: i32 = row.unwrap().get(0).unwrap();
        assert_eq!(id, 42);
    }

    #[tokio::test]
    async fn test_query_empty() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[("id", pg_types::INT4_OID)]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 0"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn
            .query("SELECT id FROM users WHERE false")
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_query_error() {
        let mut data = Vec::new();
        // ErrorResponse
        let mut err = vec![b'E', 0, 0, 0, 26];
        err.extend_from_slice(&[b'S']);
        err.extend_from_slice(b"ERROR\0");
        err.extend_from_slice(&[b'M']);
        err.extend_from_slice(b"syntax error\0");
        err.push(0);
        data.extend_from_slice(&err);
        // ReadyForQuery
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn.query("BAD SQL").await;
        assert!(result.is_err());
        assert!(conn.is_idle());
    }

    #[tokio::test]
    async fn test_query_each() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[("val", pg_types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("10")]));
        data.extend_from_slice(&build_data_row_msg(&[Some("20")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 2"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut sum = 0i32;
        let tag = conn
            .query_each("SELECT val FROM nums", |row| {
                let v: i32 = row.get(0)?;
                sum += v;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(sum, 30);
        assert_eq!(tag.as_str(), "SELECT 2");
    }

    #[tokio::test]
    async fn test_batch_execute() {
        let mut data = Vec::new();
        // First result set
        data.extend_from_slice(&build_row_description_msg(&[("id", pg_types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("1")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        // Second result set (no rows)
        data.extend_from_slice(&build_command_complete_msg("INSERT 0 1"));
        // ReadyForQuery
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let results = conn
            .batch_execute("SELECT 1; INSERT INTO t VALUES (1)")
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].len(), 1);
        assert_eq!(results[1].len(), 0);
        assert_eq!(results[1].rows_affected(), Some(1));
    }

    #[tokio::test]
    async fn test_null_handling() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[("val", pg_types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[None]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn.query("SELECT NULL").await.unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.rows()[0].is_null(0));
    }
}
