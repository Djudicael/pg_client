//! Parameterized query execution via the Extended Query Protocol.
//!
//! This module provides [`Connection::query_params`] and
//! [`Connection::query_prepared`] for executing queries with parameters,
//! using either text or binary encoding.
//!
//! # Wire protocol flow
//!
//! The extended query protocol sends Parse → Bind → Execute → Sync as a
//! **batch** (no flush between individual messages), then flushes once before
//! reading responses.  This is critical: if each message is flushed
//! individually, Nagle's algorithm on the TCP layer may delay small writes,
//! causing the server to wait for more data while the client waits for a
//! response — a protocol-level deadlock.

use std::sync::Arc;

use pg_protocol::{BackendMessage, FrontendMessage, TransactionStatus};
use pg_types::{Format, ToSql};

use crate::connection::{Connection, ConnectionState};
use crate::error::{PgError, PgServerError, Result};
use crate::query::prepared::PreparedStatement;
use crate::query::result::{CommandTag, ExecuteResult, QueryResult};
use crate::query::row::{FieldDescription, Row};
use crate::query::{read_data_row, read_row_description};
use crate::transport::AsyncTransport;

// ---------------------------------------------------------------------------
// Parameter encoding helpers
// ---------------------------------------------------------------------------

/// Encode parameters as **text** (format 0).
///
/// Used for one-shot `query_params` where we don't yet know the server's
/// inferred parameter types.  Each parameter is serialised as its text
/// representation; `None` maps to a NULL (represented as `None` in the
/// returned vector, which the Bind message encodes as length -1).
pub(crate) fn encode_params_text(params: &[&dyn ToSql]) -> Result<Vec<Option<Vec<u8>>>> {
    let mut values = Vec::with_capacity(params.len());
    for p in params {
        let mut buf = Vec::new();
        // Text encoding ignores the specific Type, so we pass UNKNOWN.
        let is_null = p.to_sql(&pg_types::Type::UNKNOWN, &mut buf, Format::Text)?;
        match is_null {
            pg_types::IsNull::Yes => values.push(None),
            pg_types::IsNull::No => values.push(Some(buf)),
        }
    }
    Ok(values)
}

/// Encode parameters as **binary** (format 1).
///
/// Used for `query_prepared` where the [`PreparedStatement`] already stores
/// the parameter types.  NULL values are represented as `None` in the
/// returned vector.
pub(crate) fn encode_params_binary(
    params: &[&dyn ToSql],
    param_types: &[pg_types::Type],
) -> Result<Vec<Option<Vec<u8>>>> {
    if params.len() != param_types.len() {
        return Err(PgError::Config(format!(
            "parameter count mismatch: expected {}, got {}",
            param_types.len(),
            params.len()
        )));
    }

    let mut values = Vec::with_capacity(params.len());
    for (p, ty) in params.iter().zip(param_types.iter()) {
        let mut buf = Vec::new();
        let is_null = p.to_sql(ty, &mut buf, Format::Binary)?;
        match is_null {
            pg_types::IsNull::Yes => values.push(None),
            pg_types::IsNull::No => values.push(Some(buf)),
        }
    }
    Ok(values)
}

// ---------------------------------------------------------------------------
// Connection methods
// ---------------------------------------------------------------------------

impl Connection {
    /// Execute a parameterized query (one-shot extended query).
    ///
    /// Uses the unnamed prepared statement and unnamed portal. Parameters are
    /// sent in **text** format (the server infers types from the SQL). Result
    /// columns are requested in **binary** format where supported.
    ///
    /// # Wire protocol
    ///
    /// The messages Parse, Bind, Execute, and Sync are written into the
    /// transport's write buffer **without flushing** between them, then a
    /// single `flush()` is issued.  This avoids Nagle's-algorithm-induced
    /// deadlocks where small individual writes are buffered by the kernel
    /// while the server is waiting for the complete pipeline.
    ///
    /// # Example
    /// ```ignore
    /// let result = conn
    ///     .query_params("SELECT * FROM users WHERE id = $1", &[&42i32])
    ///     .await?;
    /// ```
    pub async fn query_params(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<QueryResult> {
        self.transition(ConnectionState::ActiveExtendedQuery)?;

        let param_values = encode_params_text(params)?;

        // ── Batch: Parse + Bind + Describe + Execute + Sync (no flush between) ──

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

        // Describe the unnamed portal so the server sends RowDescription
        // before any DataRow.  This gives us proper column metadata.
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

        // ── Flush the entire batch ──
        self.transport.flush().await.map_err(PgError::Transport)?;

        // ── Read responses ──
        let result = self.read_extended_query_result().await;
        if result.is_err() {
            let _ = self.read_until_ready().await;
        }
        self.state = ConnectionState::Idle;
        result
    }

    /// Execute a parameterized statement that does not return rows.
    pub async fn execute_params(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<ExecuteResult> {
        let result = self.query_params(sql, params).await?;
        Ok(ExecuteResult::new(result.command_tag().clone()))
    }

    /// Execute a previously prepared statement with parameters.
    ///
    /// Parameters are encoded in **binary** format using the types stored in
    /// the [`PreparedStatement`]. Results are requested in **binary** format.
    pub async fn query_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[&dyn ToSql],
    ) -> Result<QueryResult> {
        self.transition(ConnectionState::ActiveExtendedQuery)?;

        let param_values = encode_params_binary(params, &stmt.param_types)?;

        // ── Batch: Bind + Describe + Execute + Sync ──

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

        // Describe the unnamed portal so the server sends RowDescription
        // before any DataRow.
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

        // ── Flush the entire batch ──
        self.transport.flush().await.map_err(PgError::Transport)?;

        // ── Read responses ──
        let result = self.read_extended_query_result().await;
        if result.is_err() {
            let _ = self.read_until_ready().await;
        }
        self.state = ConnectionState::Idle;
        result
    }

    /// Execute a previously prepared statement that does not return rows.
    pub async fn execute_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[&dyn ToSql],
    ) -> Result<ExecuteResult> {
        let result = self.query_prepared(stmt, params).await?;
        Ok(ExecuteResult::new(result.command_tag().clone()))
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

impl Connection {
    /// Read an extended-query result set from the wire.
    ///
    /// Similar to `read_query_result` but also handles `ParseComplete`,
    /// `BindComplete`, and `NoData` which are specific to the extended
    /// query protocol.
    async fn read_extended_query_result(&mut self) -> Result<QueryResult> {
        let mut columns: Option<Arc<Vec<FieldDescription>>> = None;
        let mut rows: Vec<Row> = Vec::new();
        let mut tag = None;

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            if self.handle_async_message(&msg) {
                continue;
            }
            match msg {
                // Extended-query acknowledgements — consume and continue.
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::NoData => {}

                BackendMessage::RowDescription(body) => {
                    columns = Some(Arc::new(read_row_description(body)?));
                }
                BackendMessage::DataRow(body) => {
                    let values = read_data_row(body)?;
                    if columns.is_none() {
                        // The server did not send RowDescription (can happen
                        // for unnamed portals). Synthesise column metadata
                        // using the requested binary format.
                        let synthetic: Vec<FieldDescription> = (0..values.len())
                            .map(|i| {
                                FieldDescription::new(
                                    format!("col{}", i),
                                    0,
                                    0,
                                    0, // UNKNOWN OID
                                    -1,
                                    -1,
                                    1, // binary format
                                )
                            })
                            .collect();
                        columns = Some(Arc::new(synthetic));
                    }
                    rows.push(Row::new(columns.clone().unwrap(), values));
                }
                BackendMessage::CommandComplete(body) => {
                    tag = Some(CommandTag::new(body.tag().unwrap_or("").into()));
                }
                BackendMessage::EmptyQueryResponse => {
                    tag = Some(CommandTag::new("".into()));
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    // The server will send ReadyForQuery after ErrorResponse
                    // in extended query mode (because we sent Sync).
                    self.read_until_ready().await?;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                BackendMessage::ReadyForQuery(body) => {
                    self.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    break;
                }
                _ => {}
            }
        }

        Ok(QueryResult::new(
            rows,
            tag.unwrap_or_default(),
            columns.unwrap_or_default(),
        ))
    }
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
            health: crate::reconnect::session::ConnectionHealth::new(),
            session_state: crate::reconnect::session::SessionState::new(),
        }
    }

    fn build_parse_complete() -> Vec<u8> {
        vec![b'1', 0, 0, 0, 4]
    }

    fn build_bind_complete() -> Vec<u8> {
        vec![b'2', 0, 0, 0, 4]
    }

    fn build_row_description_msg(fields: &[(&str, u32)]) -> Vec<u8> {
        let mut buf = vec![b'T'];
        let mut body = Vec::new();
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
    async fn test_query_params_select() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_parse_complete());
        data.extend_from_slice(&build_bind_complete());
        data.extend_from_slice(&build_row_description_msg(&[("val", pg_types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("42")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn.query_params("SELECT $1", &[&42i32]).await.unwrap();
        assert_eq!(result.len(), 1);
        let v: i32 = result.rows()[0].get(0).unwrap();
        assert_eq!(v, 42);
    }

    #[tokio::test]
    async fn test_query_params_insert() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_parse_complete());
        data.extend_from_slice(&build_bind_complete());
        data.extend_from_slice(&build_command_complete_msg("INSERT 0 1"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn
            .execute_params("INSERT INTO t (id) VALUES ($1)", &[&1i32])
            .await
            .unwrap();
        assert_eq!(result.rows_affected(), Some(1));
    }

    #[tokio::test]
    async fn test_query_params_insert_two_params() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_parse_complete());
        data.extend_from_slice(&build_bind_complete());
        data.extend_from_slice(&build_command_complete_msg("INSERT 0 1"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn
            .execute_params(
                "INSERT INTO t (id, name) VALUES ($1, $2)",
                &[&200i32, &"param_insert"],
            )
            .await
            .unwrap();
        assert_eq!(result.rows_affected(), Some(1));
    }

    #[tokio::test]
    async fn test_query_prepared_select() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_bind_complete());
        data.extend_from_slice(&build_row_description_msg(&[("val", pg_types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("99")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let stmt = PreparedStatement {
            name: "__pg_stmt_1".into(),
            sql: "SELECT $1".into(),
            param_types: vec![pg_types::Type::INT4],
            columns: Arc::new(vec![]),
        };
        let result = conn.query_prepared(&stmt, &[&99i32]).await.unwrap();
        assert_eq!(result.len(), 1);
        let v: i32 = result.rows()[0].get(0).unwrap();
        assert_eq!(v, 99);
    }

    #[tokio::test]
    async fn test_query_params_error() {
        let mut data = Vec::new();
        let mut err = vec![b'E', 0, 0, 0, 22];
        err.extend_from_slice(&[b'S']);
        err.extend_from_slice(b"ERROR\0");
        err.extend_from_slice(&[b'M']);
        err.extend_from_slice(b"syntax error\0");
        err.push(0);
        data.extend_from_slice(&err);
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn.query_params("BAD $1", &[&1i32]).await;
        assert!(result.is_err());
        assert!(conn.is_idle());
    }
}
