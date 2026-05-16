//! Streaming API for query results.
//!
//! This module provides [`RowStream`] — an async stream of rows from a query
//! result that processes rows one at a time without buffering the entire
//! result set in memory.

use std::sync::Arc;

use crate::protocol::{BackendMessage, TransactionStatus};

use crate::connection::{Connection, ConnectionState};
use crate::error::{PgError, PgServerError, Result};
use crate::query::result::CommandTag;
use crate::query::row::{FieldDescription, Row};
use crate::query::{read_data_row, read_row_description};

#[cfg(feature = "tracing")]
use crate::tracing_ext::TARGET_QUERY;

/// Internal state of the row stream.
#[derive(Debug)]
enum RowStreamState {
    WaitingForDescription,
    ReceivingRows,
    Finishing { command_tag: CommandTag },
    Done { command_tag: CommandTag },
    Error,
}

/// An async stream of rows from a query result.
///
/// Rows are fetched from the server one at a time as the consumer calls `next()`.
/// This provides natural backpressure and O(1) memory per row.
///
/// `RowStream` borrows the connection mutably. You cannot use the connection
/// while iterating the stream. When the stream is dropped (or fully consumed),
/// the connection is available again.
///
/// If the stream is dropped before being fully consumed, the connection is
/// left in an inconsistent state. The [`Connection::needs_recovery`] flag is
/// set, and you must call [`Connection::recover`] before using the connection
/// again. To avoid this, either consume the stream fully or call
/// [`RowStream::consume`] before dropping.
#[non_exhaustive]
pub struct RowStream<'a> {
    conn: &'a mut Connection,
    columns: Option<Arc<Vec<FieldDescription>>>,
    state: RowStreamState,
    /// Whether this stream was created via the extended query protocol.
    /// Used to determine protocol-specific behavior (e.g., portal-based
    /// fetch with `max_rows` for cursor streaming in a future iteration).
    #[allow(dead_code)]
    extended_protocol: bool,
    /// Number of rows fetched so far (for tracing).
    rows_fetched: u64,
    /// Time when the stream was created (for tracing duration).
    #[cfg(feature = "tracing")]
    started_at: std::time::Instant,
}

impl<'a> RowStream<'a> {
    /// Create a new `RowStream` for a simple query.
    pub(crate) fn new_simple(conn: &'a mut Connection) -> Self {
        RowStream {
            conn,
            columns: None,
            state: RowStreamState::WaitingForDescription,
            extended_protocol: false,
            rows_fetched: 0,
            #[cfg(feature = "tracing")]
            started_at: std::time::Instant::now(),
        }
    }

    /// Create a new `RowStream` for an extended query.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new_extended(conn: &'a mut Connection) -> Self {
        RowStream {
            conn,
            columns: None,
            state: RowStreamState::WaitingForDescription,
            extended_protocol: true,
            rows_fetched: 0,
            #[cfg(feature = "tracing")]
            started_at: std::time::Instant::now(),
        }
    }

    /// Create a new `RowStream` for an extended query with known columns.
    pub(crate) fn new_extended_with_columns(
        conn: &'a mut Connection,
        columns: Arc<Vec<FieldDescription>>,
    ) -> Self {
        RowStream {
            conn,
            columns: Some(columns),
            state: RowStreamState::WaitingForDescription,
            extended_protocol: true,
            rows_fetched: 0,
            #[cfg(feature = "tracing")]
            started_at: std::time::Instant::now(),
        }
    }

    /// Create a new `RowStream` for an extended query where the preamble
    /// (ParseComplete, BindComplete, RowDescription/NoData) has already
    /// been consumed. The stream starts directly in `ReceivingRows` state.
    pub(crate) fn new_extended_receiving(
        conn: &'a mut Connection,
        columns: Option<Arc<Vec<FieldDescription>>>,
    ) -> Self {
        RowStream {
            conn,
            columns,
            state: RowStreamState::ReceivingRows,
            extended_protocol: true,
            rows_fetched: 0,
            #[cfg(feature = "tracing")]
            started_at: std::time::Instant::now(),
        }
    }

    /// Fetch the next row from the stream.
    ///
    /// Returns `Ok(Some(row))` when a row is available, `Ok(None)` when the
    /// stream is exhausted, and `Err(...)` on a server or protocol error.
    /// After returning `None` or `Err`, subsequent calls return `None`.
    #[must_use = "stream errors should be checked"]
    pub async fn next(&mut self) -> Result<Option<Row>> {
        loop {
            match self.state {
                RowStreamState::Done { .. } | RowStreamState::Error => {
                    return Ok(None);
                }

                RowStreamState::WaitingForDescription => {
                    let msg = self
                        .conn
                        .codec
                        .read_message(&mut self.conn.transport)
                        .await?;
                    match msg {
                        BackendMessage::RowDescription(body) => {
                            self.columns = Some(Arc::new(read_row_description(body)?));
                            self.state = RowStreamState::ReceivingRows;
                        }
                        BackendMessage::CommandComplete(body) => {
                            let tag = CommandTag::new(body.tag().unwrap_or("").into());
                            self.state = RowStreamState::Finishing { command_tag: tag };
                        }
                        BackendMessage::EmptyQueryResponse => {
                            self.state = RowStreamState::Finishing {
                                command_tag: CommandTag::default(),
                            };
                        }
                        BackendMessage::ErrorResponse(body) => {
                            let server_err =
                                PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                            self.conn.read_until_ready().await?;
                            self.conn.state = ConnectionState::Idle;
                            self.state = RowStreamState::Error;
                            return Err(PgError::Server(Box::new(server_err)));
                        }
                        BackendMessage::NotificationResponse(body) => {
                            self.conn.notification_queue.push_back(
                                crate::notification::Notification {
                                    process_id: body.process_id(),
                                    channel: body.channel().unwrap_or("").to_string(),
                                    payload: body.message().unwrap_or("").to_string(),
                                },
                            );
                            continue;
                        }
                        BackendMessage::NoticeResponse(body) => {
                            if let Ok(notice) = crate::query::Notice::from_fields(&body) {
                                self.conn.handle_notice(&notice);
                            }
                            continue;
                        }
                        BackendMessage::ParameterStatus(body) => {
                            if let (Ok(name), Ok(value)) = (body.name(), body.value()) {
                                self.conn
                                    .server_params
                                    .params
                                    .insert(name.to_string(), value.to_string());
                            }
                            continue;
                        }
                        BackendMessage::ParseComplete
                        | BackendMessage::BindComplete
                        | BackendMessage::NoData => {
                            continue;
                        }
                        _ => continue,
                    }
                }

                RowStreamState::ReceivingRows => {
                    let msg = self
                        .conn
                        .codec
                        .read_message(&mut self.conn.transport)
                        .await?;
                    match msg {
                        BackendMessage::DataRow(body) => {
                            let values = read_data_row(body)?;
                            let cols = self.columns.clone().unwrap_or_default();
                            self.rows_fetched += 1;
                            return Ok(Some(Row::new(cols, values)));
                        }
                        BackendMessage::CommandComplete(body) => {
                            let tag = CommandTag::new(body.tag().unwrap_or("").into());
                            self.state = RowStreamState::Finishing { command_tag: tag };
                        }
                        BackendMessage::ErrorResponse(body) => {
                            let server_err =
                                PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                            self.conn.read_until_ready().await?;
                            self.conn.state = ConnectionState::Idle;
                            self.state = RowStreamState::Error;
                            return Err(PgError::Server(Box::new(server_err)));
                        }
                        BackendMessage::NotificationResponse(body) => {
                            self.conn.notification_queue.push_back(
                                crate::notification::Notification {
                                    process_id: body.process_id(),
                                    channel: body.channel().unwrap_or("").to_string(),
                                    payload: body.message().unwrap_or("").to_string(),
                                },
                            );
                            continue;
                        }
                        BackendMessage::NoticeResponse(body) => {
                            if let Ok(notice) = crate::query::Notice::from_fields(&body) {
                                self.conn.handle_notice(&notice);
                            }
                            continue;
                        }
                        BackendMessage::ParameterStatus(body) => {
                            if let (Ok(name), Ok(value)) = (body.name(), body.value()) {
                                self.conn
                                    .server_params
                                    .params
                                    .insert(name.to_string(), value.to_string());
                            }
                            continue;
                        }
                        BackendMessage::PortalSuspended => {
                            continue;
                        }
                        _ => continue,
                    }
                }

                RowStreamState::Finishing { .. } => {
                    let msg = self
                        .conn
                        .codec
                        .read_message(&mut self.conn.transport)
                        .await?;
                    match msg {
                        BackendMessage::ReadyForQuery(body) => {
                            let tag =
                                match std::mem::replace(&mut self.state, RowStreamState::Error) {
                                    RowStreamState::Finishing { command_tag } => command_tag,
                                    _ => CommandTag::default(),
                                };
                            self.conn.transaction_status =
                                TransactionStatus::from_u8(body.status())
                                    .unwrap_or(TransactionStatus::Idle);
                            self.conn.state = ConnectionState::Idle;
                            self.state = RowStreamState::Done { command_tag: tag };
                            #[cfg(feature = "tracing")]
                            tracing::info!(
                                target: TARGET_QUERY,
                                rows_fetched = self.rows_fetched,
                                command_tag = self.command_tag().map(|t| t.as_str()).unwrap_or(""),
                                elapsed_ms = self.started_at.elapsed().as_millis() as u64,
                                "Query stream completed"
                            );
                            return Ok(None);
                        }
                        BackendMessage::ErrorResponse(body) => {
                            // The server sent an error after CommandComplete but
                            // before ReadyForQuery. This can happen with certain
                            // PostgreSQL error conditions (e.g., cursor errors,
                            // constraint violations in multi-statement queries).
                            // We must surface this error rather than silently
                            // discarding it.
                            let server_err =
                                PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                            self.conn.read_until_ready().await?;
                            self.conn.state = ConnectionState::Idle;
                            self.state = RowStreamState::Error;
                            return Err(PgError::Server(Box::new(server_err)));
                        }
                        BackendMessage::NotificationResponse(body) => {
                            self.conn.notification_queue.push_back(
                                crate::notification::Notification {
                                    process_id: body.process_id(),
                                    channel: body.channel().unwrap_or("").to_string(),
                                    payload: body.message().unwrap_or("").to_string(),
                                },
                            );
                            continue;
                        }
                        BackendMessage::NoticeResponse(body) => {
                            if let Ok(notice) = crate::query::Notice::from_fields(&body) {
                                self.conn.handle_notice(&notice);
                            }
                            continue;
                        }
                        BackendMessage::ParameterStatus(body) => {
                            if let (Ok(name), Ok(value)) = (body.name(), body.value()) {
                                self.conn
                                    .server_params
                                    .params
                                    .insert(name.to_string(), value.to_string());
                            }
                            continue;
                        }
                        _ => continue,
                    }
                }
            }
        }
    }

    /// Get the column metadata for the current result set.
    pub fn columns(&self) -> Option<&[FieldDescription]> {
        self.columns.as_ref().map(|c| c.as_slice())
    }

    /// Get the command tag after the stream ends.
    pub fn command_tag(&self) -> Option<&CommandTag> {
        match &self.state {
            RowStreamState::Done { command_tag } | RowStreamState::Finishing { command_tag } => {
                Some(command_tag)
            }
            _ => None,
        }
    }

    /// Returns true if the stream has been fully consumed or encountered an error.
    pub fn is_done(&self) -> bool {
        matches!(
            self.state,
            RowStreamState::Done { .. } | RowStreamState::Error
        )
    }

    /// Consume the remaining rows in the stream, discarding them.
    #[must_use = "consume errors should be checked"]
    pub async fn consume(mut self) -> Result<CommandTag> {
        while self.next().await?.is_some() {}
        match &self.state {
            RowStreamState::Done { command_tag } => Ok(command_tag.clone()),
            _ => Ok(CommandTag::default()),
        }
    }
}

impl<'a> Drop for RowStream<'a> {
    fn drop(&mut self) {
        if !self.is_done() {
            #[cfg(feature = "tracing")]
            if !std::thread::panicking() {
                tracing::warn!(target: TARGET_QUERY, "RowStream dropped without full consumption; connection may need recovery");
            }
            self.conn.needs_recovery = true;
        }
    }
}

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

    fn build_row_description_msg(fields: &[(&str, u32)]) -> Vec<u8> {
        let mut buf = vec![b'T'];
        let mut body = Vec::new();
        body.extend_from_slice(&(fields.len() as i16).to_be_bytes());
        for (name, type_oid) in fields {
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            body.extend_from_slice(&0u32.to_be_bytes());
            body.extend_from_slice(&0i16.to_be_bytes());
            body.extend_from_slice(&type_oid.to_be_bytes());
            body.extend_from_slice(&(-1i16).to_be_bytes());
            body.extend_from_slice(&(-1i32).to_be_bytes());
            body.extend_from_slice(&0i16.to_be_bytes());
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

    fn build_error_response_msg(severity: &str, code: &str, message: &str) -> Vec<u8> {
        let mut buf = vec![b'E'];
        let mut body = Vec::new();
        // Severity
        body.push(b'S');
        body.extend_from_slice(severity.as_bytes());
        body.push(0);
        // Code (SQLSTATE)
        body.push(b'C');
        body.extend_from_slice(code.as_bytes());
        body.push(0);
        // Message
        body.push(b'M');
        body.extend_from_slice(message.as_bytes());
        body.push(0);
        // Terminator
        body.push(0);
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    #[tokio::test]
    async fn test_row_stream_basic() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[
            ("id", crate::types::INT4_OID),
            ("name", crate::types::TEXT_OID),
        ]));
        data.extend_from_slice(&build_data_row_msg(&[Some("1"), Some("alice")]));
        data.extend_from_slice(&build_data_row_msg(&[Some("2"), Some("bob")]));
        data.extend_from_slice(&build_data_row_msg(&[Some("3"), Some("charlie")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 3"));
        data.extend_from_slice(&build_ready_for_query(b'I'));
        let mut conn = make_connection(data);
        let mut stream = RowStream::new_simple(&mut conn);
        assert!(stream.columns().is_none());
        let row1 = stream.next().await.unwrap().unwrap();
        let id1: i32 = row1.get(0).unwrap();
        assert_eq!(id1, 1);
        assert!(stream.columns().is_some());
        let row2 = stream.next().await.unwrap().unwrap();
        let name2: String = row2.get(1).unwrap();
        assert_eq!(name2, "bob");
        let row3 = stream.next().await.unwrap().unwrap();
        let name3: String = row3.get(1).unwrap();
        assert_eq!(name3, "charlie");
        let done = stream.next().await.unwrap();
        assert!(done.is_none());
        assert!(stream.is_done());
        assert_eq!(stream.command_tag().unwrap().as_str(), "SELECT 3");
    }

    #[tokio::test]
    async fn test_row_stream_empty() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[("id", crate::types::INT4_OID)]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 0"));
        data.extend_from_slice(&build_ready_for_query(b'I'));
        let mut conn = make_connection(data);
        let mut stream = RowStream::new_simple(&mut conn);
        let done = stream.next().await.unwrap();
        assert!(done.is_none());
        assert!(stream.is_done());
    }

    #[tokio::test]
    async fn test_row_stream_no_row_description() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("INSERT 0 1"));
        data.extend_from_slice(&build_ready_for_query(b'I'));
        let mut conn = make_connection(data);
        let mut stream = RowStream::new_simple(&mut conn);
        let done = stream.next().await.unwrap();
        assert!(done.is_none());
        assert!(stream.is_done());
        assert_eq!(stream.command_tag().unwrap().as_str(), "INSERT 0 1");
    }

    #[tokio::test]
    async fn test_row_stream_consume() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[("val", crate::types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("10")]));
        data.extend_from_slice(&build_data_row_msg(&[Some("20")]));
        data.extend_from_slice(&build_data_row_msg(&[Some("30")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 3"));
        data.extend_from_slice(&build_ready_for_query(b'I'));
        let mut conn = make_connection(data);
        let mut stream = RowStream::new_simple(&mut conn);
        let row1 = stream.next().await.unwrap().unwrap();
        let v: i32 = row1.get(0).unwrap();
        assert_eq!(v, 10);
        let tag = stream.consume().await.unwrap();
        assert_eq!(tag.as_str(), "SELECT 3");
        assert!(!conn.needs_recovery());
    }

    #[tokio::test]
    async fn test_row_stream_drop_sets_needs_recovery() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[("val", crate::types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("10")]));
        data.extend_from_slice(&build_data_row_msg(&[Some("20")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 2"));
        data.extend_from_slice(&build_ready_for_query(b'I'));
        let mut conn = make_connection(data);
        {
            let mut stream = RowStream::new_simple(&mut conn);
            let _row1 = stream.next().await.unwrap().unwrap();
        }
        assert!(conn.needs_recovery());
    }

    #[tokio::test]
    async fn test_row_stream_extended_protocol() {
        let mut data = Vec::new();
        data.extend_from_slice(&[b'1', 0, 0, 0, 4]);
        data.extend_from_slice(&[b'2', 0, 0, 0, 4]);
        data.extend_from_slice(&build_row_description_msg(&[("val", crate::types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("42")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        data.extend_from_slice(&build_ready_for_query(b'I'));
        let mut conn = make_connection(data);
        let mut stream = RowStream::new_extended(&mut conn);
        let row = stream.next().await.unwrap().unwrap();
        let v: i32 = row.get(0).unwrap();
        assert_eq!(v, 42);
        let done = stream.next().await.unwrap();
        assert!(done.is_none());
    }

    #[tokio::test]
    async fn test_connection_recover_after_dropped_stream() {
        // Simulate: stream is dropped early, then recover() drains remaining messages
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[("val", crate::types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("10")]));
        data.extend_from_slice(&build_data_row_msg(&[Some("20")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 2"));
        data.extend_from_slice(&build_ready_for_query(b'I'));
        let mut conn = make_connection(data);
        {
            let mut stream = RowStream::new_simple(&mut conn);
            let _row1 = stream.next().await.unwrap().unwrap();
            // Drop stream without consuming remaining rows
        }
        assert!(
            conn.needs_recovery(),
            "should need recovery after dropping stream"
        );

        // Now recover — this should read until ReadyForQuery
        conn.recover().await.unwrap();
        assert!(
            !conn.needs_recovery(),
            "should not need recovery after recover"
        );
        assert_eq!(
            conn.state(),
            ConnectionState::Idle,
            "should be idle after recover"
        );
    }

    #[tokio::test]
    async fn test_row_stream_error_in_finishing_state() {
        // Simulate: server sends ErrorResponse after CommandComplete but
        // before ReadyForQuery. This used to be silently discarded.
        let mut data = Vec::new();
        data.extend_from_slice(&build_row_description_msg(&[("val", crate::types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("10")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        // Server sends an error during the finishing phase
        data.extend_from_slice(&build_error_response_msg(
            "ERROR",
            "57014", // query_canceled
            "canceling statement due to user request",
        ));
        data.extend_from_slice(&build_ready_for_query(b'I'));
        let mut conn = make_connection(data);
        let mut stream = RowStream::new_simple(&mut conn);
        // First row is fine
        let row = stream.next().await.unwrap().unwrap();
        let v: i32 = row.get(0).unwrap();
        assert_eq!(v, 10);
        // After CommandComplete, the stream enters Finishing state.
        // The next call should encounter the ErrorResponse and return an error.
        let result = stream.next().await;
        assert!(
            result.is_err(),
            "ErrorResponse in Finishing state should be surfaced as an error"
        );
        let err = result.unwrap_err();
        match err {
            PgError::Server(server_err) => {
                assert_eq!(
                    server_err.code, "57014",
                    "error code should be query_canceled"
                );
            }
            other => panic!("expected PgError::Server, got {:?}", other),
        }
        // Drop the stream so we can access conn
        drop(stream);
        // Connection should be idle after the error is handled
        assert_eq!(
            conn.state(),
            ConnectionState::Idle,
            "connection should be idle after error in finishing state"
        );
    }
}
