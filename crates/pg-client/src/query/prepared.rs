//! Prepared statement management.
//!
//! This module provides [`PreparedStatement`] and the [`Connection`] methods
//! for creating and closing prepared statements via the Extended Query Protocol.

use std::sync::Arc;

use fallible_iterator::FallibleIterator;
use crate::protocol::{BackendMessage, FrontendMessage};

use crate::connection::{Connection, ConnectionState};
use crate::error::{PgError, PgServerError, Result};
use crate::query::read_row_description;
use crate::query::row::FieldDescription;
use crate::transport::AsyncTransport;

// ---------------------------------------------------------------------------
// PreparedStatement
// ---------------------------------------------------------------------------

/// A server-side prepared statement.
///
/// Created via [`Connection::prepare`], a prepared statement can be executed
/// repeatedly with different parameters via [`Connection::query_prepared`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PreparedStatement {
    pub(crate) name: String,
    pub(crate) sql: String,
    pub(crate) param_types: Vec<crate::types::Type>,
    pub(crate) columns: Arc<Vec<FieldDescription>>,
}

impl PreparedStatement {
    /// The server-side name of this prepared statement.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The SQL text used to create this statement.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The parameter types inferred by the server.
    pub fn param_types(&self) -> &[crate::types::Type] {
        &self.param_types
    }

    /// The result column descriptions (empty for non-SELECT statements).
    pub fn columns(&self) -> &[FieldDescription] {
        &self.columns
    }
}

// ---------------------------------------------------------------------------
// Connection methods
// ---------------------------------------------------------------------------

impl Connection {
    /// Generate a unique statement name.
    fn next_statement_name(&mut self) -> String {
        self.statement_counter += 1;
        format!("__pg_stmt_{}", self.statement_counter)
    }

    /// Prepare a statement for repeated execution.
    ///
    /// The server parses the SQL, infers parameter types, and returns result
    /// column metadata. The returned [`PreparedStatement`] can be passed to
    /// [`Connection::query_prepared`] to execute with parameters.
    ///
    /// # Example
    /// ```ignore
    /// let stmt = conn.prepare("SELECT * FROM users WHERE id = $1").await?;
    /// let rows = conn.query_prepared(&stmt, &[&42i32]).await?;
    /// ```
    #[must_use = "prepare errors should be checked"]
    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement> {
        self.transition(ConnectionState::ActiveExtendedQuery)?;

        let name = self.next_statement_name();

        // Parse
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Parse {
                    name: name.clone(),
                    sql: sql.to_string(),
                    param_types: vec![], // let server infer
                },
            )
            .await?;

        // Describe (to get param types and result columns)
        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Describe {
                    variant: b'S',
                    name: name.clone(),
                },
            )
            .await?;

        // Sync
        self.codec
            .encode_and_write(&mut self.transport, &FrontendMessage::Sync)
            .await?;

        // Flush the batch
        self.transport.flush().await.map_err(PgError::Transport)?;

        // Read responses
        let mut param_types = Vec::new();
        let mut columns = Vec::new();

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            if self.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::ParameterDescription(body) => {
                    let mut iter = body.parameters();
                    while let Some(oid) = iter.next()? {
                        if let Some(ty) = crate::types::type_from_oid(oid) {
                            param_types.push(ty);
                        } else {
                            param_types.push(crate::types::Type::UNKNOWN);
                        }
                    }
                }
                BackendMessage::RowDescription(body) => {
                    columns = read_row_description(body)?;
                }
                BackendMessage::NoData => {
                    // Statement doesn't return rows (INSERT, UPDATE, etc.)
                }
                BackendMessage::ReadyForQuery(body) => {
                    self.transaction_status =
                        crate::protocol::TransactionStatus::from_u8(body.status())
                            .unwrap_or(crate::protocol::TransactionStatus::Idle);
                    self.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.read_until_ready().await?;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                _ => {}
            }
        }

        Ok(PreparedStatement {
            name,
            sql: sql.to_string(),
            param_types,
            columns: Arc::new(columns),
        })
    }

    /// Deallocate a prepared statement on the server.
    #[must_use = "close errors should be checked"]
    pub async fn close_statement(&mut self, stmt: &PreparedStatement) -> Result<()> {
        self.transition(ConnectionState::ActiveExtendedQuery)?;

        self.codec
            .encode_and_write(
                &mut self.transport,
                &FrontendMessage::Close {
                    variant: b'S',
                    name: stmt.name.clone(),
                },
            )
            .await?;

        self.codec
            .encode_and_write(&mut self.transport, &FrontendMessage::Sync)
            .await?;

        // Flush the batch
        self.transport.flush().await.map_err(PgError::Transport)?;

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            if self.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::CloseComplete => {}
                BackendMessage::ReadyForQuery(body) => {
                    self.transaction_status =
                        crate::protocol::TransactionStatus::from_u8(body.status())
                            .unwrap_or(crate::protocol::TransactionStatus::Idle);
                    self.state = ConnectionState::Idle;
                    break;
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    self.read_until_ready().await?;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                _ => {}
            }
        }

        Ok(())
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
            transaction_status: crate::protocol::TransactionStatus::Idle,
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

    fn build_parameter_description(oids: &[u32]) -> Vec<u8> {
        let mut buf = vec![b't'];
        let mut body = Vec::new();
        body.extend_from_slice(&(oids.len() as i16).to_be_bytes());
        for oid in oids {
            body.extend_from_slice(&oid.to_be_bytes());
        }
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
    }

    fn build_no_data() -> Vec<u8> {
        vec![b'n', 0, 0, 0, 4]
    }

    fn build_ready_for_query(status: u8) -> Vec<u8> {
        vec![b'Z', 0, 0, 0, 5, status]
    }

    #[tokio::test]
    async fn test_prepare_select() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_parse_complete());
        // ParameterDescription: 2 params (INT4=23, TEXT=25)
        data.extend_from_slice(&build_parameter_description(&[23, 25]));
        // RowDescription: id INT4, name TEXT
        data.extend_from_slice(&super::super::tests::build_row_description_msg(&[
            ("id", crate::types::INT4_OID),
            ("name", crate::types::TEXT_OID),
        ]));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let stmt = conn
            .prepare("SELECT * FROM users WHERE id = $1 AND name = $2")
            .await
            .unwrap();

        assert_eq!(stmt.name(), "__pg_stmt_1");
        assert_eq!(
            stmt.sql(),
            "SELECT * FROM users WHERE id = $1 AND name = $2"
        );
        assert_eq!(stmt.param_types().len(), 2);
        assert_eq!(stmt.param_types()[0], crate::types::Type::INT4);
        assert_eq!(stmt.param_types()[1], crate::types::Type::TEXT);
        assert_eq!(stmt.columns().len(), 2);
        assert_eq!(stmt.columns()[0].name(), "id");
        assert_eq!(stmt.columns()[1].name(), "name");
    }

    #[tokio::test]
    async fn test_prepare_insert() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_parse_complete());
        // ParameterDescription: 1 param (INT4=23)
        data.extend_from_slice(&build_parameter_description(&[23]));
        data.extend_from_slice(&build_no_data());
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let stmt = conn
            .prepare("INSERT INTO users (id) VALUES ($1)")
            .await
            .unwrap();

        assert_eq!(stmt.param_types().len(), 1);
        assert!(stmt.columns().is_empty());
    }

    #[tokio::test]
    async fn test_close_statement() {
        let mut data = Vec::new();
        data.extend_from_slice(&[b'3', 0, 0, 0, 4]); // CloseComplete
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let stmt = PreparedStatement {
            name: "__pg_stmt_1".into(),
            sql: "SELECT 1".into(),
            param_types: vec![],
            columns: Arc::new(vec![]),
        };
        conn.close_statement(&stmt).await.unwrap();
        assert!(conn.is_idle());
    }
}
