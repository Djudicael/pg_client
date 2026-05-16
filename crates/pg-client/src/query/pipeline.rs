//! Pipelined extended query execution.
//!
//! A [`Pipeline`] batches multiple parameterized queries into a single
//! round-trip, reducing latency when multiple independent queries need to
//! execute.
//!
//! # Example
//! ```ignore
//! let results = conn.pipeline()
//!     .query("SELECT $1", &[&1i32])
//!     .query("SELECT $1", &[&2i32])
//!     .finish()
//!     .await?;
//! ```

use std::sync::Arc;

use crate::protocol::{BackendMessage, FrontendMessage, TransactionStatus};

use crate::connection::{Connection, ConnectionState};
use crate::error::{PgError, PgServerError, Result};
use crate::query::params::encode_params_text;
use crate::query::result::{CommandTag, QueryResult};
use crate::query::row::{FieldDescription, Row};
use crate::query::{read_data_row, read_row_description};
use crate::transport::AsyncTransport;

// ---------------------------------------------------------------------------
// Pipeline types
// ---------------------------------------------------------------------------

/// A single operation in a pipeline.
#[derive(Debug)]
pub(crate) enum PipelineOp {
    /// A query that returns rows.
    Query {
        sql: String,
        params: Vec<Option<Vec<u8>>>,
    },
    /// A statement that does not return rows.
    Execute {
        sql: String,
        params: Vec<Option<Vec<u8>>>,
    },
}

/// The result of a single pipeline operation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PipelineResult {
    /// A query returned rows.
    Query(QueryResult),
    /// A statement completed without returning rows.
    Execute(CommandTag),
}

// ---------------------------------------------------------------------------
// Pipeline builder
// ---------------------------------------------------------------------------

/// A builder for pipelined extended-query operations.
///
/// Created via [`Connection::pipeline`]. All operations are buffered locally
/// until [`Pipeline::finish`] is called, at which point they are sent to the
/// server in a single batch followed by one `Sync`.
#[non_exhaustive]
pub struct Pipeline<'a> {
    conn: &'a mut Connection,
    ops: Vec<PipelineOp>,
}

impl<'a> Pipeline<'a> {
    pub(crate) fn new(conn: &'a mut Connection) -> Self {
        Self {
            conn,
            ops: Vec::new(),
        }
    }

    /// Add a query operation that returns rows.
    pub fn query(mut self, sql: &str, params: &[&dyn crate::types::ToSql]) -> Result<Self> {
        let values = encode_params_text(params)?;
        self.ops.push(PipelineOp::Query {
            sql: sql.to_string(),
            params: values,
        });
        Ok(self)
    }

    /// Add an execute operation that does not return rows.
    pub fn execute(mut self, sql: &str, params: &[&dyn crate::types::ToSql]) -> Result<Self> {
        let values = encode_params_text(params)?;
        self.ops.push(PipelineOp::Execute {
            sql: sql.to_string(),
            params: values,
        });
        Ok(self)
    }

    /// Send all buffered operations and collect results.
    ///
    /// Operations are sent as a single pipeline:
    /// ```text
    /// Parse | Bind | Execute | ... | Parse | Bind | Execute | Sync
    /// ```
    #[must_use = "pipeline errors should be checked"]
    pub async fn finish(self) -> Result<Vec<PipelineResult>> {
        let conn = self.conn;
        conn.transition(ConnectionState::ActiveExtendedQuery)?;

        // Send all operations as a batch
        for op in &self.ops {
            match op {
                PipelineOp::Query { sql, params } | PipelineOp::Execute { sql, params } => {
                    conn.codec
                        .encode_and_write(
                            &mut conn.transport,
                            &FrontendMessage::Parse {
                                name: String::new(),
                                sql: sql.clone(),
                                param_types: vec![],
                            },
                        )
                        .await?;

                    conn.codec
                        .encode_and_write(
                            &mut conn.transport,
                            &FrontendMessage::Bind {
                                portal: String::new(),
                                statement: String::new(),
                                param_formats: vec![crate::protocol::FormatCode::Text],
                                params: params.clone(),
                                result_formats: vec![crate::protocol::FormatCode::Binary],
                            },
                        )
                        .await?;

                    // Describe the unnamed portal so the server sends
                    // RowDescription before any DataRow.
                    conn.codec
                        .encode_and_write(
                            &mut conn.transport,
                            &FrontendMessage::Describe {
                                variant: b'P',
                                name: String::new(),
                            },
                        )
                        .await?;

                    conn.codec
                        .encode_and_write(
                            &mut conn.transport,
                            &FrontendMessage::Execute {
                                portal: String::new(),
                                max_rows: 0,
                            },
                        )
                        .await?;
                }
            }
        }

        // Single Sync at the end
        conn.codec
            .encode_and_write(&mut conn.transport, &FrontendMessage::Sync)
            .await?;

        // Flush the entire batch
        conn.transport.flush().await.map_err(PgError::Transport)?;

        // Read results
        let mut results = Vec::with_capacity(self.ops.len());
        let mut current_op = 0;
        let mut current_columns: Option<Arc<Vec<FieldDescription>>> = None;
        let mut current_rows: Vec<Row> = Vec::new();

        loop {
            let msg = conn.codec.read_message(&mut conn.transport).await?;
            if conn.handle_async_message(&msg) {
                continue;
            }
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::NoData => {
                    // Statement doesn't return rows (INSERT, UPDATE, etc.)
                }
                BackendMessage::RowDescription(body) => {
                    current_columns = Some(Arc::new(read_row_description(body)?));
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

                    // Determine if this was a Query or Execute op
                    match self.ops.get(current_op) {
                        Some(PipelineOp::Query { .. }) => {
                            results.push(PipelineResult::Query(QueryResult::new(
                                std::mem::take(&mut current_rows),
                                tag,
                                current_columns.take().unwrap_or_default(),
                            )));
                        }
                        Some(PipelineOp::Execute { .. }) => {
                            results.push(PipelineResult::Execute(tag));
                        }
                        None => {}
                    }
                    current_op += 1;
                }
                BackendMessage::EmptyQueryResponse => {
                    match self.ops.get(current_op) {
                        Some(PipelineOp::Query { .. }) => {
                            results.push(PipelineResult::Query(QueryResult::new(
                                Vec::new(),
                                CommandTag::new("".into()),
                                Arc::new(Vec::new()),
                            )));
                        }
                        Some(PipelineOp::Execute { .. }) => {
                            results.push(PipelineResult::Execute(CommandTag::new("".into())));
                        }
                        None => {}
                    }
                    current_op += 1;
                }
                BackendMessage::ErrorResponse(body) => {
                    let server_err = PgServerError::from_error_body(&body).map_err(PgError::Io)?;
                    conn.read_until_ready().await?;
                    conn.state = ConnectionState::Idle;
                    return Err(PgError::Server(Box::new(server_err)));
                }
                BackendMessage::ReadyForQuery(body) => {
                    conn.transaction_status = TransactionStatus::from_u8(body.status())
                        .unwrap_or(TransactionStatus::Idle);
                    conn.state = ConnectionState::Idle;
                    break;
                }
                _ => {}
            }
        }

        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// Connection method
// ---------------------------------------------------------------------------

impl Connection {
    /// Start building a pipeline of extended-query operations.
    ///
    /// See [`Pipeline`] for details.
    pub fn pipeline(&mut self) -> Pipeline<'_> {
        Pipeline::new(self)
    }
}
