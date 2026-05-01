//! Transaction management: BEGIN, COMMIT, ROLLBACK, savepoints.
//!
//! This module provides the [`Transaction`] guard type and supporting types
//! ([`TransactionOptions`], [`IsolationLevel`], [`Savepoint`]).

use crate::connection::Connection;
use crate::error::Result;
use crate::query::result::{ExecuteResult, QueryResult};

pub mod options;
pub mod savepoint;

pub use options::{IsolationLevel, TransactionOptions};
pub use savepoint::Savepoint;

// ---------------------------------------------------------------------------
// Transaction guard
// ---------------------------------------------------------------------------

/// An active transaction guard.
///
/// Created via [`Connection::transaction`] or [`Connection::transaction_with`].
/// The guard provides methods to execute queries within the transaction and
/// to [`commit`](Self::commit) or [`rollback`](Self::rollback) it.
///
/// # Drop behaviour
///
/// `Drop` cannot perform async I/O.  If the transaction is not explicitly
/// committed or rolled back before it goes out of scope, the connection may
/// be left in an idle-in-transaction state.  **Always** call `.commit().await`
/// or `.rollback().await` explicitly.
pub struct Transaction<'a> {
    pub(crate) conn: &'a mut Connection,
    pub(crate) committed: bool,
    pub(crate) savepoint_depth: u32,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(conn: &'a mut Connection) -> Self {
        Self {
            conn,
            committed: false,
            savepoint_depth: 0,
        }
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<()> {
        self.conn.execute("COMMIT").await?;
        self.committed = true;
        Ok(())
    }

    /// Roll back the transaction.
    pub async fn rollback(mut self) -> Result<()> {
        self.conn.execute("ROLLBACK").await?;
        self.committed = true;
        Ok(())
    }

    /// Returns `true` if the transaction is in a failed state.
    pub fn is_failed(&self) -> bool {
        self.conn.transaction_status() == pg_protocol::TransactionStatus::Failed
    }

    /// Execute a query that returns rows, within the transaction.
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.conn.query(sql).await
    }

    /// Execute a statement that does not return rows.
    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResult> {
        self.conn.execute(sql).await
    }

    /// Execute a query and return at most one row.
    pub async fn query_one(&mut self, sql: &str) -> Result<Option<crate::Row>> {
        self.conn.query_one(sql).await
    }

    /// Execute a parameterized query that returns rows.
    pub async fn query_params(
        &mut self,
        sql: &str,
        params: &[&dyn pg_types::ToSql],
    ) -> Result<QueryResult> {
        self.conn.query_params(sql, params).await
    }

    /// Execute a parameterized statement that does not return rows.
    pub async fn execute_params(
        &mut self,
        sql: &str,
        params: &[&dyn pg_types::ToSql],
    ) -> Result<ExecuteResult> {
        self.conn.execute_params(sql, params).await
    }

    /// Prepare a statement within the transaction.
    pub async fn prepare(&mut self, sql: &str) -> Result<crate::query::PreparedStatement> {
        self.conn.prepare(sql).await
    }

    /// Create a savepoint (nested transaction scope).
    ///
    /// Only one `Savepoint` guard can be active at a time for a given
    /// `Transaction` because it holds a mutable borrow.
    pub async fn savepoint(&mut self, name: &str) -> Result<Savepoint<'_, 'a>> {
        let sql = format!("SAVEPOINT {}", quote_identifier(name));
        self.conn.execute(&sql).await?;
        self.savepoint_depth += 1;
        Ok(Savepoint {
            transaction: self,
            name: name.to_string(),
            released: false,
        })
    }

    /// Start a COPY IN operation within this transaction.
    pub async fn copy_in(&mut self, sql: &str) -> Result<crate::CopyIn<'_>> {
        self.conn.copy_in(sql).await
    }

    /// Start a COPY OUT operation within this transaction.
    pub async fn copy_out(&mut self, sql: &str) -> Result<crate::CopyOut<'_>> {
        self.conn.copy_out(sql).await
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if self.committed || std::thread::panicking() {
            return;
        }
        // Drop cannot be async.  We cannot send ROLLBACK here.
        // Users must call .commit().await or .rollback().await explicitly.
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Quote a PostgreSQL identifier to prevent SQL injection.
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Connection extensions
// ---------------------------------------------------------------------------

impl Connection {
    /// Begin a new transaction.
    ///
    /// # Example
    /// ```ignore
    /// let mut txn = conn.transaction().await?;
    /// txn.execute("INSERT INTO users (name) VALUES ('alice')").await?;
    /// txn.commit().await?;
    /// ```
    pub async fn transaction(&mut self) -> Result<Transaction<'_>> {
        self.execute("BEGIN").await?;
        Ok(Transaction::new(self))
    }

    /// Begin a new transaction with the given options.
    ///
    /// # Example
    /// ```ignore
    /// let mut txn = conn.transaction_with(
    ///     TransactionOptions::new()
    ///         .isolation_level(IsolationLevel::Serializable)
    ///         .read_only(true)
    /// ).await?;
    /// ```
    pub async fn transaction_with(
        &mut self,
        options: &TransactionOptions,
    ) -> Result<Transaction<'_>> {
        let sql = options.to_begin_sql();
        self.execute(&sql).await?;
        Ok(Transaction::new(self))
    }

    /// Execute an async closure within a transaction.
    ///
    /// Commits on `Ok`, rolls back on `Err`. This requires Rust 1.85+ (async
    /// closures are stable).
    ///
    /// # Example
    /// ```ignore
    /// let rows: Vec<i32> = conn.with_transaction(async |txn| {
    ///     txn.execute("INSERT INTO nums (v) VALUES (1)").await?;
    ///     let result = txn.query("SELECT v FROM nums").await?;
    ///     let vals: Vec<i32> = result.iter().map(|r| r.get(0).unwrap()).collect();
    ///     Ok(vals)
    /// }).await?;
    /// ```
    pub async fn with_transaction<T, F>(&mut self, f: F) -> Result<T>
    where
        F: AsyncFnOnce(&mut Transaction<'_>) -> Result<T>,
    {
        let mut txn = self.transaction().await?;
        match f(&mut txn).await {
            Ok(val) => {
                txn.commit().await?;
                Ok(val)
            }
            Err(e) => {
                let _ = txn.rollback().await;
                Err(e)
            }
        }
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
    use crate::error::Error;
    use crate::transport::{BufferedTransport, ClientTransport, MockTransport, PgTransport};
    use pg_protocol::TransactionStatus;
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
        }
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

    fn build_error_response(msg: &str) -> Vec<u8> {
        let mut buf = vec![b'E'];
        let mut body = Vec::new();
        body.push(b'S');
        body.extend_from_slice(b"ERROR\0");
        body.push(b'M');
        body.extend_from_slice(msg.as_bytes());
        body.push(0);
        body.push(0);
        let len = (body.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);
        buf
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

    // -----------------------------------------------------------------------
    // Identifier quoting
    // -----------------------------------------------------------------------

    #[test]
    fn test_quote_identifier_basic() {
        assert_eq!(quote_identifier("foo"), "\"foo\"");
    }

    #[test]
    fn test_quote_identifier_with_quotes() {
        assert_eq!(quote_identifier("foo\"bar"), "\"foo\"\"bar\"");
    }

    #[test]
    fn test_quote_identifier_empty() {
        assert_eq!(quote_identifier(""), "\"\"");
    }

    // -----------------------------------------------------------------------
    // Transaction options
    // -----------------------------------------------------------------------

    #[test]
    fn test_transaction_options_default() {
        let opts = TransactionOptions::new();
        assert_eq!(opts.to_begin_sql(), "BEGIN");
    }

    #[test]
    fn test_transaction_options_isolation() {
        let opts = TransactionOptions::new().isolation_level(IsolationLevel::Serializable);
        assert_eq!(opts.to_begin_sql(), "BEGIN ISOLATION LEVEL SERIALIZABLE");
    }

    #[test]
    fn test_transaction_options_all() {
        let opts = TransactionOptions::new()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .deferrable(true);
        assert_eq!(
            opts.to_begin_sql(),
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY DEFERRABLE"
        );
    }

    #[test]
    fn test_transaction_options_read_write() {
        let opts = TransactionOptions::new().read_only(false);
        assert_eq!(opts.to_begin_sql(), "BEGIN READ WRITE");
    }

    // -----------------------------------------------------------------------
    // Transaction lifecycle (mock transport)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_transaction_commit_mock() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("BEGIN"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_command_complete_msg("COMMIT"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let txn = conn.transaction().await.unwrap();
        assert!(!txn.committed);
        txn.commit().await.unwrap();
        // After commit txn is consumed; verify connection state
        assert_eq!(conn.transaction_status(), TransactionStatus::Idle);
    }

    #[tokio::test]
    async fn test_transaction_rollback_mock() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("BEGIN"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_command_complete_msg("ROLLBACK"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let txn = conn.transaction().await.unwrap();
        assert!(!txn.committed);
        txn.rollback().await.unwrap();
        assert_eq!(conn.transaction_status(), TransactionStatus::Idle);
    }

    #[tokio::test]
    async fn test_transaction_is_failed_mock() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("BEGIN"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_error_response("syntax error"));
        data.extend_from_slice(&build_ready_for_query(b'E'));
        data.extend_from_slice(&build_command_complete_msg("ROLLBACK"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut txn = conn.transaction().await.unwrap();
        assert!(!txn.is_failed());

        // Bad query puts the transaction in failed state
        let err = txn.execute("BAD SQL").await;
        assert!(err.is_err());
        assert!(txn.is_failed());

        // Rolling back should clear the failed state
        txn.rollback().await.unwrap();
        assert_eq!(conn.transaction_status(), TransactionStatus::Idle);
    }

    #[tokio::test]
    async fn test_transaction_query_delegation_mock() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("BEGIN"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        // RowDescription + DataRow + CommandComplete + ReadyForQuery for SELECT
        data.extend_from_slice(&build_row_description_msg(&[("val", pg_types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("42")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_command_complete_msg("COMMIT"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut txn = conn.transaction().await.unwrap();
        let result = txn.query("SELECT 42").await.unwrap();
        assert_eq!(result.len(), 1);
        let v: i32 = result.rows()[0].get(0).unwrap();
        assert_eq!(v, 42);
        txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn test_with_transaction_success_mock() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("BEGIN"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_row_description_msg(&[("val", pg_types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("42")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_command_complete_msg("COMMIT"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn
            .with_transaction(async |txn| {
                let qr = txn.query("SELECT 42").await?;
                let v: i32 = qr.rows()[0].get(0)?;
                Ok(v)
            })
            .await
            .unwrap();
        assert_eq!(result, 42);
        assert_eq!(conn.transaction_status(), TransactionStatus::Idle);
    }

    #[tokio::test]
    async fn test_with_transaction_error_rolls_back_mock() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("BEGIN"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_row_description_msg(&[("val", pg_types::INT4_OID)]));
        data.extend_from_slice(&build_data_row_msg(&[Some("42")]));
        data.extend_from_slice(&build_command_complete_msg("SELECT 1"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_command_complete_msg("ROLLBACK"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let result = conn
            .with_transaction(async |txn| {
                let qr = txn.query("SELECT 42").await?;
                let _v: i32 = qr.rows()[0].get(0)?;
                // Force an error to trigger rollback
                Err::<i32, Error>(Error::Config("intentional failure".into()))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(conn.transaction_status(), TransactionStatus::Idle);
    }

    #[tokio::test]
    async fn test_transaction_savepoint_mock() {
        let mut data = Vec::new();
        data.extend_from_slice(&build_command_complete_msg("BEGIN"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_command_complete_msg("SAVEPOINT sp1"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_command_complete_msg("RELEASE SAVEPOINT sp1"));
        data.extend_from_slice(&build_ready_for_query(b'T'));
        data.extend_from_slice(&build_command_complete_msg("COMMIT"));
        data.extend_from_slice(&build_ready_for_query(b'I'));

        let mut conn = make_connection(data);
        let mut txn = conn.transaction().await.unwrap();
        assert_eq!(txn.savepoint_depth, 0);
        let sp = txn.savepoint("sp1").await.unwrap();
        sp.release().await.unwrap();
        assert_eq!(txn.savepoint_depth, 0);
        txn.commit().await.unwrap();
    }

    #[test]
    fn test_transaction_drop_without_commit_does_not_panic() {
        let mut conn = make_connection(Vec::new());
        let txn = Transaction::new(&mut conn);
        assert!(!txn.committed);
        drop(txn); // must not panic
    }

    #[test]
    fn test_transaction_drop_after_commit_is_noop() {
        let mut conn = make_connection(Vec::new());
        let txn = Transaction::new(&mut conn);
        // Simulate committed state (normally set by commit())
        let mut txn = txn;
        txn.committed = true;
        drop(txn); // must not panic
    }
}
