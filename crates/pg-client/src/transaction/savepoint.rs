//! Savepoint (nested transaction) support.

use crate::error::Result;
use crate::query::result::{ExecuteResult, QueryResult};
use crate::transaction::quote_identifier;
use crate::Transaction;

/// A savepoint within an active transaction.
///
/// Created via [`Transaction::savepoint`].  The savepoint acts as a
/// sub-transaction: it can be released (committed) or rolled back independently
/// of the outer transaction.
///
/// # Drop behaviour
///
/// If the savepoint is not explicitly released or rolled back before it goes
/// out of scope, `Drop` cannot perform async I/O.  Users should always call
/// [`release`](Self::release) or [`rollback`](Self::rollback) explicitly.
#[non_exhaustive]
pub struct Savepoint<'t, 'c> {
    pub(crate) transaction: &'t mut Transaction<'c>,
    pub(crate) name: String,
    pub(crate) released: bool,
}

impl<'t, 'c> Savepoint<'t, 'c> {
    /// Release the savepoint (like a commit for the nested scope).
    #[must_use = "savepoint release errors should be checked"]
    pub async fn release(mut self) -> Result<()> {
        let sql = format!("RELEASE SAVEPOINT {}", quote_identifier(&self.name));
        self.transaction.conn.execute(&sql).await?;
        self.released = true;
        self.transaction.savepoint_depth -= 1;
        Ok(())
    }

    /// Roll back to the savepoint.
    #[must_use = "savepoint rollback errors should be checked"]
    pub async fn rollback(mut self) -> Result<()> {
        let sql = format!("ROLLBACK TO SAVEPOINT {}", quote_identifier(&self.name));
        self.transaction.conn.execute(&sql).await?;
        self.released = true;
        self.transaction.savepoint_depth -= 1;
        Ok(())
    }

    /// Execute a query that returns rows, within the savepoint scope.
    #[must_use = "query errors should be checked"]
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        self.transaction.query(sql).await
    }

    /// Execute a statement that does not return rows.
    #[must_use = "execute errors should be checked"]
    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResult> {
        self.transaction.execute(sql).await
    }

    /// Execute a parameterized query that returns rows.
    #[must_use = "query errors should be checked"]
    pub async fn query_params(
        &mut self,
        sql: &str,
        params: &[&dyn crate::types::ToSql],
    ) -> Result<QueryResult> {
        self.transaction.query_params(sql, params).await
    }

    /// Execute a parameterized statement that does not return rows.
    #[must_use = "execute errors should be checked"]
    pub async fn execute_params(
        &mut self,
        sql: &str,
        params: &[&dyn crate::types::ToSql],
    ) -> Result<ExecuteResult> {
        self.transaction.execute_params(sql, params).await
    }

    /// Prepare a statement within the savepoint scope.
    #[must_use = "prepare errors should be checked"]
    pub async fn prepare(&mut self, sql: &str) -> Result<crate::query::PreparedStatement> {
        self.transaction.prepare(sql).await
    }
}

impl<'t, 'c> Drop for Savepoint<'t, 'c> {
    fn drop(&mut self) {
        if self.released || std::thread::panicking() {
            return;
        }
        // Drop cannot be async.  We cannot send a ROLLBACK TO SAVEPOINT here.
        // Users must call .release().await or .rollback().await explicitly.
        // Best-effort: the outer Transaction::drop will eventually close the
        // connection if the transaction is left in a bad state.
        self.transaction.savepoint_depth -= 1;
    }
}
