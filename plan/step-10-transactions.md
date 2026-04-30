# Step 10 - Transactions & Savepoints (Async)

> **Note:** All network I/O is async. Transaction and Savepoint guards still use Drop for safety, but Drop cannot be async — users should prefer explicit `.commit().await` / `.rollback().await`.

## Goal
Implement full transaction management with BEGIN/COMMIT/ROLLBACK, savepoints, nested transactions, and a safe RAII-based transaction guard API.

## Context
PostgreSQL transactions are managed via SQL commands (BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO SAVEPOINT). The wire protocol tracks transaction state via the `ReadyForQuery` message's transaction status indicator:
- `I` = Idle (no transaction)
- `T` = In transaction block
- `E` = In failed transaction block (all commands rejected until ROLLBACK)

A production-level client must:
- Track transaction state accurately
- Provide a safe API that prevents leaked transactions (uncommitted on drop)
- Support savepoints for nested transaction-like behavior
- Handle the failed transaction state properly

## Tasks

### 10.1 - Basic transaction API
```rust
impl Connection {
    /// Begin a transaction. Returns a Transaction guard.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, PgError> {
        self.execute("BEGIN").await?;
        Ok(Transaction {
            conn: self,
            committed: false,
            savepoint_depth: 0,
        })
    }

    /// Begin a transaction with specific isolation level
    pub async fn transaction_with(
        &mut self,
        options: &TransactionOptions,
    ) -> Result<Transaction<'_>, PgError> {
        let sql = options.to_begin_sql();
        self.execute(&sql).await?;
        Ok(Transaction {
            conn: self,
            committed: false,
            savepoint_depth: 0,
        })
    }
}
```

### 10.2 - Transaction options
```rust
pub struct TransactionOptions {
    pub isolation_level: Option<IsolationLevel>,
    pub read_only: Option<bool>,
    pub deferrable: Option<bool>,
}

pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,      // PostgreSQL default
    RepeatableRead,
    Serializable,
}

impl TransactionOptions {
    pub fn to_begin_sql(&self) -> String {
        let mut sql = String::from("BEGIN");
        if let Some(iso) = &self.isolation_level {
            sql.push_str(" ISOLATION LEVEL ");
            sql.push_str(match iso {
                IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
                IsolationLevel::ReadCommitted => "READ COMMITTED",
                IsolationLevel::RepeatableRead => "REPEATABLE READ",
                IsolationLevel::Serializable => "SERIALIZABLE",
            });
        }
        if let Some(read_only) = self.read_only {
            sql.push_str(if read_only { " READ ONLY" } else { " READ WRITE" });
        }
        if let Some(true) = self.deferrable {
            sql.push_str(" DEFERRABLE");
        }
        sql
    }
}
```

### 10.3 - Transaction guard (RAII)
```rust
pub struct Transaction<'a> {
    conn: &'a mut Connection,
    committed: bool,
    savepoint_depth: u32,
}

impl<'a> Transaction<'a> {
    /// Commit the transaction
    pub async fn commit(mut self) -> Result<(), PgError> {
        self.conn.execute("COMMIT").await?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction
    pub async fn rollback(mut self) -> Result<(), PgError> {
        self.conn.execute("ROLLBACK").await?;
        self.committed = true; // prevent double-rollback in Drop
        Ok(())
    }

    /// Access the connection to execute queries within the transaction
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult, PgError> {
        self.conn.query(sql).await
    }

    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResult, PgError> {
        self.conn.execute(sql).await
    }

    pub async fn query_params(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<QueryResult, PgError> {
        self.conn.query_params(sql, params).await
    }

    pub async fn execute_params(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<ExecuteResult, PgError> {
        self.conn.execute_params(sql, params).await
    }

    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, PgError> {
        self.conn.prepare(sql).await
    }

    /// Check if the transaction is in a failed state
    pub fn is_failed(&self) -> bool {
        self.conn.transaction_status == TransactionStatus::Failed
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.committed {
            // NOTE: Drop cannot be async. We do a best-effort synchronous cleanup.
            // The transport's Drop will close the TCP socket if ROLLBACK can't be sent.
            // Users should always call .commit().await or .rollback().await explicitly.
            // In WASI P2 single-threaded context, we can attempt a blocking write
            // of the ROLLBACK message, but cannot await the response.
        }
    }
}
```

### 10.4 - Savepoints (nested transactions)
```rust
impl<'a> Transaction<'a> {
    /// Create a savepoint (nested transaction)
    pub async fn savepoint(&mut self, name: &str) -> Result<Savepoint<'_, 'a>, PgError> {
        self.conn.execute(&format!("SAVEPOINT {}", quote_identifier(name))).await?;
        Ok(Savepoint {
            transaction: self,
            name: name.to_string(),
            released: false,
        })
    }
}

pub struct Savepoint<'t, 'c> {
    transaction: &'t mut Transaction<'c>,
    name: String,
    released: bool,
}

impl<'t, 'c> Savepoint<'t, 'c> {
    /// Release the savepoint (like commit for nested transaction)
    pub async fn release(mut self) -> Result<(), PgError> {
        self.transaction.conn.execute(
            &format!("RELEASE SAVEPOINT {}", quote_identifier(&self.name))
        ).await?;
        self.released = true;
        Ok(())
    }

    /// Rollback to the savepoint
    pub async fn rollback(mut self) -> Result<(), PgError> {
        self.transaction.conn.execute(
            &format!("ROLLBACK TO SAVEPOINT {}", quote_identifier(&self.name))
        ).await?;
        self.released = true;
        Ok(())
    }

    /// Execute queries within the savepoint scope
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult, PgError> {
        self.transaction.query(sql).await
    }

    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResult, PgError> {
        self.transaction.execute(sql).await
    }

    pub async fn query_params(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<QueryResult, PgError> {
        self.transaction.query_params(sql, params).await
    }

    pub async fn execute_params(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<ExecuteResult, PgError> {
        self.transaction.execute_params(sql, params).await
    }
}

impl<'t, 'c> Drop for Savepoint<'t, 'c> {
    fn drop(&mut self) {
        if !self.released {
            // NOTE: Drop cannot be async. We do a best-effort synchronous cleanup.
            // The transport's Drop will close the TCP socket if ROLLBACK can't be sent.
            // Users should always call .release().await or .rollback().await explicitly.
            // In WASI P2 single-threaded context, we can attempt a blocking write
            // of the ROLLBACK TO SAVEPOINT message, but cannot await the response.
        }
    }
}
```

### 10.5 - Failed transaction handling
```rust
impl Connection {
    /// Check the transaction status after any error
    fn handle_transaction_error(&mut self, err: &PgError) -> PgError {
        // If we're in a failed transaction state, the user needs to rollback
        if self.transaction_status == TransactionStatus::Failed {
            // All further queries will fail with
            // "current transaction is aborted, commands ignored until end of transaction block"
        }
        err.clone()
    }
}
```

### 10.6 - Convenience: with_transaction closure
```rust
use std::future::Future;

impl Connection {
    /// Execute an async closure within a transaction.
    /// Commits on Ok, rolls back on Err.
    pub async fn with_transaction<T, F, Fut>(&mut self, f: F) -> Result<T, PgError>
    where
        F: FnOnce(&mut Transaction<'_>) -> Fut,
        Fut: Future<Output = Result<T, PgError>>,
    {
        let mut txn = self.transaction().await?;
        match f(&mut txn).await {
            Ok(val) => {
                txn.commit().await?;
                Ok(val)
            }
            Err(e) => {
                txn.rollback().await?;
                Err(e)
            }
        }
    }
}
```

### 10.7 - Utility: identifier quoting
```rust
/// Quote a PostgreSQL identifier to prevent SQL injection in DDL/savepoint names
fn quote_identifier(name: &str) -> String {
    // Double any existing double-quotes, wrap in double-quotes
    format!("\"{}\"", name.replace('"', "\"\""))
}
```

## File Layout
```
crates/pg-client/src/
├── transaction/
│   ├── mod.rs          (Transaction, with_transaction)
│   ├── savepoint.rs    (Savepoint)
│   └── options.rs      (TransactionOptions, IsolationLevel)
```

## Acceptance Criteria
- [ ] BEGIN/COMMIT works (async)
- [ ] BEGIN/ROLLBACK works (async)
- [ ] Transaction Drop provides best-effort cleanup (cannot be async)
- [ ] All isolation levels supported
- [ ] READ ONLY / DEFERRABLE options
- [ ] Savepoints: create, release, rollback (all async)
- [ ] Savepoint Drop provides best-effort cleanup (cannot be async)
- [ ] Failed transaction state detected and reported
- [ ] `with_transaction` async closure API works
- [ ] Queries within transaction see uncommitted changes
- [ ] Concurrent connections have isolated transactions (integration test)
- [ ] Identifier quoting prevents injection in savepoint names

## Testing
- Basic commit/rollback (async)
- Drop cleanup behavior (best-effort, non-async)
- Nested savepoints (async)
- Savepoint rollback and retry (async)
- Failed transaction state handling
- Isolation level behavior (serializable conflict)
- Read-only transaction rejects writes
- `with_transaction` async error propagation
