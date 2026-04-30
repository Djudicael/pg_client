# Step 07 - Simple Query Protocol (Async)

## Goal
Implement the PostgreSQL Simple Query protocol, which allows executing SQL statements and receiving results using a single async round-trip.

## Context
The simple query protocol is the most straightforward way to execute SQL:
1. Client sends `Query` message with SQL text (async write)
2. Server responds with zero or more result sets (RowDescription + DataRow*) followed by CommandComplete (async read)
3. Server sends `ReadyForQuery` when done

All network I/O is async using the `AsyncTransport` trait established in Step 02.

Multiple statements can be sent in a single Query message (separated by `;`). Each produces its own result set.

## Tasks

### 7.1 - Row representation
```rust
/// A row from a query result
pub struct Row {
    columns: Arc<Vec<FieldDescription>>,  // shared across all rows in result
    values: Vec<Option<Vec<u8>>>,         // raw column values (None = SQL NULL)
}

impl Row {
    /// Get a column value by index, decoded as type T
    pub fn get<T: FromSql>(&self, index: usize) -> Result<T, PgError>;

    /// Get a column value by name
    pub fn get_by_name<T: FromSql>(&self, name: &str) -> Result<T, PgError>;

    /// Get raw bytes for a column
    pub fn get_raw(&self, index: usize) -> Option<&[u8]>;

    /// Check if column is NULL
    pub fn is_null(&self, index: usize) -> bool;

    /// Number of columns
    pub fn len(&self) -> usize;

    /// Column metadata
    pub fn columns(&self) -> &[FieldDescription];
}
```

### 7.2 - Query result types
```rust
/// Result of a query that returns rows
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub command_tag: CommandTag,
    pub columns: Arc<Vec<FieldDescription>>,
}

impl QueryResult {
    pub fn rows_affected(&self) -> u64;
    pub fn is_empty(&self) -> bool;
    pub fn len(&self) -> usize;
    pub fn iter(&self) -> impl Iterator<Item = &Row>;
}

/// Result for statements that don't return rows (INSERT, UPDATE, DELETE, DDL)
pub struct ExecuteResult {
    pub command_tag: CommandTag,
}

impl ExecuteResult {
    pub fn rows_affected(&self) -> u64;
}
```

### 7.3 - Async simple query execution
```rust
impl Connection {
    /// Execute a query that returns rows
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult, PgError> {
        self.codec.send(&mut self.transport, &FrontendMessage::Query {
            sql: sql.to_string(),
        }).await?;

        let mut columns = None;
        let mut rows = Vec::new();
        let mut command_tag = None;

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            match msg {
                BackendMessage::RowDescription { fields } => {
                    columns = Some(Arc::new(fields));
                }
                BackendMessage::DataRow { values } => {
                    let cols = columns.as_ref().ok_or(PgError::Protocol(
                        "DataRow without RowDescription".into()
                    ))?;
                    rows.push(Row {
                        columns: cols.clone(),
                        values,
                    });
                }
                BackendMessage::CommandComplete { tag } => {
                    command_tag = Some(tag);
                }
                BackendMessage::EmptyQueryResponse => {
                    // Empty SQL string
                }
                BackendMessage::ErrorResponse { fields } => {
                    // Must still read until ReadyForQuery
                    self.read_until_ready().await?;
                    return Err(PgError::Server(PgServerError::from_fields(fields)));
                }
                BackendMessage::ReadyForQuery { transaction_status } => {
                    self.transaction_status = transaction_status;
                    break;
                }
                BackendMessage::NoticeResponse { .. } => {
                    // Log or ignore notices
                }
                _ => {} // ignore unexpected messages
            }
        }

        Ok(QueryResult {
            columns: columns.unwrap_or_default(),
            rows,
            command_tag: command_tag.unwrap_or_default(),
        })
    }

    /// Execute a statement that doesn't return rows
    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResult, PgError> {
        let result = self.query(sql).await?;
        Ok(ExecuteResult {
            command_tag: result.command_tag,
        })
    }

    /// Execute and return the first row, or None
    pub async fn query_one(&mut self, sql: &str) -> Result<Option<Row>, PgError> {
        let result = self.query(sql).await?;
        Ok(result.rows.into_iter().next())
    }

    /// Execute a query, process rows with a callback (streaming, lower memory)
    pub async fn query_each<F>(&mut self, sql: &str, mut f: F) -> Result<CommandTag, PgError>
    where
        F: FnMut(Row) -> Result<(), PgError>,
    {
        self.codec.send(&mut self.transport, &FrontendMessage::Query {
            sql: sql.to_string(),
        }).await?;

        let mut columns = None;
        let mut tag = None;

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            match msg {
                BackendMessage::RowDescription { fields } => {
                    columns = Some(Arc::new(fields));
                }
                BackendMessage::DataRow { values } => {
                    let cols = columns.as_ref().unwrap();
                    f(Row { columns: cols.clone(), values })?;
                }
                BackendMessage::CommandComplete { tag: t } => { tag = Some(t); }
                BackendMessage::ErrorResponse { fields } => {
                    self.read_until_ready().await?;
                    return Err(PgError::Server(PgServerError::from_fields(fields)));
                }
                BackendMessage::ReadyForQuery { transaction_status } => {
                    self.transaction_status = transaction_status;
                    break;
                }
                _ => {}
            }
        }

        Ok(tag.unwrap_or_default())
    }
}
```

### 7.4 - Async batch execution (multiple statements)
```rust
impl Connection {
    /// Execute multiple statements in a single query message.
    /// Returns results for each statement.
    pub async fn batch_execute(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        // Simple query protocol naturally supports this -
        // multiple statements separated by ; produce multiple result sets
        self.codec.send(&mut self.transport, &FrontendMessage::Query {
            sql: sql.to_string(),
        }).await?;

        let mut results = Vec::new();
        let mut current_columns = None;
        let mut current_rows = Vec::new();
        let mut current_tag = None;

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            match msg {
                BackendMessage::RowDescription { fields } => {
                    current_columns = Some(Arc::new(fields));
                    current_rows.clear();
                }
                BackendMessage::DataRow { values } => {
                    let cols = current_columns.as_ref().unwrap();
                    current_rows.push(Row { columns: cols.clone(), values });
                }
                BackendMessage::CommandComplete { tag } => {
                    results.push(QueryResult {
                        columns: current_columns.take().unwrap_or_default(),
                        rows: std::mem::take(&mut current_rows),
                        command_tag: tag,
                    });
                }
                BackendMessage::ReadyForQuery { transaction_status } => {
                    self.transaction_status = transaction_status;
                    break;
                }
                BackendMessage::ErrorResponse { fields } => {
                    self.read_until_ready().await?;
                    return Err(PgError::Server(PgServerError::from_fields(fields)));
                }
                _ => {}
            }
        }

        Ok(results)
    }
}
```

### 7.5 - Notice handling
```rust
pub struct Notice {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

// Connection should have a notice handler
pub type NoticeHandler = Box<dyn Fn(&Notice)>;
```

## File Layout
```
crates/pg-client/src/
├── query/
│   ├── mod.rs          (query, execute, query_one, batch_execute)
│   ├── row.rs          (Row, column access)
│   └── result.rs       (QueryResult, ExecuteResult)
```

## Acceptance Criteria
- [ ] `query()` returns rows with correct data
- [ ] `execute()` returns rows_affected for INSERT/UPDATE/DELETE
- [ ] `query_one()` returns first row or None
- [ ] `query_each()` streams rows without buffering all in memory
- [ ] `batch_execute()` handles multiple statements
- [ ] Error responses are properly propagated
- [ ] ReadyForQuery always consumed (even on error)
- [ ] Notices are handled (not discarded silently)
- [ ] NULL values handled correctly

## Limitations
- Simple query protocol always returns data in **text format** (not binary)
- No parameterized queries (SQL injection risk if interpolating user input)
- These limitations are addressed by the Extended Query Protocol (Step 08)

## Testing
- SELECT with various column types
- INSERT/UPDATE/DELETE and verify rows_affected
- Multi-statement batch
- Empty query
- Query returning zero rows
- Query error (syntax error, table not found)
- NULL handling
- Large result set (streaming test)
