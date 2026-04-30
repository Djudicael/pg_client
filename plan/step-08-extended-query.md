# Step 08 - Extended Query Protocol & Prepared Statements (Async)

## Goal
Implement the PostgreSQL Extended Query protocol, enabling parameterized queries (preventing SQL injection), prepared statements (performance), and binary data transfer.

## Context
The extended query protocol separates parsing, binding, and execution:
1. **Parse**: Server parses SQL, creates a prepared statement
2. **Bind**: Bind parameter values to a statement, creating a portal
3. **Describe**: Get metadata about statement params or portal results
4. **Execute**: Execute a portal, get results
5. **Sync**: End the extended query pipeline, get ReadyForQuery

This enables:
- **Parameterized queries**: `SELECT * FROM users WHERE id = $1`
- **Prepared statements**: Parse once, bind+execute many times
- **Binary format**: More efficient data transfer
- **SQL injection prevention**: Parameters are never interpolated into SQL

All network I/O is async using the `AsyncTransport` trait. The Parse/Bind/Describe/Execute/Sync pipeline is sent as a batch (multiple messages without flushing between them), then flushed once before reading responses.

## Tasks

### 8.1 - Prepared statement management
```rust
pub struct PreparedStatement {
    name: String,           // server-side statement name
    sql: String,
    param_types: Vec<Oid>,  // from ParameterDescription
    columns: Vec<FieldDescription>,  // from RowDescription (via Describe)
}

impl Connection {
    /// Prepare a statement for repeated execution
    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, PgError> {
        let name = self.next_statement_name();

        // Parse
        self.codec.send(&mut self.transport, &FrontendMessage::Parse {
            name: name.clone(),
            sql: sql.to_string(),
            param_types: vec![],  // let server infer
        }).await?;

        // Describe (to get param types and result columns)
        self.codec.send(&mut self.transport, &FrontendMessage::Describe {
            variant: DescribeVariant::Statement,
            name: name.clone(),
        }).await?;

        // Sync
        self.codec.send(&mut self.transport, &FrontendMessage::Sync).await?;

        // Read responses
        let mut param_types = Vec::new();
        let mut columns = Vec::new();

        loop {
            let msg = self.codec.read_message(&mut self.transport).await?;
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::ParameterDescription { types } => {
                    param_types = types;
                }
                BackendMessage::RowDescription { fields } => {
                    columns = fields;
                }
                BackendMessage::NoData => {
                    // Statement doesn't return rows (INSERT, UPDATE, etc.)
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

        Ok(PreparedStatement { name, sql: sql.to_string(), param_types, columns })
    }

    /// Deallocate a prepared statement
    pub async fn close_statement(&mut self, stmt: &PreparedStatement) -> Result<(), PgError> {
        self.codec.send(&mut self.transport, &FrontendMessage::Close {
            variant: CloseVariant::Statement,
            name: stmt.name.clone(),
        }).await?;
        self.codec.send(&mut self.transport, &FrontendMessage::Sync).await?;
        self.read_until_ready().await?;
        Ok(())
    }
}
```

### 8.2 - Parameterized query execution
```rust
impl Connection {
    /// Execute a parameterized query (parse + bind + execute in one pipeline)
    pub async fn query_params(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<QueryResult, PgError> {
        // Use unnamed statement + unnamed portal for one-shot queries
        let param_values: Vec<Option<Vec<u8>>> = params
            .iter()
            .map(|p| p.to_sql())
            .collect::<Result<_, _>>()?;

        // Parse (unnamed statement)
        self.codec.send(&mut self.transport, &FrontendMessage::Parse {
            name: String::new(),  // unnamed
            sql: sql.to_string(),
            param_types: vec![],
        }).await?;

        // Bind (unnamed portal)
        self.codec.send(&mut self.transport, &FrontendMessage::Bind {
            portal: String::new(),
            statement: String::new(),
            param_formats: vec![FormatCode::Binary],  // send params as binary
            params: param_values,
            result_formats: vec![FormatCode::Binary],  // request binary results
        }).await?;

        // Describe portal (to get column metadata)
        self.codec.send(&mut self.transport, &FrontendMessage::Describe {
            variant: DescribeVariant::Portal,
            name: String::new(),
        }).await?;

        // Execute
        self.codec.send(&mut self.transport, &FrontendMessage::Execute {
            portal: String::new(),
            max_rows: 0,  // 0 = return all rows
        }).await?;

        // Sync
        self.codec.send(&mut self.transport, &FrontendMessage::Sync).await?;

        // Read results (same as simple query but with binary data)
        self.read_query_result().await
    }

    /// Execute a parameterized statement (no rows returned)
    pub async fn execute_params(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<ExecuteResult, PgError> {
        let result = self.query_params(sql, params).await?;
        Ok(ExecuteResult { command_tag: result.command_tag })
    }
}
```

### 8.3 - Execute prepared statements
```rust
impl Connection {
    /// Execute a previously prepared statement with parameters
    pub async fn query_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[&dyn ToSql],
    ) -> Result<QueryResult, PgError> {
        let param_values = encode_params(params, &stmt.param_types)?;

        // Bind
        self.codec.send(&mut self.transport, &FrontendMessage::Bind {
            portal: String::new(),
            statement: stmt.name.clone(),
            param_formats: vec![FormatCode::Binary],
            params: param_values,
            result_formats: vec![FormatCode::Binary],
        }).await?;

        // Execute
        self.codec.send(&mut self.transport, &FrontendMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        }).await?;

        // Sync
        self.codec.send(&mut self.transport, &FrontendMessage::Sync).await?;

        self.read_query_result_with_columns(&stmt.columns).await
    }
}
```

### 8.4 - Statement cache
Automatically cache prepared statements to avoid re-parsing:
```rust
pub struct StatementCache {
    cache: HashMap<String, PreparedStatement>,  // key = SQL text
    capacity: usize,
    // LRU eviction
    order: VecDeque<String>,
}

impl StatementCache {
    pub async fn get_or_prepare(
        &mut self,
        conn: &mut ConnectionInner,
        sql: &str,
    ) -> Result<&PreparedStatement, PgError>;

    pub async fn evict_lru(&mut self, conn: &mut ConnectionInner) -> Result<(), PgError>;
}
```

### 8.5 - Pipelined execution
The extended protocol allows pipelining multiple operations before a single Sync:
```rust
impl Connection {
    /// Execute multiple parameterized queries in a single pipeline
    pub fn pipeline<'a>(
        &'a mut self,
    ) -> Pipeline<'a> {
        Pipeline::new(self)
    }
}

pub struct Pipeline<'a> {
    conn: &'a mut Connection,
    operations: Vec<PipelineOp>,
}

impl<'a> Pipeline<'a> {
    pub fn query(mut self, sql: &str, params: &[&dyn ToSql]) -> Self;
    pub fn execute(mut self, sql: &str, params: &[&dyn ToSql]) -> Self;

    /// Send all operations and collect results
    pub async fn finish(self) -> Result<Vec<PipelineResult>, PgError>;
}
```

### 8.6 - Cursor support (portal with max_rows)
```rust
impl Connection {
    /// Open a cursor for large result sets
    pub async fn query_cursor(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
        fetch_size: i32,
    ) -> Result<Cursor, PgError>;
}

pub struct Cursor<'a> {
    conn: &'a mut Connection,
    portal_name: String,
    columns: Arc<Vec<FieldDescription>>,
    fetch_size: i32,
    done: bool,
}

impl<'a> Cursor<'a> {
    /// Fetch next batch of rows
    pub async fn fetch_next(&mut self) -> Result<Vec<Row>, PgError>;

    /// Close the cursor
    pub async fn close(self) -> Result<(), PgError>;
}
```

## File Layout
```
crates/pg-client/src/
├── query/
│   ├── mod.rs
│   ├── row.rs
│   ├── result.rs
│   ├── prepared.rs     (PreparedStatement, prepare/close)
│   ├── params.rs       (parameter encoding, query_params)
│   ├── pipeline.rs     (Pipeline)
│   ├── cursor.rs       (Cursor)
│   └── cache.rs        (StatementCache)
```

## Acceptance Criteria
- [ ] Parameterized queries work with `$1, $2, ...` placeholders
- [ ] Prepared statements can be created, executed, and closed
- [ ] Statement cache avoids redundant Parse messages
- [ ] Pipeline sends multiple queries efficiently
- [ ] Cursors fetch data in batches
- [ ] Binary format used for data transfer
- [ ] SQL injection is impossible through parameter binding
- [ ] Correct parameter type inference by server

## Testing
- Parameterized SELECT, INSERT, UPDATE, DELETE
- Prepared statement reuse
- Pipeline with mixed queries
- Cursor with large result set
- Type mismatch errors (wrong param type)
- NULL parameters
- Statement cache eviction
