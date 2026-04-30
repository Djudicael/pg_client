# Step 19 - Public API Design & Documentation

## Goal
Design a clean, ergonomic, well-documented public API surface for the library. This step finalizes the crate's public exports, ensures API consistency, and documents usage patterns. The API incorporates all features from previous steps: streaming results, reconnection, tracing, and channel-based pooling.

## Context
The API should be:
- **Simple for common cases**: connect + query + disconnect in 3 lines
- **Powerful for advanced use**: transactions, prepared statements, COPY, notifications, streaming, reconnection
- **Safe**: no SQL injection via parameters, RAII guards for transactions, `#[must_use]` on Results
- **Forward-compatible**: `#[non_exhaustive]` on all enums and structs that may grow
- **Familiar**: inspired by `rust-postgres` / `sqlx` naming conventions
- **WASI-transparent**: users shouldn't need WASI knowledge for basic use

## Tasks

### 19.1 - Crate public API surface

```rust
// pg-client/src/lib.rs (published as `wasi-pg-client`)

//! # wasi-pg-client
//!
//! A production-grade PostgreSQL client library for WASI Preview 2.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use wasi_pg_client::{Connection, Config};
//!
//! #[wstd::main]
//! async fn main() -> Result<(), wasi_pg_client::PgError> {
//!     let config = Config::from_uri("postgresql://user:pass@localhost/mydb")?;
//!     let mut conn = Connection::connect(&config).await?;
//!
//!     let result = conn.query("SELECT id, name FROM users").await?;
//!     for row in result.iter() {
//!         let id: i32 = row.get(0)?;
//!         let name: String = row.get(1)?;
//!         println!("{}: {}", id, name);
//!     }
//!
//!     conn.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Feature Flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `tls` | ✅ | TLS support via rustls |
//! | `scram` | ✅ | SCRAM-SHA-256 authentication |
//! | `md5-auth` | ❌ | MD5 authentication (legacy) |
//! | `pool` | ❌ | Connection pooling |
//! | `tracing` | ✅ | Structured logging via tracing |
//! | `uuid` | ❌ | UUID type support via uuid crate |
//! | `serde-json` | ❌ | JSON type support via serde_json |
//! | `chrono` | ❌ | chrono integration for date/time |
//! | `test-native` | ❌ | Native transport for testing |
//!
//! ## WASI P2 Requirements
//!
//! This library targets `wasm32-wasip2`. When running in wasmtime, use:
//! ```bash
//! wasmtime run --wasi inherit-network --wasi inherit-env component.wasm
//! ```
//!
//! The `getrandom` crate must be configured with `features = ["wasi"]` for
//! cryptographic randomness (required for SCRAM auth and TLS).

// ── Re-exports ──

// Core types
pub use connection::{Connection, Config, ConfigBuilder};
pub use query::{Row, QueryResult, ExecuteResult};
pub use query::stream::{RowStream, CursorStream};
pub use query::prepared::PreparedStatement;
pub use query::pipeline::Pipeline;
pub use transaction::{Transaction, Savepoint, TransactionOptions, IsolationLevel};
pub use copy::{CopyIn, CopyOut, CopyFormat};
pub use notification::Notification;
pub use cancel::CancelToken;

// Error types
pub use error::{PgError, PgServerError, ErrorClass};
pub use error::sqlstate; // SQLSTATE code constants

// Type system
pub use types::{ToSql, FromSql, Oid};
pub use types::datetime::{PgDate, PgTime, PgTimestamp, PgTimestampTz, PgInterval};
pub use types::uuid::PgUuid;
pub use types::json::{PgJson, PgJsonb};
pub use types::numeric::PgNumeric;
pub use types::array::PgArray;

// Transport (for custom transports / testing)
pub use transport::{AsyncTransport, TransportError};

// TLS (behind feature flag)
#[cfg(feature = "tls")]
pub use transport::tls::{TlsConfig, SslMode, TlsInfo};

// Reconnection
pub use reconnect::{ReconnectConfig, StaleConfig, RetryPolicy};

// Pool (behind feature flag)
#[cfg(feature = "pool")]
pub use pool::{Pool, PoolConfig, PoolGuard, PoolStatus, PoolError};

// Protocol types (for advanced use)
pub use pg_protocol::types::{FormatCode, TransactionStatus, FieldDescription, CommandTag};
pub use pg_protocol::frontend::{FrontendMessage, DescribeVariant, CloseVariant};
pub use pg_protocol::backend::BackendMessage;
```

### 19.2 - Usage examples

#### Basic connection and query

```rust
use wasi_pg_client::{Connection, Config};

#[wstd::main]
async fn main() -> Result<(), wasi_pg_client::PgError> {
    let config = Config::from_uri("postgresql://user:pass@localhost/mydb")?;
    let mut conn = Connection::connect(&config).await?;

    // Simple query
    let result = conn.query("SELECT id, name FROM users").await?;
    for row in result.iter() {
        let id: i32 = row.get(0)?;
        let name: String = row.get(1)?;
        println!("{}: {}", id, name);
    }

    // Parameterized query (prevents SQL injection)
    let result = conn.query_params(
        "SELECT * FROM users WHERE age > $1 AND city = $2",
        &[&18i32, &"Paris"],
    ).await?;

    conn.close().await?;
    Ok(())
}
```

#### Streaming large result sets

```rust
use wasi_pg_client::{Connection, Config};

#[wstd::main]
async fn main() -> Result<(), wasi_pg_client::PgError> {
    let mut conn = Connection::connect(&Config::from_uri("postgresql://localhost/mydb")?).await?;

    // Stream rows one at a time (O(1) memory)
    let mut stream = conn.query_stream("SELECT * FROM large_table").await?;
    while let Some(row) = stream.next().await? {
        let id: i32 = row.get(0)?;
        // Process each row as it arrives
    }

    // Cursor-based streaming with fetch size (for very large results)
    let mut cursor = conn.cursor(
        "SELECT * FROM huge_table WHERE category = $1",
        &[&"electronics"],
        1000, // fetch 1000 rows per round-trip
    ).await?;
    while let Some(row) = cursor.next().await? {
        // Process row
    }

    conn.close().await?;
    Ok(())
}
```

#### Transactions

```rust
async fn transfer_funds(
    conn: &mut Connection,
    from: i64,
    to: i64,
    amount: f64,
) -> Result<(), wasi_pg_client::PgError> {
    // Automatic rollback on error, commit on success
    conn.with_transaction(|txn| async {
        txn.execute_params(
            "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
            &[&amount, &from],
        ).await?;
        txn.execute_params(
            "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
            &[&amount, &to],
        ).await?;
        Ok(())
    }).await
}

// Manual transaction control
async fn manual_transaction(conn: &mut Connection) -> Result<(), wasi_pg_client::PgError> {
    let mut txn = conn.transaction().await?;

    txn.execute("INSERT INTO orders (product, qty) VALUES ('widget', 10)").await?;

    // Nested savepoint
    let mut sp = txn.savepoint("inventory_check").await?;
    sp.execute("UPDATE inventory SET qty = qty - 10 WHERE product = 'widget'").await?;
    sp.release().await?; // commit savepoint

    txn.commit().await?; // commit transaction
    Ok(())
}
```

#### Prepared statements

```rust
async fn prepared_statements(conn: &mut Connection) -> Result<(), wasi_pg_client::PgError> {
    // Prepare once, execute many times
    let stmt = conn.prepare(
        "SELECT * FROM products WHERE category = $1 AND price < $2"
    ).await?;

    let electronics = conn.query_prepared(&stmt, &[&"electronics", &100.0f64]).await?;
    let books = conn.query_prepared(&stmt, &[&"books", &50.0f64]).await?;

    conn.close_statement(&stmt).await?;
    Ok(())
}
```

#### COPY for bulk operations

```rust
async fn bulk_operations(conn: &mut Connection) -> Result<(), wasi_pg_client::PgError> {
    // Bulk import
    let mut copy_in = conn.copy_in(
        "COPY products (name, price) FROM STDIN WITH (FORMAT csv)"
    ).await?;
    copy_in.write(b"Widget,9.99\n").await?;
    copy_in.write(b"Gadget,19.99\n").await?;
    let rows_imported = copy_in.finish().await?;

    // Bulk export
    let mut copy_out = conn.copy_out(
        "COPY products TO STDOUT WITH (FORMAT csv, HEADER)"
    ).await?;
    let csv_data = copy_out.read_all().await?;

    Ok(())
}
```

#### Connection pool

```rust
use wasi_pg_client::{Pool, PoolConfig, Config};

#[wstd::main]
async fn main() -> Result<(), wasi_pg_client::PgError> {
    let pool_config = PoolConfig {
        connection: Config::from_uri("postgresql://user:pass@localhost/mydb")?,
        max_size: 5,
        ..Default::default()
    };

    let pool = Pool::new(pool_config).await?;

    // Acquire a connection (takes &self, not &mut self)
    let mut guard = pool.acquire().await?;
    let result = guard.query("SELECT 1").await?;

    // Explicitly release (preferred — does async cleanup)
    guard.release().await?;

    // Can acquire again while other guards exist
    let g1 = pool.acquire().await?;
    let g2 = pool.acquire().await?; // works because acquire takes &self
    let status = pool.status(); // works too
    g1.release().await?;
    g2.release().await?;

    pool.close().await;
    Ok(())
}
```

#### Reconnection and resilience

```rust
use wasi_pg_client::{Config, ReconnectConfig, RetryPolicy, Connection};

async fn resilient_connection() -> Result<(), wasi_pg_client::PgError> {
    let mut config = Config::from_uri("postgresql://user:pass@localhost/mydb")?;
    config.reconnect = ReconnectConfig {
        enabled: true,
        max_attempts: 3,
        ..Default::default()
    };

    // Connect with retry
    let policy = RetryPolicy::exponential_backoff(3, std::time::Duration::from_millis(500), std::time::Duration::from_secs(10));
    let mut conn = Connection::connect_with_retry(&config, &policy).await?;

    // Execute with automatic reconnection on broken connection
    let result = conn.with_retry(|c| {
        c.query_params("SELECT * FROM users WHERE id = $1", &[&42i32])
    }).await?;

    conn.close().await?;
    Ok(())
}
```

#### Notifications (LISTEN/NOTIFY)

```rust
async fn notifications(conn: &mut Connection) -> Result<(), wasi_pg_client::PgError> {
    conn.listen("order_updates").await?;

    loop {
        if let Some(notification) = conn.wait_for_notification(
            Some(std::time::Duration::from_secs(30))
        ).await? {
            println!("Channel: {}, Payload: {}", notification.channel, notification.payload);
        }
    }
}
```

#### Error handling

```rust
use wasi_pg_client::{PgError, ErrorClass};

async fn handle_errors(conn: &mut Connection) -> Result<(), wasi_pg_client::PgError> {
    match conn.execute_params(
        "INSERT INTO users (email) VALUES ($1)",
        &[&"user@example.com"],
    ).await {
        Ok(result) => {
            println!("Inserted {} rows", result.rows_affected());
        }
        Err(PgError::Server(e)) if e.is_unique_violation() => {
            println!(
                "Email already exists: {}",
                e.constraint.as_deref().unwrap_or("unknown")
            );
        }
        Err(PgError::Server(e)) if e.is_serialization_failure() => {
            println!("Serialization failure — should retry");
        }
        Err(e) => {
            // Classify the error for retry decisions
            match Connection::classify_error(&e) {
                ErrorClass::Broken => println!("Connection broken — need to reconnect"),
                ErrorClass::Transient => println!("Transient error — can retry"),
                ErrorClass::Permanent => println!("Permanent error — cannot retry"),
            }
            return Err(e);
        }
    }
    Ok(())
}
```

### 19.3 - Builder pattern for Config

```rust
let config = wasi_pg_client::Config::builder()
    .host("localhost")
    .port(5432)
    .user("myuser")
    .password("mypassword")
    .database("mydb")
    .ssl_mode(wasi_pg_client::SslMode::Require)
    .connect_timeout(std::time::Duration::from_secs(10))
    .application_name("my-wasi-app")
    .enable_reconnect()
    .max_reconnect_attempts(5)
    .stale_threshold(std::time::Duration::from_secs(60))
    .build()?;
```

### 19.4 - `#[non_exhaustive]` on all public enums and structs

Every public enum and struct that may gain new variants or fields in the future must be marked `#[non_exhaustive]`. This prevents breaking changes when new variants/fields are added.

```rust
/// PostgreSQL error classification for retry/reconnection decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorClass {
    Broken,
    Transient,
    Permanent,
    // Future variants can be added without breaking changes
}

/// SSL mode — mirrors PostgreSQL's `sslmode` connection parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// COPY format options.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CopyFormat {
    Text,
    Csv { delimiter: char, null: String, header: bool, quote: char, escape: char },
    Binary,
}

/// Pool error types.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum PoolError {
    #[error("connection pool exhausted (max_size reached)")]
    Exhausted,

    #[error("connection pool is closed")]
    Closed,

    #[error("failed to create pool connection: {0}")]
    CreateFailed(String),

    #[error("connection reset failed: {0}")]
    ResetFailed(String),
}

/// PostgreSQL error type.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PgError {
    #[error("PostgreSQL error: {0}")]
    Server(PgServerError),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Transport error: {0}")]
    Transport(TransportError),

    #[error("Authentication failed: {0}")]
    Auth(AuthError),

    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    #[error("Configuration error: {0}")]
    Config(ConfigError),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Unexpected NULL in column {column}")]
    UnexpectedNull { column: String },

    #[error("Column not found: {name}")]
    ColumnNotFound { name: String },

    #[error("Column index {index} out of bounds (have {count} columns)")]
    ColumnIndexOutOfBounds { index: usize, count: usize },

    #[error("Operation timed out")]
    Timeout,

    #[error("Pool error: {0}")]
    Pool(String),
}

/// Transport error types.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    #[error("connection refused")]
    ConnectionRefused,

    #[error("connection reset by peer")]
    ConnectionReset,

    #[error("operation timed out")]
    Timeout,

    #[error("DNS resolution failed for host: {host}")]
    DnsResolutionFailed { host: String },

    #[error("TLS handshake failed: {0}")]
    TlsHandshake(String),

    #[error("TLS not supported by server")]
    TlsNotSupported,

    #[error("unexpected end of stream")]
    UnexpectedEof,

    #[error("I/O error: {0}")]
    Io(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Reconnection configuration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ReconnectConfig {
    pub enabled: bool,
    pub max_attempts: u32,
    pub initial_delay: std::time::Duration,
    pub max_delay: std::time::Duration,
    pub rebuild_session: bool,
    pub allow_mid_transaction: bool,
}

/// Pool configuration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PoolConfig {
    pub connection: Config,
    pub min_idle: usize,
    pub max_size: usize,
    pub acquire_timeout: Option<std::time::Duration>,
    pub max_lifetime: Option<std::time::Duration>,
    pub idle_timeout: Option<std::time::Duration>,
    pub test_on_acquire: bool,
    pub after_connect: Option<String>,
    pub before_return: Option<String>,
}
```

### 19.5 - `#[must_use]` on Result-returning methods

All public methods that return `Result` must be marked `#[must_use]` to prevent silently ignoring errors.

```rust
impl Connection {
    /// Execute a query that returns rows.
    ///
    /// # Errors
    ///
    /// Returns `PgError` if the query fails or the connection is broken.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// let result = conn.query("SELECT 1").await?;
    /// ```
    #[must_use = "query results should be checked for errors"]
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult, PgError> { /* ... */ }

    #[must_use = "query results should be checked for errors"]
    pub async fn query_params(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<QueryResult, PgError> { /* ... */ }

    #[must_use = "execute results should be checked for errors"]
    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResult, PgError> { /* ... */ }

    #[must_use = "execute results should be checked for errors"]
    pub async fn execute_params(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<ExecuteResult, PgError> { /* ... */ }

    #[must_use = "connection results should be checked for errors"]
    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, PgError> { /* ... */ }

    #[must_use = "connection results should be checked for errors"]
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, PgError> { /* ... */ }

    #[must_use = "connection results should be checked for errors"]
    pub async fn close(self) -> Result<(), PgError> { /* ... */ }
}

impl Config {
    #[must_use = "config parsing errors should be checked"]
    pub fn from_uri(uri: &str) -> Result<Config, ConfigError> { /* ... */ }
}

#[cfg(feature = "pool")]
impl Pool {
    #[must_use = "pool acquisition errors should be checked"]
    pub async fn acquire(&self) -> Result<PoolGuard<'_>, PgError> { /* ... */ }
}
```

### 19.6 - Row access API

```rust
/// A row from a query result.
///
/// Rows are created by the query execution methods and provide
/// type-safe access to column values.
pub struct Row {
    columns: Arc<Vec<FieldDescription>>,
    values: Vec<Option<Vec<u8>>>,
}

impl Row {
    /// Get a column value by index, decoded as type `T`.
    ///
    /// # Type inference
    ///
    /// The type `T` must implement `FromSql` for the column's PostgreSQL
    /// type OID. If the types don't match, a `TypeConversion` error is
    /// returned.
    ///
    /// # NULL handling
    ///
    /// If the column value is SQL NULL, this method returns
    /// `Err(PgError::UnexpectedNull)`. To handle NULL values, use
    /// `Option<T>` as the type parameter:
    ///
    /// ```rust,no_run
    /// let val: Option<i32> = row.get(0)?; // NULL → None
    /// ```
    ///
    /// # Errors
    ///
    /// - `PgError::ColumnIndexOutOfBounds` — index exceeds column count
    /// - `PgError::UnexpectedNull` — column is NULL and `T` is not `Option`
    /// - `PgError::TypeConversion` — type mismatch between PG type and `T`
    pub fn get<T: FromSql>(&self, index: usize) -> Result<T, PgError> { /* ... */ }

    /// Get a column value by name, decoded as type `T`.
    ///
    /// Column name lookup is O(n) where n is the number of columns.
    /// For performance-critical code, prefer index-based access.
    ///
    /// # Errors
    ///
    /// - `PgError::ColumnNotFound` — no column with the given name
    /// - `PgError::UnexpectedNull` — column is NULL and `T` is not `Option`
    /// - `PgError::TypeConversion` — type mismatch between PG type and `T`
    pub fn get_by_name<T: FromSql>(&self, name: &str) -> Result<T, PgError> { /* ... */ }

    /// Get raw bytes for a column. Returns `None` if the column is SQL NULL.
    pub fn get_raw(&self, index: usize) -> Option<&[u8]> { /* ... */ }

    /// Check if a column is SQL NULL.
    pub fn is_null(&self, index: usize) -> bool { /* ... */ }

    /// Number of columns in this row.
    pub fn len(&self) -> usize { /* ... */ }

    /// Returns true if the row has no columns.
    pub fn is_empty(&self) -> bool { /* ... */ }

    /// Column metadata for this row.
    pub fn columns(&self) -> &[FieldDescription] { /* ... */ }

    /// Get the name of a column by index.
    pub fn column_name(&self, index: usize) -> Option<&str> { /* ... */ }

    /// Get the index of a column by name.
    pub fn column_index(&self, name: &str) -> Option<usize> { /* ... */ }
}
```

### 19.7 - Connection API summary

```rust
impl Connection {
    // ── Connection lifecycle ──

    /// Connect to PostgreSQL using the given configuration.
    pub async fn connect(config: &Config) -> Result<Connection, PgError>;

    /// Connect with retry policy.
    pub async fn connect_with_retry(
        config: &Config,
        retry_policy: &RetryPolicy,
    ) -> Result<Connection, PgError>;

    /// Convenience: connect from a connection string.
    pub async fn connect_str(s: &str) -> Result<Connection, PgError>;

    /// Close the connection gracefully (sends Terminate message).
    pub async fn close(self) -> Result<(), PgError>;

    // ── Simple query ──

    /// Execute a query, collecting all rows into a Vec.
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult, PgError>;

    /// Execute a statement that doesn't return rows.
    pub async fn execute(&mut self, sql: &str) -> Result<ExecuteResult, PgError>;

    /// Execute and return the first row, or None.
    pub async fn query_one(&mut self, sql: &str) -> Result<Option<Row>, PgError>;

    /// Execute and process rows with a callback (streaming).
    pub async fn query_each<F>(&mut self, sql: &str, f: F) -> Result<CommandTag, PgError>
    where F: FnMut(Row) -> Result<(), PgError>;

    /// Execute and process rows with an async callback (streaming).
    pub async fn query_each_async<F, Fut>(&mut self, sql: &str, f: F) -> Result<CommandTag, PgError>
    where F: FnMut(Row) -> Fut, Fut: Future<Output = Result<(), PgError>>;

    /// Execute multiple statements in a single query message.
    pub async fn batch_execute(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError>;

    // ── Streaming query ──

    /// Execute a simple query and return a stream of rows.
    pub async fn query_stream(&mut self, sql: &str) -> Result<RowStream<'_>, PgError>;

    /// Execute a parameterized query and return a stream of rows.
    pub async fn query_params_stream(
        &mut self, sql: &str, params: &[&dyn ToSql],
    ) -> Result<RowStream<'_>, PgError>;

    /// Execute a prepared statement and return a stream of rows.
    pub async fn query_prepared_stream(
        &mut self, stmt: &PreparedStatement, params: &[&dyn ToSql],
    ) -> Result<RowStream<'_>, PgError>;

    /// Execute a query with cursor-based streaming (fetch_size rows per round-trip).
    pub async fn cursor(
        &mut self, sql: &str, params: &[&dyn ToSql], fetch_size: i32,
    ) -> Result<CursorStream<'_>, PgError>;

    // ── Extended query (parameterized) ──

    /// Execute a parameterized query, collecting all rows.
    pub async fn query_params(
        &mut self, sql: &str, params: &[&dyn ToSql],
    ) -> Result<QueryResult, PgError>;

    /// Execute a parameterized statement (no rows returned).
    pub async fn execute_params(
        &mut self, sql: &str, params: &[&dyn ToSql],
    ) -> Result<ExecuteResult, PgError>;

    // ── Prepared statements ──

    /// Prepare a statement for repeated execution.
    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, PgError>;

    /// Execute a prepared statement, collecting all rows.
    pub async fn query_prepared(
        &mut self, stmt: &PreparedStatement, params: &[&dyn ToSql],
    ) -> Result<QueryResult, PgError>;

    /// Close a prepared statement, freeing server resources.
    pub async fn close_statement(&mut self, stmt: &PreparedStatement) -> Result<(), PgError>;

    // ── Transactions ──

    /// Begin a transaction. Returns a Transaction guard.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, PgError>;

    /// Begin a transaction with specific options.
    pub async fn transaction_with(
        &mut self, options: &TransactionOptions,
    ) -> Result<Transaction<'_>, PgError>;

    /// Execute an async closure within a transaction.
    /// Commits on Ok, rolls back on Err.
    pub async fn with_transaction<T, F, Fut>(&mut self, f: F) -> Result<T, PgError>
    where
        F: FnOnce(&mut Transaction<'_>) -> Fut,
        Fut: Future<Output = Result<T, PgError>>;

    // ── COPY protocol ──

    /// Start a COPY IN operation (bulk import).
    pub async fn copy_in(&mut self, sql: &str) -> Result<CopyIn<'_>, PgError>;

    /// Start a COPY OUT operation (bulk export).
    pub async fn copy_out(&mut self, sql: &str) -> Result<CopyOut<'_>, PgError>;

    // ── LISTEN/NOTIFY ──

    /// Listen for notifications on a channel.
    pub async fn listen(&mut self, channel: &str) -> Result<(), PgError>;

    /// Stop listening on a channel.
    pub async fn unlisten(&mut self, channel: &str) -> Result<(), PgError>;

    /// Send a notification.
    pub async fn notify(&mut self, channel: &str, payload: &str) -> Result<(), PgError>;

    /// Wait for the next notification (async).
    pub async fn wait_for_notification(
        &mut self, timeout: Option<Duration>,
    ) -> Result<Option<Notification>, PgError>;

    /// Drain buffered notifications (sync, no I/O).
    pub fn notifications(&mut self) -> Vec<Notification>;

    // ── Query cancellation ──

    /// Get a cancellation token for this connection.
    pub fn cancel_token(&self) -> CancelToken;

    // ── Health and resilience ──

    /// Check if the connection is alive by sending a ping query.
    pub async fn ping(&mut self) -> Result<(), PgError>;

    /// Check if the connection is believed to be alive (fast, no I/O).
    pub fn is_alive(&self) -> bool;

    /// Check if the connection might be stale (based on time since last use).
    pub fn is_stale(&self, threshold: Duration) -> bool;

    /// Ensure the connection is alive before use (pings if stale).
    pub async fn ensure_alive(&mut self) -> Result<(), PgError>;

    /// Reset the connection state (ROLLBACK + DISCARD ALL).
    pub async fn reset(&mut self) -> Result<(), PgError>;

    /// Recover after an incomplete stream consumption.
    pub async fn recover(&mut self) -> Result<(), PgError>;

    /// Whether the connection needs recovery.
    pub fn needs_recovery(&self) -> bool;

    /// Attempt to reconnect a broken connection.
    pub async fn reconnect(&mut self) -> Result<(), PgError>;

    /// Execute an operation with automatic reconnection and retry.
    pub async fn with_retry<T, F, Fut>(&mut self, f: F) -> Result<T, PgError>
    where
        F: Fn(&mut Connection) -> Fut,
        Fut: Future<Output = Result<T, PgError>>;

    /// Classify an error for retry/reconnection decisions.
    pub fn classify_error(err: &PgError) -> ErrorClass;

    // ── Connection state ──

    /// Current transaction status.
    pub fn transaction_status(&self) -> TransactionStatus;

    /// Server version string.
    pub fn server_version(&self) -> &str;

    /// Server parameters.
    pub fn server_params(&self) -> &ServerParams;

    /// Whether the connection is closed.
    pub fn is_closed(&self) -> bool;

    // ── TLS info ──

    /// Returns true if the connection is using TLS.
    pub fn is_tls(&self) -> bool;

    /// Get TLS info if the connection is encrypted.
    #[cfg(feature = "tls")]
    pub fn tls_info(&self) -> Option<TlsInfo>;
}
```

### 19.8 - Module visibility audit

Ensure only the intended types are public:

| Module | Public | `pub(crate)` only |
|--------|--------|-------------------|
| `connection/` | `Connection`, `Config`, `ConfigBuilder`, `ServerParams` | `Codec`, `ConnectionState`, `SessionState` |
| `transport/` | `AsyncTransport`, `TransportError`, `TlsConfig`, `SslMode`, `TlsInfo` | `WasiTcpTransport`, `BufferedTransport`, `TlsTransport`, `PgTransport` |
| `query/` | `Row`, `QueryResult`, `ExecuteResult`, `RowStream`, `CursorStream`, `PreparedStatement`, `Pipeline` | `RowStreamState`, `encode_params` |
| `transaction/` | `Transaction`, `Savepoint`, `TransactionOptions`, `IsolationLevel` | — |
| `copy/` | `CopyIn`, `CopyOut`, `CopyFormat` | `BinaryCopyWriter` |
| `notification/` | `Notification` | — |
| `cancel/` | `CancelToken` | — |
| `error/` | `PgError`, `PgServerError`, `ErrorClass`, `sqlstate` | — |
| `reconnect/` | `ReconnectConfig`, `StaleConfig`, `RetryPolicy` | `SessionState` |
| `types/` | `ToSql`, `FromSql`, `Oid`, `PgDate`, `PgTime`, etc. | Internal impl details |
| `pool/` | `Pool`, `PoolConfig`, `PoolGuard`, `PoolStatus`, `PoolError` | `PoolInner`, `PooledConnection`, `AcquiredConnection` |

### 19.9 - Versioning and stability

```toml
# Cargo.toml
[package]
name = "wasi-pg-client"
version = "0.1.0"         # pre-1.0 for initial development
edition = "2021"
rust-version = "1.78"     # minimum for wasm32-wasip2 target
license = "MIT OR Apache-2.0"
description = "PostgreSQL client library for WASI Preview 2"
keywords = ["postgresql", "wasi", "wasip2", "database", "sql"]
categories = ["database", "wasm"]
repository = "https://github.com/your-org/wasi-pg-client"
```

**Stability guarantees for v0.1**:
- The public API may change between minor versions (semver pre-1.0)
- `#[non_exhaustive]` ensures adding new enum variants/struct fields isn't breaking
- Internal `pub(crate)` items can change freely
- The `AsyncTransport` trait is public for custom transports/testing but may evolve

**Stability guarantees for v1.0** (future):
- Full semver compatibility
- No breaking changes without major version bump
- `#[non_exhaustive]` already in place for forward compatibility

### 19.10 - Inline documentation

Every public type and method must have `///` doc comments with:
- Brief description
- Parameters explained (if not obvious)
- Return value explained
- Error conditions listed
- At least one example for key methods
- `# Panics` section if applicable (should be rare — prefer `Result`)
- `# Safety` section if any `unsafe` is involved (should be none in public API)

```rust
/// Execute a parameterized query, collecting all rows into a `Vec`.
///
/// This uses the PostgreSQL extended query protocol (Parse + Bind + Execute + Sync).
/// Parameters are sent in binary format, preventing SQL injection.
///
/// # Parameters
///
/// - `sql` — The SQL query with `$1`, `$2`, ... placeholders
/// - `params` — Parameter values to bind, implementing [`ToSql`]
///
/// # Returns
///
/// A [`QueryResult`] containing all rows and column metadata.
///
/// # Errors
///
/// - [`PgError::Server`] — SQL error (syntax, permission, constraint violation, etc.)
/// - [`PgError::ConnectionClosed`] — connection dropped during query
/// - [`PgError::TypeConversion`] — parameter type doesn't match column type
///
/// # Examples
///
/// ```rust,no_run
/// let result = conn.query_params(
///     "SELECT * FROM users WHERE age > $1 AND city = $2",
///     &[&18i32, &"Paris"],
/// ).await?;
/// for row in result.iter() {
///     let name: String = row.get("name")?;
///     println!("{}", name);
/// }
/// ```
///
/// # Note
///
/// For large result sets, prefer [`Connection::query_params_stream`] to avoid
/// buffering all rows in memory.
#[must_use = "query results should be checked for errors"]
pub async fn query_params(
    &mut self,
    sql: &str,
    params: &[&dyn ToSql],
) -> Result<QueryResult, PgError> {
    // ...
}
```

### 19.11 - Feature flag summary

```toml
[features]
default = ["tls", "scram", "tracing"]

# TLS support via rustls (pure Rust, WASI-compatible)
tls = ["dep:rustls", "dep:rustls-rustcrypto", "dep:webpki-roots"]

# SCRAM-SHA-256 authentication (recommended, PG 10+ default)
scram = ["dep:sha2", "dep:hmac", "dep:pbkdf2", "dep:base64"]

# MD5 authentication (legacy, less secure)
md5-auth = ["dep:md-5"]

# Connection pooling
pool = ["dep:wasi-pg-pool"]

# Structured logging via tracing crate
tracing = ["dep:tracing"]

# UUID type support via uuid crate
uuid = ["pg-types/uuid", "dep:uuid"]

# JSON type support via serde_json
serde-json = ["pg-types/serde-json", "dep:serde_json"]

# chrono integration for date/time types
chrono = ["pg-types/chrono", "dep:chrono"]

# Native test support (blocking I/O transport for non-WASI testing)
test-native = []

# futures::Stream implementation for RowStream (optional)
stream = ["dep:futures-core"]
```

### 19.12 - README quick-start guide

```markdown
# wasi-pg-client

A production-grade PostgreSQL client library for WASI Preview 2, written in Rust.

## Features

- ✅ Full PostgreSQL wire protocol v3 support
- ✅ Parameterized queries (SQL injection prevention)
- ✅ Prepared statements with automatic caching
- ✅ Streaming results (O(1) memory for large queries)
- ✅ Transactions with RAII guards and savepoints
- ✅ COPY protocol for bulk import/export
- ✅ LISTEN/NOTIFY for pub/sub
- ✅ TLS via rustls (pure Rust, WASI-compatible)
- ✅ SCRAM-SHA-256 and MD5 authentication
- ✅ Connection pooling
- ✅ Automatic reconnection and retry policies
- ✅ Structured logging via tracing
- ✅ Compiles to `wasm32-wasip2`

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
wasi-pg-client = "0.1"
wstd = "0.5"
```

Write your application:

```rust
use wasi_pg_client::{Connection, Config};

#[wstd::main]
async fn main() -> Result<(), wasi_pg_client::PgError> {
    let config = Config::from_uri("postgresql://user:pass@localhost/mydb")?;
    let mut conn = Connection::connect(&config).await?;

    let result = conn.query("SELECT id, name FROM users").await?;
    for row in result.iter() {
        let id: i32 = row.get(0)?;
        let name: String = row.get(1)?;
        println!("{}: {}", id, name);
    }

    conn.close().await?;
    Ok(())
}
```

Build and run with wasmtime:

```bash
cargo build --target wasm32-wasip2
wasmtime run --wasi inherit-network --wasi inherit-env target/wasm32-wasip2/debug/your_app.wasm
```

## WASI P2 Requirements

- **Target**: `wasm32-wasip2` (stable since Rust 1.78)
- **Runtime**: wasmtime with `--wasi inherit-network`
- **getrandom**: Must use `features = ["wasi"]` for cryptographic randomness

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tls` | ✅ | TLS support via rustls |
| `scram` | ✅ | SCRAM-SHA-256 authentication |
| `tracing` | ✅ | Structured logging |
| `pool` | ❌ | Connection pooling |
| `uuid` | ❌ | UUID type support |
| `serde-json` | ❌ | JSON type support |

## License

MIT OR Apache-2.0
```

### 19.13 - No public `unsafe`

The public API must not expose any `unsafe` items. If `unsafe` is used internally (e.g., for `Pin` projection in stream implementations), it must be encapsulated within `pub(crate)` modules with safety invariants documented.

```rust
// ❌ NEVER: public unsafe function
pub unsafe fn raw_query(/* ... */);

// ✅ OK: internal unsafe with documented safety invariant
pub(crate) unsafe fn pin_projection(/* ... */) {
    // SAFETY: The caller guarantees that the pointer is valid and
    // properly aligned. This is only called from within the
    // RowStream implementation where we have exclusive access.
}
```

### 19.14 - Consistent naming conventions

| Pattern | Convention | Example |
|---------|-----------|---------|
| Query methods | `query_*` | `query`, `query_params`, `query_stream` |
| Execute methods | `execute_*` | `execute`, `execute_params` |
| Stream methods | `*_stream` | `query_stream`, `query_params_stream` |
| Async cleanup | verb + `.await` | `commit().await`, `release().await` |
| Sync accessors | noun | `transaction_status()`, `is_alive()` |
| Result types | `*Result` | `QueryResult`, `ExecuteResult` |
| Error types | `*Error` | `PgError`, `TransportError`, `PoolError` |
| Config types | `*Config` | `Config`, `PoolConfig`, `TlsConfig`, `ReconnectConfig` |
| Guard types | `*Guard` or bare noun | `PoolGuard`, `Transaction`, `Savepoint` |
| PG-prefixed types | `Pg*` | `PgDate`, `PgUuid`, `PgNumeric` |
| Feature flags | kebab-case | `serde-json`, `md5-auth`, `test-native` |

## File Layout

```
crates/pg-client/src/
├── lib.rs              (public re-exports, crate-level docs)
├── connection/
│   ├── mod.rs          (Connection struct + async connect)
│   ├── config.rs       (Config, ConfigBuilder, connection string parsing)
│   ├── codec.rs        (Codec — async protocol + transport bridge)
│   └── lifecycle.rs    (close, read_until_ready, recover)
├── transport/
│   ├── mod.rs          (AsyncTransport trait + PgTransport enum + re-exports)
│   ├── tcp.rs          (WasiTcpTransport)
│   ├── tls.rs          (TlsTransport, TlsConfig, SslMode, TlsInfo)
│   ├── buffered.rs     (BufferedTransport)
│   ├── native.rs       (NativeTcpTransport — test-native feature)
│   ├── raw_wasi.rs     (RawWasiTransport — fallback)
│   ├── config.rs       (ConnectionParams)
│   └── error.rs        (TransportError)
├── query/
│   ├── mod.rs          (query, execute, query_one, batch_execute)
│   ├── row.rs          (Row, column access)
│   ├── result.rs       (QueryResult, ExecuteResult)
│   ├── stream.rs       (RowStream, RowStreamState)
│   ├── cursor_stream.rs (CursorStream)
│   ├── prepared.rs     (PreparedStatement, prepare/close)
│   ├── params.rs       (parameter encoding)
│   ├── pipeline.rs     (Pipeline)
│   └── cache.rs        (StatementCache)
├── transaction/
│   ├── mod.rs          (Transaction, with_transaction)
│   ├── savepoint.rs    (Savepoint)
│   └── options.rs      (TransactionOptions, IsolationLevel)
├── copy/
│   ├── mod.rs          (copy_in, copy_out entry points)
│   ├── copy_in.rs      (CopyIn writer)
│   ├── copy_out.rs     (CopyOut reader)
│   ├── format.rs       (CopyFormat)
│   └── binary.rs       (BinaryCopyWriter)
├── notification.rs     (Notification, listen/unlisten, wait_for_notification)
├── cancel.rs           (CancelToken)
├── reconnect/
│   ├── mod.rs          (reconnect, with_retry, ensure_alive)
│   ├── config.rs       (ReconnectConfig, StaleConfig)
│   ├── retry.rs        (RetryPolicy)
│   ├── session.rs      (SessionState tracking)
│   ├── classify.rs     (ErrorClass, classify_error)
│   └── env.rs          (environment variable parsing)
├── error/
│   ├── mod.rs          (PgError enum)
│   ├── server.rs       (PgServerError with all fields)
│   ├── sqlstate.rs     (SQLSTATE code constants)
│   └── auth.rs         (AuthError)
└── tracing_ext.rs      (internal: target constants, redaction helpers)
```

## Acceptance Criteria

- [ ] All public types documented with `///` comments
- [ ] Usage examples compile and work (tested via `cargo test --doc`)
- [ ] `cargo doc` generates clean documentation with no warnings
- [ ] No unnecessary public types exposed (visibility audit passed)
- [ ] Builder pattern for Config works
- [ ] Feature flags well-documented in crate-level docs and README
- [ ] Crate metadata complete (license, description, keywords, repository)
- [ ] README with quick-start guide
- [ ] API is consistent (naming conventions followed)
- [ ] `#[must_use]` on all Result-returning public methods
- [ ] `#[non_exhaustive]` on all public enums and structs that may grow
- [ ] No public `unsafe` items
- [ ] Every public method has an `# Errors` section in its doc comment
- [ ] Every public method has at least one `# Examples` section
- [ ] `Row::get()` supports both index and name-based access
- [ ] `Row::get()` supports `Option<T>` for NULL handling
- [ ] Streaming API is the primary (Vec-based methods are convenience)
- [ ] Connection string parsing supports URI and key-value formats
- [ ] Environment variable fallback documented and working
- [ ] TLS info accessible after connection
- [ ] Reconnection is opt-in with clear documentation
- [ ] Pool takes `&self` for acquire (not `&mut self`)
- [ ] Tracing levels documented for users

## WASI P3 Migration Path

Since the library is already fully async, migration to WASI P3 is minimal:
- Replace `wstd` runtime with WASI P3's native async Component Model support
- Pool may gain true concurrent access if WASI P3 adds threading
- The public API surface stays identical — no `.await` additions needed since it's already async
- `#[non_exhaustive]` ensures new features can be added without breaking changes
- Feature flags may be added (not removed) for WASI P3-specific features
