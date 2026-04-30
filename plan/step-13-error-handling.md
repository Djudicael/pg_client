# Step 13 - Error Handling & Resilience

## Goal
Design a comprehensive error handling system, implement connection health checks, automatic reconnection strategies, and robust error reporting with full PostgreSQL error field mapping.

## Context
Production database clients must:
- Map all PostgreSQL error fields to structured Rust errors
- Distinguish between recoverable and fatal errors
- Detect broken connections and handle them gracefully
- Provide clear, actionable error messages

## Tasks

### 13.1 - Error type hierarchy
```rust
#[derive(Debug, thiserror::Error)]
pub enum PgError {
    /// Error returned by the PostgreSQL server
    #[error("PostgreSQL error: {0}")]
    Server(PgServerError),

    /// Wire protocol violation
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Network/transport error
    #[error("Transport error: {0}")]
    Transport(TransportError),

    /// Authentication failure
    #[error("Authentication failed: {0}")]
    Auth(AuthError),

    /// Type conversion error
    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(ConfigError),

    /// Connection is closed
    #[error("Connection closed")]
    ConnectionClosed,

    /// Unexpected NULL value
    #[error("Unexpected NULL in column {column}")]
    UnexpectedNull { column: String },

    /// Column not found
    #[error("Column not found: {name}")]
    ColumnNotFound { name: String },

    /// Column index out of bounds
    #[error("Column index {index} out of bounds (have {count} columns)")]
    ColumnIndexOutOfBounds { index: usize, count: usize },

    /// Timeout
    #[error("Operation timed out")]
    Timeout,

    /// Pool error
    #[error("Pool error: {0}")]
    Pool(String),
}
```

### 13.2 - PostgreSQL server error (all fields from ErrorResponse)
```rust
#[derive(Debug, Clone)]
pub struct PgServerError {
    /// Severity: ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG
    pub severity: String,
    /// Localized severity
    pub severity_v: Option<String>,
    /// SQLSTATE error code (e.g., "23505" for unique violation)
    pub code: String,
    /// Primary error message
    pub message: String,
    /// Optional detail
    pub detail: Option<String>,
    /// Optional hint
    pub hint: Option<String>,
    /// Error position in query string (1-based character offset)
    pub position: Option<u32>,
    /// Internal position
    pub internal_position: Option<u32>,
    /// Internal query
    pub internal_query: Option<String>,
    /// Where context (call stack in PL/pgSQL etc.)
    pub where_: Option<String>,
    /// Schema name
    pub schema: Option<String>,
    /// Table name
    pub table: Option<String>,
    /// Column name
    pub column: Option<String>,
    /// Data type name
    pub data_type: Option<String>,
    /// Constraint name
    pub constraint: Option<String>,
    /// Source file (in PG server code)
    pub file: Option<String>,
    /// Source line
    pub line: Option<u32>,
    /// Source routine
    pub routine: Option<String>,
}

impl PgServerError {
    pub fn from_fields(fields: Vec<(u8, String)>) -> Self {
        let mut err = PgServerError::default();
        for (code, value) in fields {
            match code {
                b'S' => err.severity = value,
                b'V' => err.severity_v = Some(value),
                b'C' => err.code = value,
                b'M' => err.message = value,
                b'D' => err.detail = Some(value),
                b'H' => err.hint = Some(value),
                b'P' => err.position = value.parse().ok(),
                b'p' => err.internal_position = value.parse().ok(),
                b'q' => err.internal_query = Some(value),
                b'W' => err.where_ = Some(value),
                b's' => err.schema = Some(value),
                b't' => err.table = Some(value),
                b'c' => err.column = Some(value),
                b'd' => err.data_type = Some(value),
                b'n' => err.constraint = Some(value),
                b'F' => err.file = Some(value),
                b'L' => err.line = value.parse().ok(),
                b'R' => err.routine = Some(value),
                _ => {} // ignore unknown fields (forward-compatible)
            }
        }
        err
    }

    /// Check SQLSTATE error code class
    pub fn is_class(&self, class: &str) -> bool {
        self.code.starts_with(class)
    }

    // Common error class checks
    pub fn is_integrity_constraint_violation(&self) -> bool { self.is_class("23") }
    pub fn is_unique_violation(&self) -> bool { self.code == "23505" }
    pub fn is_foreign_key_violation(&self) -> bool { self.code == "23503" }
    pub fn is_not_null_violation(&self) -> bool { self.code == "23502" }
    pub fn is_check_violation(&self) -> bool { self.code == "23514" }
    pub fn is_syntax_error(&self) -> bool { self.is_class("42") }
    pub fn is_insufficient_privilege(&self) -> bool { self.code == "42501" }
    pub fn is_undefined_table(&self) -> bool { self.code == "42P01" }
    pub fn is_undefined_column(&self) -> bool { self.code == "42703" }
    pub fn is_serialization_failure(&self) -> bool { self.code == "40001" }
    pub fn is_deadlock_detected(&self) -> bool { self.code == "40P01" }
    pub fn is_connection_exception(&self) -> bool { self.is_class("08") }
}

impl std::fmt::Display for PgServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} (SQLSTATE {})", self.severity, self.message, self.code)?;
        if let Some(detail) = &self.detail {
            write!(f, "\nDETAIL: {}", detail)?;
        }
        if let Some(hint) = &self.hint {
            write!(f, "\nHINT: {}", hint)?;
        }
        Ok(())
    }
}
```

### 13.3 - Transport errors
```rust
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Connection refused")]
    ConnectionRefused,

    #[error("Connection reset by peer")]
    ConnectionReset,

    #[error("Connection timed out")]
    Timeout,

    #[error("DNS resolution failed for host: {host}")]
    DnsResolutionFailed { host: String },

    #[error("TLS handshake failed: {0}")]
    TlsHandshake(String),

    #[error("TLS not supported by server")]
    TlsNotSupported,

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Connection closed unexpectedly")]
    UnexpectedEof,
}
```

### 13.4 - Connection health checks
```rust
impl Connection {
    /// Check if the connection is still alive by sending a simple query
    pub async fn ping(&mut self) -> Result<(), PgError> {
        self.query("SELECT 1").await?;
        Ok(())
    }

    /// Check connection state without sending a query.
    /// Examines the transport and protocol state.
    pub fn is_healthy(&self) -> bool {
        !self.is_closed() && self.transaction_status != TransactionStatus::Failed
    }

    /// Reset the connection state (clear failed transaction, discard temp objects)
    pub async fn reset(&mut self) -> Result<(), PgError> {
        if self.transaction_status == TransactionStatus::Failed
            || self.transaction_status == TransactionStatus::InTransaction
        {
            self.execute("ROLLBACK").await?;
        }
        self.execute("DISCARD ALL").await?;
        Ok(())
    }
}
```

### 13.5 - Error recovery patterns
```rust
/// Helper for retry logic with serialization failures
pub async fn with_retry<T, F, Fut>(
    conn: &mut Connection,
    max_retries: u32,
    f: F,
) -> Result<T, PgError>
where
    F: Fn(&mut Connection) -> Fut,
    Fut: Future<Output = Result<T, PgError>>,
{
    let mut attempt = 0;
    loop {
        match f(conn).await {
            Ok(val) => return Ok(val),
            Err(PgError::Server(ref e)) if e.is_serialization_failure() && attempt < max_retries => {
                attempt += 1;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### 13.6 - Error context and chaining
```rust
impl PgError {
    /// Add context to an error
    pub fn context(self, msg: impl Into<String>) -> Self {
        // Wrap with context for debugging
        PgError::Protocol(format!("{}: {}", msg.into(), self))
    }

    /// Check if the error indicates the connection is broken
    pub fn is_connection_broken(&self) -> bool {
        matches!(
            self,
            PgError::ConnectionClosed
            | PgError::Transport(TransportError::ConnectionReset)
            | PgError::Transport(TransportError::UnexpectedEof)
            | PgError::Server(ref e) if e.is_connection_exception()
        )
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            PgError::Server(e) => e.is_serialization_failure() || e.is_deadlock_detected(),
            PgError::Transport(TransportError::Timeout) => true,
            _ => false,
        }
    }
}
```

## File Layout
```
crates/pg-client/src/
├── error/
│   ├── mod.rs          (PgError enum)
│   ├── server.rs       (PgServerError with all fields)
│   ├── transport.rs    (TransportError)
│   ├── sqlstate.rs     (SQLSTATE code constants and helpers)
│   └── retry.rs        (with_retry helper)
```

## Acceptance Criteria
- [ ] All PostgreSQL error fields parsed correctly
- [ ] SQLSTATE codes accessible and classified
- [ ] Common error type checks (unique violation, syntax error, etc.)
- [ ] Transport errors clearly distinguished from server errors
- [ ] `is_connection_broken()` correctly identifies dead connections
- [ ] `ping()` validates connection liveness
- [ ] `reset()` recovers from failed transaction state
- [ ] `with_retry` handles serialization failures
- [ ] Error Display/Debug output is clear and actionable
- [ ] All error types implement `std::error::Error`

## Testing
- Parse ErrorResponse with all fields populated
- Verify SQLSTATE classification for known error codes
- Trigger unique constraint violation, verify structured error
- Trigger syntax error, verify position field
- Connection health after various error scenarios
- Reset after failed transaction
- Retry on serialization failure (requires SERIALIZABLE isolation)
