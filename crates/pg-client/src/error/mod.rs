//! Error types for the PostgreSQL client.
//!
//! This module defines the [`PgError`] enum which represents all possible errors
//! that can occur when using the PostgreSQL client, along with supporting types:
//!
//! - [`PgServerError`] — structured PostgreSQL server error with all ErrorResponse fields
//! - [`TransportError`] — network/transport layer errors
//! - [`sqlstate`] — SQLSTATE error code constants and helpers
//! - [`retry`] — retry helpers for transient errors

pub mod retry;
pub mod server;
pub mod sqlstate;

use std::fmt;

use pg_protocol::ProtocolError;
use pg_types::Error as TypeConversionError;

pub use crate::transport::TransportError;
pub use server::PgServerError;

// ---------------------------------------------------------------------------
// PgError
// ---------------------------------------------------------------------------

/// The main error type for the PostgreSQL client.
///
/// This enum distinguishes between different error categories:
///
/// - **Server errors** — PostgreSQL returned an `ErrorResponse` message with
///   structured fields (SQLSTATE code, severity, detail, hint, etc.).
/// - **Transport errors** — Network-level failures (connection refused, timeout,
///   TLS handshake failure, etc.).
/// - **Protocol errors** — Wire protocol violations (unexpected message, bad encoding).
/// - **Authentication errors** — Failed authentication (wrong password, unsupported method).
/// - **Type conversion errors** — Failed conversion between Rust and PostgreSQL types.
/// - **Configuration errors** — Invalid connection string or parameters.
/// - **Connection state errors** — Connection closed, wrong state for operation.
/// - **Column/row errors** — Column not found, index out of bounds, unexpected NULL.
/// - **Timeout** — Operation timed out.
/// - **Pool errors** — Connection pool exhaustion or management errors.
#[derive(Debug)]
#[non_exhaustive]
pub enum PgError {
    /// Error returned by the PostgreSQL server (ErrorResponse).
    ///
    /// Contains all fields from the ErrorResponse message: severity, SQLSTATE
    /// code, message, detail, hint, position, constraint, etc.
    ///
    /// Boxed to reduce the overall size of the enum since `PgServerError`
    /// contains many `String` and `Option` fields.
    Server(Box<PgServerError>),

    /// Wire protocol violation.
    Protocol(ProtocolError),

    /// Network/transport error.
    Transport(TransportError),

    /// Authentication failure.
    Auth(String),

    /// Type conversion error.
    TypeConversion(TypeConversionError),

    /// Configuration error (invalid connection string, missing required field, etc.).
    Config(String),

    /// Connection is closed.
    ConnectionClosed,

    /// Unexpected NULL value in a column that was expected to be non-null.
    UnexpectedNull { column: String },

    /// Column not found in the result set.
    ColumnNotFound { name: String },

    /// Column index out of bounds.
    ColumnIndexOutOfBounds { index: usize, count: usize },

    /// Operation timed out.
    Timeout,

    /// Connection pool error.
    Pool(String),

    /// Invalid connection state for the requested operation.
    InvalidState(String),

    /// The operation is not supported.
    Unsupported(String),

    /// Operation was cancelled.
    Cancelled,

    /// I/O error.
    Io(std::io::Error),

    /// TLS error.
    #[cfg(feature = "tls")]
    Tls(rustls::Error),

    /// Any other error not covered by the above variants.
    Other(String),
}

impl std::fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PgError::Server(e) => write!(f, "server error: {}", e),
            PgError::Protocol(e) => write!(f, "protocol error: {}", e),
            PgError::Transport(e) => write!(f, "transport error: {}", e),
            PgError::Auth(msg) => write!(f, "authentication error: {}", msg),
            PgError::TypeConversion(e) => write!(f, "type conversion error: {}", e),
            PgError::Config(msg) => write!(f, "configuration error: {}", msg),
            PgError::ConnectionClosed => write!(f, "connection closed"),
            PgError::UnexpectedNull { column } => write!(f, "unexpected NULL in column {}", column),
            PgError::ColumnNotFound { name } => write!(f, "column not found: {}", name),
            PgError::ColumnIndexOutOfBounds { index, count } => {
                write!(
                    f,
                    "column index {} out of bounds (have {} columns)",
                    index, count
                )
            }
            PgError::Timeout => write!(f, "operation timed out"),
            PgError::Pool(msg) => write!(f, "pool error: {}", msg),
            PgError::InvalidState(msg) => write!(f, "invalid connection state: {}", msg),
            PgError::Unsupported(msg) => write!(f, "unsupported: {}", msg),
            PgError::Cancelled => write!(f, "cancelled"),
            PgError::Io(e) => write!(f, "I/O error: {}", e),
            #[cfg(feature = "tls")]
            PgError::Tls(e) => write!(f, "TLS error: {}", e),
            PgError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for PgError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PgError::Server(e) => Some(e.as_ref()),
            PgError::Protocol(e) => Some(e),
            PgError::Transport(e) => Some(e),
            PgError::TypeConversion(e) => Some(e),
            PgError::Io(e) => Some(e),
            #[cfg(feature = "tls")]
            PgError::Tls(e) => Some(e),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// From impls
// ---------------------------------------------------------------------------

impl From<PgServerError> for PgError {
    fn from(e: PgServerError) -> Self {
        PgError::Server(Box::new(e))
    }
}

impl From<ProtocolError> for PgError {
    fn from(e: ProtocolError) -> Self {
        PgError::Protocol(e)
    }
}

impl From<TransportError> for PgError {
    fn from(e: TransportError) -> Self {
        PgError::Transport(e)
    }
}

impl From<TypeConversionError> for PgError {
    fn from(e: TypeConversionError) -> Self {
        PgError::TypeConversion(e)
    }
}

impl From<std::io::Error> for PgError {
    fn from(e: std::io::Error) -> Self {
        PgError::Io(e)
    }
}

#[cfg(feature = "tls")]
impl From<rustls::Error> for PgError {
    fn from(e: rustls::Error) -> Self {
        PgError::Tls(e)
    }
}

/// Backward-compatible alias for [`PgError`].
pub type Error = PgError;

// ---------------------------------------------------------------------------
// PgError methods
// ---------------------------------------------------------------------------

impl PgError {
    /// Check if the error indicates the connection is broken and cannot be
    /// recovered without reconnecting.
    pub fn is_connection_broken(&self) -> bool {
        match self {
            PgError::ConnectionClosed => true,
            PgError::Transport(TransportError::ConnectionReset) => true,
            PgError::Transport(TransportError::UnexpectedEof) => true,
            PgError::Transport(TransportError::ConnectionRefused) => true,
            PgError::Server(ref e) => {
                e.is_connection_exception() || e.is_admin_shutdown() || e.is_crash_shutdown()
            }
            PgError::Io(ref e) => {
                matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
                )
            }
            _ => false,
        }
    }

    /// Check if this error is potentially retryable without reconnecting.
    pub fn is_retryable(&self) -> bool {
        match self {
            PgError::Server(e) => e.is_serialization_failure() || e.is_deadlock_detected(),
            PgError::Transport(TransportError::Timeout) => true,
            PgError::Timeout => true,
            _ => false,
        }
    }

    /// Returns the SQLSTATE error code if this is a server error.
    pub fn code(&self) -> Option<&str> {
        match self {
            PgError::Server(e) => Some(&e.code),
            _ => None,
        }
    }

    /// Add context to an error by wrapping it in a descriptive message.
    pub fn context(self, msg: impl Into<String>) -> Self {
        PgError::Other(format!("{}: {}", msg.into(), self))
    }
}

/// A specialized `Result` type for client operations.
pub type Result<T> = std::result::Result<T, PgError>;

// ---------------------------------------------------------------------------
// Conversion from auth::AuthError
// ---------------------------------------------------------------------------

impl From<crate::auth::AuthError> for PgError {
    fn from(e: crate::auth::AuthError) -> Self {
        match e {
            crate::auth::AuthError::PasswordRequired => PgError::Auth("password required".into()),
            crate::auth::AuthError::UnsupportedSaslMechanisms(mechs) => {
                PgError::Auth(format!("unsupported SASL mechanisms: {mechs:?}"))
            }
            crate::auth::AuthError::Scram(msg) => PgError::Auth(format!("SCRAM error: {msg}")),
            crate::auth::AuthError::ServerError(msg) => PgError::Other(msg),
            crate::auth::AuthError::UnexpectedMessage => {
                PgError::Auth("unexpected message during authentication".into())
            }
            crate::auth::AuthError::Protocol(p) => PgError::Protocol(p),
            crate::auth::AuthError::Transport(t) => PgError::Transport(t),
            crate::auth::AuthError::Io(i) => PgError::Io(i),
            crate::auth::AuthError::Utf8(u) => PgError::Other(u.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn test_pg_error_display() {
        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "23505".to_string()),
            (b'M', "duplicate key".to_string()),
        ])));
        let display = err.to_string();
        assert!(display.contains("server error"));
        assert!(display.contains("duplicate key"));
        assert!(display.contains("23505"));
    }

    #[test]
    fn test_pg_error_is_connection_broken() {
        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "FATAL".to_string()),
            (b'C', "08006".to_string()),
            (b'M', "connection failure".to_string()),
        ])));
        assert!(err.is_connection_broken());

        assert!(PgError::Transport(TransportError::ConnectionReset).is_connection_broken());
        assert!(PgError::Transport(TransportError::UnexpectedEof).is_connection_broken());
        assert!(PgError::Transport(TransportError::ConnectionRefused).is_connection_broken());
        assert!(!PgError::Transport(TransportError::Timeout).is_connection_broken());
        assert!(PgError::ConnectionClosed.is_connection_broken());

        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "FATAL".to_string()),
            (b'C', "57P01".to_string()),
            (b'M', "admin shutdown".to_string()),
        ])));
        assert!(err.is_connection_broken());

        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "23505".to_string()),
            (b'M', "duplicate key".to_string()),
        ])));
        assert!(!err.is_connection_broken());
    }

    #[test]
    fn test_pg_error_is_retryable() {
        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "40001".to_string()),
            (b'M', "serialization failure".to_string()),
        ])));
        assert!(err.is_retryable());

        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "40P01".to_string()),
            (b'M', "deadlock detected".to_string()),
        ])));
        assert!(err.is_retryable());

        assert!(PgError::Transport(TransportError::Timeout).is_retryable());
        assert!(PgError::Timeout.is_retryable());

        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "23505".to_string()),
            (b'M', "duplicate key".to_string()),
        ])));
        assert!(!err.is_retryable());
        assert!(!PgError::ConnectionClosed.is_retryable());
    }

    #[test]
    fn test_pg_error_code() {
        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "23505".to_string()),
            (b'M', "duplicate key".to_string()),
        ])));
        assert_eq!(err.code(), Some("23505"));
        assert_eq!(PgError::ConnectionClosed.code(), None);
    }

    #[test]
    fn test_pg_error_context() {
        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "23505".to_string()),
            (b'M', "duplicate key".to_string()),
        ])));
        let with_ctx = err.context("inserting user");
        match with_ctx {
            PgError::Other(msg) => {
                assert!(msg.contains("inserting user"));
                assert!(msg.contains("duplicate key"));
            }
            _ => panic!("expected Other variant with context"),
        }
    }

    #[test]
    fn test_from_protocol_error() {
        let err = PgError::from(ProtocolError::UnexpectedEof);
        assert!(matches!(err, PgError::Protocol(_)));
    }

    #[test]
    fn test_from_transport_error() {
        let err = PgError::from(TransportError::ConnectionRefused);
        assert!(matches!(err, PgError::Transport(_)));
    }

    #[test]
    fn test_from_io_error() {
        let err = PgError::from(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "broken",
        ));
        assert!(matches!(err, PgError::Io(_)));
    }

    #[test]
    fn test_io_error_is_connection_broken() {
        let err = PgError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "broken",
        ));
        assert!(err.is_connection_broken());

        let err = PgError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert!(err.is_connection_broken());

        let err = PgError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
        assert!(!err.is_connection_broken());
    }

    #[test]
    fn test_error_source_chain() {
        let err = PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "23505".to_string()),
            (b'M', "duplicate key".to_string()),
        ])));
        assert!(err.source().is_some());

        let err = PgError::Transport(TransportError::ConnectionReset);
        assert!(err.source().is_some());

        let err = PgError::ConnectionClosed;
        assert!(err.source().is_none());
    }

    #[test]
    fn test_unexpected_null_display() {
        let err = PgError::UnexpectedNull {
            column: "id".to_string(),
        };
        assert_eq!(err.to_string(), "unexpected NULL in column id");
    }

    #[test]
    fn test_column_not_found_display() {
        let err = PgError::ColumnNotFound {
            name: "email".to_string(),
        };
        assert_eq!(err.to_string(), "column not found: email");
    }

    #[test]
    fn test_column_index_out_of_bounds_display() {
        let err = PgError::ColumnIndexOutOfBounds { index: 5, count: 3 };
        assert!(err.to_string().contains("5"));
        assert!(err.to_string().contains("3"));
    }
}
