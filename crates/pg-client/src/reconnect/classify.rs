//! Error classification for retry/reconnection decisions.
//!
//! This module defines [`ErrorClass`] and [`classify_error`] which categorize
//! PostgreSQL errors into three classes:
//! - **Broken**: The connection is definitely dead. Must reconnect.
//! - **Transient**: The error might resolve on retry. Connection may still be alive.
//! - **Permanent**: The error will not resolve on retry. Connection is still alive.

use crate::error::PgError;
use crate::transport::TransportError;

/// Classification of a PostgreSQL error for retry/reconnection decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorClass {
    /// The connection is definitely broken. Must reconnect.
    /// Examples: ConnectionClosed, ConnectionReset, UnexpectedEof.
    Broken,

    /// The error is transient and may resolve on retry.
    /// The connection is still alive.
    /// Examples: SerializationFailure, DeadlockDetected, Timeout.
    Transient,

    /// The error is permanent and will not resolve on retry.
    /// The connection is still alive.
    /// Examples: SyntaxError, PermissionDenied, UniqueViolation.
    Permanent,
}

/// Detect if an error indicates the connection is broken, transient, or permanent.
///
/// This classifies errors into three categories:
/// - **Broken**: The connection is definitely dead. Must reconnect.
/// - **Transient**: The error might resolve on retry. Connection may still be alive.
/// - **Permanent**: The error will not resolve on retry. Connection is still alive.
pub fn classify_error(err: &PgError) -> ErrorClass {
    match err {
        // Connection is definitely broken
        PgError::ConnectionClosed => ErrorClass::Broken,
        PgError::Transport(TransportError::ConnectionReset) => ErrorClass::Broken,
        PgError::Transport(TransportError::UnexpectedEof) => ErrorClass::Broken,
        PgError::Transport(TransportError::ConnectionRefused) => ErrorClass::Broken,

        // Transient errors — connection is alive, but the operation failed
        PgError::Server(ref e) if e.is_serialization_failure() => ErrorClass::Transient,
        PgError::Server(ref e) if e.is_deadlock_detected() => ErrorClass::Transient,
        // Connection exceptions and server shutdowns indicate broken connection
        PgError::Server(ref e) if e.is_connection_exception() => ErrorClass::Broken,
        PgError::Server(ref e) if e.is_admin_shutdown() => ErrorClass::Broken,
        PgError::Server(ref e) if e.is_crash_shutdown() => ErrorClass::Broken,
        PgError::Transport(TransportError::Timeout) => ErrorClass::Transient,
        PgError::Timeout => ErrorClass::Transient,

        // I/O errors that indicate broken connections
        PgError::Io(ref e) => match e.kind() {
            std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof => ErrorClass::Broken,
            std::io::ErrorKind::TimedOut => ErrorClass::Transient,
            _ => ErrorClass::Permanent,
        },

        // Permanent errors — connection is alive, operation is invalid
        PgError::Server(_) => ErrorClass::Permanent,
        PgError::TypeConversion(_) => ErrorClass::Permanent,
        PgError::Config(_) => ErrorClass::Permanent,
        PgError::Auth(_) => ErrorClass::Permanent,

        // Default: treat unknown errors as permanent
        _ => ErrorClass::Permanent,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PgServerError;

    fn make_server_error(code: &str, message: &str) -> PgError {
        PgError::Server(Box::new(PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', code.to_string()),
            (b'M', message.to_string()),
        ])))
    }

    #[test]
    fn test_classify_broken() {
        assert_eq!(
            classify_error(&PgError::ConnectionClosed),
            ErrorClass::Broken
        );
        assert_eq!(
            classify_error(&PgError::Transport(TransportError::ConnectionReset)),
            ErrorClass::Broken
        );
        assert_eq!(
            classify_error(&PgError::Transport(TransportError::UnexpectedEof)),
            ErrorClass::Broken
        );
        assert_eq!(
            classify_error(&PgError::Transport(TransportError::ConnectionRefused)),
            ErrorClass::Broken
        );

        // Connection exception (080xx SQLSTATE class)
        let err = make_server_error("08006", "connection failure");
        assert_eq!(classify_error(&err), ErrorClass::Broken);

        // Admin shutdown
        let err = make_server_error("57P01", "admin shutdown");
        assert_eq!(classify_error(&err), ErrorClass::Broken);
    }

    #[test]
    fn test_classify_transient() {
        // Serialization failure
        let err = make_server_error("40001", "could not serialize access");
        assert_eq!(classify_error(&err), ErrorClass::Transient);

        // Deadlock detected
        let err = make_server_error("40P01", "deadlock detected");
        assert_eq!(classify_error(&err), ErrorClass::Transient);

        // Transport timeout
        assert_eq!(
            classify_error(&PgError::Transport(TransportError::Timeout)),
            ErrorClass::Transient
        );

        // Generic timeout
        assert_eq!(classify_error(&PgError::Timeout), ErrorClass::Transient);
    }

    #[test]
    fn test_classify_permanent() {
        // Unique violation
        let err = make_server_error("23505", "duplicate key");
        assert_eq!(classify_error(&err), ErrorClass::Permanent);

        // Syntax error
        let err = make_server_error("42601", "syntax error");
        assert_eq!(classify_error(&err), ErrorClass::Permanent);

        // Config error
        assert_eq!(
            classify_error(&PgError::Config("bad config".into())),
            ErrorClass::Permanent
        );

        // Auth error
        assert_eq!(
            classify_error(&PgError::Auth("bad password".into())),
            ErrorClass::Permanent
        );

        // Type conversion error
        assert_eq!(
            classify_error(&PgError::TypeConversion(pg_types::Error::Conversion(
                "conversion failed".into(),
            ))),
            ErrorClass::Permanent
        );
    }

    #[test]
    fn test_classify_io_errors() {
        let broken = PgError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert_eq!(classify_error(&broken), ErrorClass::Broken);

        let broken = PgError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe"));
        assert_eq!(classify_error(&broken), ErrorClass::Broken);

        let transient = PgError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
        assert_eq!(classify_error(&transient), ErrorClass::Transient);

        let permanent = PgError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "bad input",
        ));
        assert_eq!(classify_error(&permanent), ErrorClass::Permanent);
    }

    #[test]
    fn test_classify_unknown_errors() {
        // Pool errors are treated as permanent
        assert_eq!(
            classify_error(&PgError::Pool("exhausted".into())),
            ErrorClass::Permanent
        );

        // InvalidState is permanent
        assert_eq!(
            classify_error(&PgError::InvalidState("wrong state".into())),
            ErrorClass::Permanent
        );
    }
}
