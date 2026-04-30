//! Error types for the PostgreSQL client.
//!
//! This module defines the `Error` enum which represents all possible errors
//! that can occur when using the PostgreSQL client.

use std::io;

/// The main error type for the PostgreSQL client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error in the wire protocol.
    #[error("protocol error: {0}")]
    Protocol(#[from] pg_protocol::ProtocolError),

    /// An error in type conversion.
    #[error("type error: {0}")]
    Type(#[from] pg_types::Error),

    /// An I/O error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// A TLS error.
    #[cfg(feature = "tls")]
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),

    /// A DNS resolution error.
    #[error("dns error: {0}")]
    Dns(String),

    /// A connection error (e.g., connection refused, timeout).
    #[error("connection error: {0}")]
    Connection(String),

    /// An authentication error.
    #[error("authentication error: {0}")]
    Authentication(String),

    /// An error from the PostgreSQL server (e.g., error response).
    #[error("server error: {0}")]
    Server(String),

    /// An error indicating the connection is closed.
    #[error("connection closed")]
    ConnectionClosed,

    /// An error indicating the connection is in an invalid state for the operation.
    #[error("invalid connection state: {0}")]
    InvalidState(String),

    /// An error indicating a timeout.
    #[error("timeout")]
    Timeout,

    /// An error indicating that the operation was cancelled.
    #[error("cancelled")]
    Cancelled,

    /// An error indicating that the operation is not supported.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// An error indicating that a configuration is invalid.
    #[error("configuration error: {0}")]
    Config(String),

    /// An error from the underlying async runtime (if any).
    #[error("runtime error: {0}")]
    Runtime(String),

    /// Any other error.
    #[error("{0}")]
    Other(String),
}

/// A specialized `Result` type for client operations.
pub type Result<T> = std::result::Result<T, Error>;
