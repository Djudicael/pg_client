//! Protocol error types.

/// Errors that can occur during protocol encoding or decoding.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// An I/O error occurred while serializing or deserializing a message.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The connection was closed before a complete message could be read.
    #[error("unexpected EOF")]
    UnexpectedEof,

    /// An unknown or unsupported message type byte was received.
    #[error("unknown message type: {0}")]
    UnknownMessageType(u8),

    /// The message length field was invalid (e.g. too small or overflow).
    #[error("invalid message length")]
    InvalidLength,

    /// A UTF-8 conversion failed.
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    /// A protocol-level invariant was violated (e.g. unexpected message order).
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),

    /// Buffered protocol data exceeded the configured safety limit.
    #[error("buffered protocol data exceeded limit: {actual} > {limit} bytes")]
    BufferLimitExceeded { limit: usize, actual: usize },

    /// A specific PostgreSQL type could not be encoded or decoded.
    #[error("type error: {0}")]
    TypeError(String),
}

impl From<postgres_protocol::message::frontend::BindError> for ProtocolError {
    fn from(_e: postgres_protocol::message::frontend::BindError) -> Self {
        ProtocolError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "bind encoding failed",
        ))
    }
}
