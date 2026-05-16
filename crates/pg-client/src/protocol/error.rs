#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("unexpected EOF")]
    UnexpectedEof,

    #[error("unknown message type: {0}")]
    UnknownMessageType(u8),

    #[error("invalid message length")]
    InvalidLength,

    #[error("utf8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("protocol violation: {0}")]
    ProtocolViolation(String),

    #[error("buffered protocol data exceeded limit: {actual} > {limit} bytes")]
    BufferLimitExceeded { limit: usize, actual: usize },

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
