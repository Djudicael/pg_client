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

impl TransportError {
    /// Returns true if this error indicates the connection is broken
    /// and cannot be recovered (e.g., EOF, connection reset).
    pub fn is_connection_broken(&self) -> bool {
        matches!(
            self,
            TransportError::ConnectionReset
                | TransportError::UnexpectedEof
                | TransportError::ConnectionRefused
        )
    }

    /// Returns true if this error is potentially transient
    /// (e.g., timeout, DNS failure).
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            TransportError::Timeout | TransportError::DnsResolutionFailed { .. }
        )
    }
}
