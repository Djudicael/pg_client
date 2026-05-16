//! PostgreSQL authentication (async).
//!
//! This module implements the client side of PostgreSQL authentication:
//! Trust, Cleartext Password, MD5, and SCRAM-SHA-256.
//!
//! All network I/O is async; all cryptography is sync pure-Rust.

mod cleartext;
mod md5;
mod scram;

use std::collections::HashMap;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use crate::protocol::{BackendMessage, FrontendMessage, MessageBuffer, MessageEncoder, ProtocolError};

use crate::config::Config;
use crate::transport::{AsyncTransport, TransportError};

#[cfg(feature = "tracing")]
use crate::tracing_ext::TARGET_AUTH;

// ---------------------------------------------------------------------------
// AuthError
// ---------------------------------------------------------------------------

/// Errors that can occur during authentication.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// No password was provided but one is required.
    #[error("password required")]
    PasswordRequired,

    /// The server offered SASL mechanisms we don't support.
    #[error("unsupported SASL mechanism(s): {0:?}")]
    UnsupportedSaslMechanisms(Vec<String>),

    /// SCRAM protocol error.
    #[error("SCRAM error: {0}")]
    Scram(String),

    /// The server requested an authentication method that is not allowed on
    /// the current transport security level.
    #[error("refusing {method} authentication over an unencrypted transport; enable TLS or explicitly opt in to insecure auth")]
    InsecureAuthentication { method: &'static str },

    /// The server returned an error response during authentication.
    #[error("server error: {0}")]
    ServerError(String),

    /// An unexpected message was received during authentication.
    #[error("unexpected message during authentication")]
    UnexpectedMessage,

    /// Protocol codec error.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// Transport I/O error.
    #[error(transparent)]
    Transport(#[from] TransportError),

    /// std::io error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// UTF-8 conversion error.
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
}

// ---------------------------------------------------------------------------
// ServerParams
// ---------------------------------------------------------------------------

/// Parameters sent by the server after a successful authentication.
#[derive(Debug, Clone, Default)]
pub struct ServerParams {
    /// Backend process ID (from `BackendKeyData`).
    pub process_id: i32,
    /// Secret key (from `BackendKeyData`).
    pub secret_key: i32,
    /// Server version string.
    pub server_version: String,
    /// Server encoding.
    pub server_encoding: String,
    /// Client encoding reported by the server.
    pub client_encoding: String,
    /// All other parameter-status key/value pairs.
    pub params: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Codec — thin wrapper that bridges AsyncTransport ↔ pg-protocol
// ---------------------------------------------------------------------------

/// A small helper that reads raw bytes from an [`AsyncTransport`], parses
/// backend messages, and encodes / writes frontend messages.
pub struct Codec {
    read_buf: MessageBuffer,
    write_buf: BytesMut,
}

impl Codec {
    /// Create a new codec with default buffers.
    pub fn new() -> Self {
        Self {
            read_buf: MessageBuffer::new(),
            write_buf: BytesMut::with_capacity(4096),
        }
    }

    /// Read the next complete backend message from `transport`.
    ///
    /// This method blocks (async) until a full message has been received.
    pub async fn read_message<T: AsyncTransport>(
        &mut self,
        transport: &mut T,
    ) -> Result<BackendMessage, AuthError> {
        loop {
            if let Some(msg) = self.read_buf.next_message()? {
                #[cfg(feature = "tracing")]
                tracing::trace!(
                    target: crate::tracing_ext::TARGET_PROTOCOL,
                    direction = "recv",
                    message_type = backend_message_type_name(&msg),
                    "Received backend message"
                );
                return Ok(msg);
            }
            let mut tmp = [0u8; 4096];
            let n = transport.read(&mut tmp).await?;
            if n == 0 {
                return Err(AuthError::Transport(TransportError::UnexpectedEof));
            }
            self.read_buf.try_extend(&tmp[..n])?;
        }
    }

    /// Encode `msg` and write it to `transport`, then flush immediately.
    ///
    /// Use this for standalone messages that need to be sent right away
    /// (e.g. during authentication, simple query, etc.).
    pub async fn send<T: AsyncTransport>(
        &mut self,
        transport: &mut T,
        msg: &FrontendMessage,
    ) -> Result<(), AuthError> {
        #[cfg(feature = "tracing")]
        tracing::trace!(
            target: crate::tracing_ext::TARGET_PROTOCOL,
            direction = "send",
            message_type = frontend_message_type_name(msg),
            "Sending frontend message"
        );
        self.write_buf.clear();
        MessageEncoder::encode(msg, &mut self.write_buf)?;
        transport.write_all(&self.write_buf).await?;
        transport.flush().await?;
        Ok(())
    }

    /// Encode `msg` and write it to `transport` **without flushing**.
    ///
    /// This is used for the extended query protocol where Parse, Bind,
    /// Execute, and Sync must be sent as a single batch.  Call
    /// `transport.flush()` once after all messages have been written.
    pub async fn encode_and_write<T: AsyncTransport>(
        &mut self,
        transport: &mut T,
        msg: &FrontendMessage,
    ) -> Result<(), AuthError> {
        self.write_buf.clear();
        MessageEncoder::encode(msg, &mut self.write_buf)?;
        transport.write_all(&self.write_buf).await?;
        Ok(())
    }

    /// Encode `msg` into the internal write buffer without writing to transport.
    ///
    /// This is used by the COPY protocol recovery code in `Drop` implementations
    /// where async I/O is not available. The encoded message sits in the buffer
    /// until the next `send()`, `encode_and_write()`, or until the transport
    /// is flushed.
    pub fn encode_to_buffer(&mut self, msg: &FrontendMessage) -> Result<(), AuthError> {
        self.write_buf.clear();
        MessageEncoder::encode(msg, &mut self.write_buf)?;
        Ok(())
    }

    /// Returns a reference to the internal write buffer.
    ///
    /// Used by sync COPY recovery to access the encoded message bytes.
    pub fn write_buffer(&self) -> &[u8] {
        &self.write_buf
    }
}

impl Default for Codec {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// authenticate — main dispatcher
// ---------------------------------------------------------------------------

/// Perform the PostgreSQL authentication handshake.
///
/// After the startup message has been sent, call this function. It will
/// handle all authentication methods (Trust, Cleartext, MD5, SCRAM-SHA-256)
/// and then read the post-auth startup parameters (`BackendKeyData`,
/// `ParameterStatus`, …) until `ReadyForQuery`.
pub async fn authenticate<T: AsyncTransport>(
    transport: &mut T,
    codec: &mut Codec,
    config: &Config,
) -> Result<ServerParams, AuthError> {
    loop {
        let msg = codec.read_message(transport).await?;
        match msg {
            BackendMessage::AuthenticationOk => {
                #[cfg(feature = "tracing")]
                tracing::info!(target: TARGET_AUTH, "Authentication successful");
                break;
            }
            BackendMessage::AuthenticationCleartextPassword => {
                #[cfg(feature = "tracing")]
                tracing::debug!(target: TARGET_AUTH, method = "cleartext", "Server requested cleartext password authentication");
                if !transport.is_secure() && !config.get_allow_insecure_auth() {
                    return Err(AuthError::InsecureAuthentication {
                        method: "cleartext password",
                    });
                }
                cleartext::auth(transport, codec, config).await?;
            }
            BackendMessage::AuthenticationMd5Password(body) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(target: TARGET_AUTH, method = "md5", "Server requested MD5 password authentication");
                if !transport.is_secure() && !config.get_allow_insecure_auth() {
                    return Err(AuthError::InsecureAuthentication { method: "MD5" });
                }
                md5::auth(transport, codec, config, body.salt()).await?;
            }
            BackendMessage::AuthenticationSasl(body) => {
                let mechanisms: Vec<String> =
                    body.mechanisms().map(|m| Ok(m.to_string())).collect()?;
                #[cfg(feature = "tracing")]
                tracing::debug!(target: TARGET_AUTH, method = "scram-sha-256", "Server requested SASL authentication");
                scram::auth(transport, codec, config, &mechanisms).await?;
            }
            BackendMessage::ErrorResponse(body) => {
                #[cfg(feature = "tracing")]
                tracing::error!(target: TARGET_AUTH, "Authentication failed: server returned error");
                let msg = format_error_fields(&body)?;
                return Err(AuthError::ServerError(msg));
            }
            _ => {
                #[cfg(feature = "tracing")]
                tracing::error!(target: TARGET_AUTH, "Unexpected message during authentication");
                return Err(AuthError::UnexpectedMessage);
            }
        }
    }

    read_startup_params(transport, codec).await
}

// ---------------------------------------------------------------------------
// read_startup_params
// ---------------------------------------------------------------------------

async fn read_startup_params<T: AsyncTransport>(
    transport: &mut T,
    codec: &mut Codec,
) -> Result<ServerParams, AuthError> {
    let mut params = ServerParams::default();

    loop {
        let msg = codec.read_message(transport).await?;
        match msg {
            BackendMessage::BackendKeyData(body) => {
                params.process_id = body.process_id();
                params.secret_key = body.secret_key();
            }
            BackendMessage::ParameterStatus(body) => {
                let name = body.name()?;
                let value = body.value()?;
                match name {
                    "server_version" => params.server_version = value.to_string(),
                    "server_encoding" => params.server_encoding = value.to_string(),
                    "client_encoding" => params.client_encoding = value.to_string(),
                    _ => {}
                }
                params.params.insert(name.to_string(), value.to_string());
            }
            BackendMessage::ReadyForQuery(_) => break,
            BackendMessage::ErrorResponse(body) => {
                let msg = format_error_fields(&body)?;
                return Err(AuthError::ServerError(msg));
            }
            _ => {}
        }
    }

    Ok(params)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the primary human-readable message from an `ErrorResponse` body.
fn format_error_fields(
    body: &crate::protocol::backend::ErrorResponseBody,
) -> Result<String, AuthError> {
    let mut msg = String::new();
    let mut fields = body.fields();
    loop {
        match fields.next() {
            Ok(Some(field)) => {
                if field.type_() == b'M' {
                    if let Ok(v) = std::str::from_utf8(field.value_bytes()) {
                        if !msg.is_empty() {
                            msg.push_str("; ");
                        }
                        msg.push_str(v);
                    }
                }
            }
            Ok(None) => break,
            Err(e) => return Err(AuthError::Io(e)),
        }
    }
    Ok(msg)
}

// ---------------------------------------------------------------------------
// Message type name helpers (for TRACE-level protocol logging)
// ---------------------------------------------------------------------------

/// Return a human-readable name for a frontend message type.
/// Used only for TRACE-level protocol tracing.
#[cfg(feature = "tracing")]
fn frontend_message_type_name(msg: &FrontendMessage) -> &'static str {
    match msg {
        FrontendMessage::Startup { .. } => "Startup",
        FrontendMessage::SslRequest => "SSLRequest",
        FrontendMessage::CancelRequest { .. } => "CancelRequest",
        FrontendMessage::Query { .. } => "Query",
        FrontendMessage::Parse { .. } => "Parse",
        FrontendMessage::Bind { .. } => "Bind",
        FrontendMessage::Describe { .. } => "Describe",
        FrontendMessage::Execute { .. } => "Execute",
        FrontendMessage::Sync => "Sync",
        FrontendMessage::Flush => "Flush",
        FrontendMessage::Close { .. } => "Close",
        FrontendMessage::Terminate => "Terminate",
        FrontendMessage::PasswordMessage { .. } => "PasswordMessage",
        FrontendMessage::SaslInitialResponse { .. } => "SASLInitialResponse",
        FrontendMessage::SaslResponse { .. } => "SASLResponse",
        FrontendMessage::CopyData { .. } => "CopyData",
        FrontendMessage::CopyDone => "CopyDone",
        FrontendMessage::CopyFail { .. } => "CopyFail",
    }
}

/// Return a human-readable name for a backend message type.
/// Used only for TRACE-level protocol tracing.
#[cfg(feature = "tracing")]
fn backend_message_type_name(msg: &BackendMessage) -> &'static str {
    match msg {
        BackendMessage::AuthenticationOk => "AuthenticationOk",
        BackendMessage::AuthenticationCleartextPassword => "AuthenticationCleartextPassword",
        BackendMessage::AuthenticationMd5Password(_) => "AuthenticationMD5Password",
        BackendMessage::AuthenticationSasl(_) => "AuthenticationSASL",
        BackendMessage::AuthenticationSaslContinue(_) => "AuthenticationSASLContinue",
        BackendMessage::AuthenticationSaslFinal(_) => "AuthenticationSASLFinal",
        BackendMessage::BackendKeyData(_) => "BackendKeyData",
        BackendMessage::ParameterStatus(_) => "ParameterStatus",
        BackendMessage::ReadyForQuery(_) => "ReadyForQuery",
        BackendMessage::RowDescription(_) => "RowDescription",
        BackendMessage::DataRow(_) => "DataRow",
        BackendMessage::CommandComplete(_) => "CommandComplete",
        BackendMessage::EmptyQueryResponse => "EmptyQueryResponse",
        BackendMessage::ParseComplete => "ParseComplete",
        BackendMessage::BindComplete => "BindComplete",
        BackendMessage::CloseComplete => "CloseComplete",
        BackendMessage::NoData => "NoData",
        BackendMessage::ParameterDescription(_) => "ParameterDescription",
        BackendMessage::PortalSuspended => "PortalSuspended",
        BackendMessage::CopyInResponse(_) => "CopyInResponse",
        BackendMessage::CopyOutResponse(_) => "CopyOutResponse",
        BackendMessage::CopyData(_) => "CopyData",
        BackendMessage::CopyDone => "CopyDone",
        BackendMessage::ErrorResponse(_) => "ErrorResponse",
        BackendMessage::NoticeResponse(_) => "NoticeResponse",
        BackendMessage::NotificationResponse(_) => "NotificationResponse",
        _ => "Unknown",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::MockTransport;

    #[tokio::test]
    async fn test_authenticate_trust() {
        // Build the server response bytes manually
        let mut response = Vec::new();
        // AuthenticationOk
        response.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
        // ParameterStatus server_version
        let mut ps = vec![b'S', 0, 0, 0, 24];
        ps.extend_from_slice(b"server_version\0");
        ps.extend_from_slice(b"16.0\0");
        response.extend_from_slice(&ps);
        // ReadyForQuery
        response.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);

        let mut mock = MockTransport::new(response);
        let mut codec = Codec::new();
        let config = Config::new().user("postgres").database("test");

        let params = authenticate(&mut mock, &mut codec, &config).await.unwrap();
        assert_eq!(params.server_version, "16.0");
    }

    #[tokio::test]
    async fn test_authenticate_cleartext() {
        let mut response = Vec::new();
        // AuthenticationCleartextPassword
        response.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 3]);
        // AuthenticationOk
        response.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
        // ReadyForQuery
        response.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);

        let mut mock = MockTransport::new(response);
        let mut codec = Codec::new();
        let config = Config::new()
            .user("postgres")
            .password("secret")
            .allow_insecure_auth(true);

        let _params = authenticate(&mut mock, &mut codec, &config).await.unwrap();
        // Verify a PasswordMessage was sent
        let written = mock.written();
        assert_eq!(written[0], b'p');
    }

    #[tokio::test]
    async fn test_authenticate_md5() {
        let mut response = Vec::new();
        // AuthenticationMD5Password with salt [1,2,3,4]
        response.extend_from_slice(&[b'R', 0, 0, 0, 12, 0, 0, 0, 5, 1, 2, 3, 4]);
        // AuthenticationOk
        response.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
        // ReadyForQuery
        response.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);

        let mut mock = MockTransport::new(response);
        let mut codec = Codec::new();
        let config = Config::new()
            .user("postgres")
            .password("secret")
            .allow_insecure_auth(true);

        let _params = authenticate(&mut mock, &mut codec, &config).await.unwrap();
        let written = mock.written();
        assert_eq!(written[0], b'p');
        // Should contain "md5" prefix
        let password_msg = &written[5..]; // skip type + length
        assert!(password_msg.starts_with(b"md5"));
    }

    #[tokio::test]
    async fn test_authenticate_cleartext_rejected_without_tls() {
        let mut response = Vec::new();
        response.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 3]);

        let mut mock = MockTransport::new(response);
        let mut codec = Codec::new();
        let config = Config::new().user("postgres").password("secret");

        let result = authenticate(&mut mock, &mut codec, &config).await;
        assert!(matches!(
            result,
            Err(AuthError::InsecureAuthentication {
                method: "cleartext password"
            })
        ));
    }

    #[tokio::test]
    async fn test_authenticate_md5_rejected_without_tls() {
        let mut response = Vec::new();
        response.extend_from_slice(&[b'R', 0, 0, 0, 12, 0, 0, 0, 5, 1, 2, 3, 4]);

        let mut mock = MockTransport::new(response);
        let mut codec = Codec::new();
        let config = Config::new().user("postgres").password("secret");

        let result = authenticate(&mut mock, &mut codec, &config).await;
        assert!(matches!(
            result,
            Err(AuthError::InsecureAuthentication { method: "MD5" })
        ));
    }

    #[tokio::test]
    async fn test_authenticate_server_error() {
        let mut response = Vec::new();
        // ErrorResponse
        let mut err = vec![b'E', 0, 0, 0, 22];
        err.extend_from_slice(b"S");
        err.extend_from_slice(b"FATAL\0");
        err.extend_from_slice(b"M");
        err.extend_from_slice(b"bad auth\0");
        err.push(0);
        response.extend_from_slice(&err);

        let mut mock = MockTransport::new(response);
        let mut codec = Codec::new();
        let config = Config::new().user("postgres");

        let result = authenticate(&mut mock, &mut codec, &config).await;
        assert!(matches!(result, Err(AuthError::ServerError(_))));
    }
}
