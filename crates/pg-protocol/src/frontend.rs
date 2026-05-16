//! Encoding of PostgreSQL wire protocol **frontend** messages (client → server).
//!
//! The free functions in this module delegate to `postgres_protocol::message::frontend`
//! but expose a strongly-typed `FrontendMessage` enum so callers do not have to
//! call individual encoding functions manually.

use bytes::BytesMut;
use postgres_protocol::IsNull;

use crate::error::ProtocolError;
use crate::types::FormatCode;
use crate::Oid;

/// A strongly-typed frontend message.
///
/// Each variant maps 1-to-1 to a PostgreSQL frontend message type.
#[derive(Debug, Clone, PartialEq)]
pub enum FrontendMessage {
    /// Startup message (protocol version 3.0 + connection parameters).
    Startup {
        /// Key-value parameters such as `user`, `database`, `client_encoding`, …
        params: Vec<(String, String)>,
    },

    /// SSL/TLS negotiation request (sent before startup).
    SslRequest,

    /// Cancel an in-progress query on another connection.
    CancelRequest {
        /// The process ID obtained from `BackendKeyData`.
        process_id: i32,
        /// The secret key obtained from `BackendKeyData`.
        secret_key: i32,
    },

    /// Simple query (type `Q`).
    Query {
        /// The SQL statement.
        sql: String,
    },

    /// Extended query: parse a statement (type `P`).
    Parse {
        /// Name of the prepared statement (empty = unnamed).
        name: String,
        /// SQL string.
        sql: String,
        /// OIDs of parameter types (may be empty).
        param_types: Vec<Oid>,
    },

    /// Extended query: bind parameters to a prepared statement (type `B`).
    Bind {
        /// Name of the destination portal (empty = unnamed).
        portal: String,
        /// Name of the source prepared statement (empty = unnamed).
        statement: String,
        /// Format codes for parameters (one per parameter, or one for all).
        param_formats: Vec<FormatCode>,
        /// Parameter values, already serialized to bytes (`None` = NULL).
        params: Vec<Option<Vec<u8>>>,
        /// Desired result-column format codes.
        result_formats: Vec<FormatCode>,
    },

    /// Extended query: describe a prepared statement or portal (type `D`).
    Describe {
        /// `'S'` = prepared statement, `'P'` = portal.
        variant: u8,
        /// Name of the object to describe.
        name: String,
    },

    /// Extended query: execute a portal (type `E`).
    Execute {
        /// Name of the portal to execute (empty = unnamed).
        portal: String,
        /// Maximum rows to return (`0` = unlimited).
        max_rows: i32,
    },

    /// Extended query: close a prepared statement or portal (type `C`).
    Close {
        /// `'S'` = prepared statement, `'P'` = portal.
        variant: u8,
        /// Name of the object to close.
        name: String,
    },

    /// End the extended-query sub-protocol and commit it (type `S`).
    Sync,

    /// Flush any pending output to the server (type `H`).
    Flush,

    /// Copy data chunk (type `d`).
    CopyData {
        /// Raw data bytes.
        data: Vec<u8>,
    },

    /// End of copy-in data stream (type `c`).
    CopyDone,

    /// Abort a copy-in operation (type `f`).
    CopyFail {
        /// Human-readable reason.
        message: String,
    },

    /// Password response (type `p`).
    PasswordMessage {
        /// Password bytes (may include MD5 hash prefix).
        password: Vec<u8>,
    },

    /// SASL initial response (type `p`).
    SaslInitialResponse {
        /// Mechanism name (e.g. `"SCRAM-SHA-256"`).
        mechanism: String,
        /// Initial client data.
        data: Vec<u8>,
    },

    /// SASL continuation / final response (type `p`).
    SaslResponse {
        /// Client data.
        data: Vec<u8>,
    },

    /// Gracefully close the connection (type `X`).
    Terminate,
}

/// Encoder that writes a [`FrontendMessage`] into a [`BytesMut`] buffer.
#[derive(Debug)]
pub struct MessageEncoder;

impl MessageEncoder {
    /// Serialize `msg` and append it to `buf`.
    ///
    /// # Errors
    /// Returns a [`ProtocolError`] if the message contains invalid data
    /// (e.g. a C string with an embedded NUL byte).
    pub fn encode(msg: &FrontendMessage, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        match msg {
            FrontendMessage::Startup { params } => {
                let pairs: Vec<(&str, &str)> = params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                postgres_protocol::message::frontend::startup_message(pairs, buf)?;
            }
            FrontendMessage::SslRequest => {
                postgres_protocol::message::frontend::ssl_request(buf);
            }
            FrontendMessage::CancelRequest {
                process_id,
                secret_key,
            } => {
                postgres_protocol::message::frontend::cancel_request(*process_id, *secret_key, buf);
            }
            FrontendMessage::Query { sql } => {
                postgres_protocol::message::frontend::query(sql, buf)?;
            }
            FrontendMessage::Parse {
                name,
                sql,
                param_types,
            } => {
                postgres_protocol::message::frontend::parse(
                    name,
                    sql,
                    param_types.iter().copied(),
                    buf,
                )?;
            }
            FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            } => {
                let pf: Vec<i16> = param_formats.iter().map(|f| *f as i16).collect();
                let rf: Vec<i16> = result_formats.iter().map(|f| *f as i16).collect();
                postgres_protocol::message::frontend::bind(
                    portal,
                    statement,
                    pf,
                    params.iter().map(|p| p.as_ref().map(|v| v.as_slice())),
                    |item, buf| match item {
                        Some(data) => {
                            buf.extend_from_slice(data);
                            Ok::<_, Box<dyn std::error::Error + Sync + Send>>(IsNull::No)
                        }
                        None => Ok(IsNull::Yes),
                    },
                    rf,
                    buf,
                )
                .map_err(|_e| {
                    ProtocolError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "bind encoding failed",
                    ))
                })?;
            }
            FrontendMessage::Describe { variant, name } => {
                postgres_protocol::message::frontend::describe(*variant, name, buf)?;
            }
            FrontendMessage::Execute { portal, max_rows } => {
                postgres_protocol::message::frontend::execute(portal, *max_rows, buf)?;
            }
            FrontendMessage::Close { variant, name } => {
                postgres_protocol::message::frontend::close(*variant, name, buf)?;
            }
            FrontendMessage::Sync => {
                postgres_protocol::message::frontend::sync(buf);
            }
            FrontendMessage::Flush => {
                postgres_protocol::message::frontend::flush(buf);
            }
            FrontendMessage::CopyData { data } => {
                postgres_protocol::message::frontend::CopyData::new(data.as_slice())?.write(buf);
            }
            FrontendMessage::CopyDone => {
                postgres_protocol::message::frontend::copy_done(buf);
            }
            FrontendMessage::CopyFail { message } => {
                postgres_protocol::message::frontend::copy_fail(message, buf)?;
            }
            FrontendMessage::PasswordMessage { password } => {
                postgres_protocol::message::frontend::password_message(password, buf)?;
            }
            FrontendMessage::SaslInitialResponse { mechanism, data } => {
                postgres_protocol::message::frontend::sasl_initial_response(mechanism, data, buf)?;
            }
            FrontendMessage::SaslResponse { data } => {
                postgres_protocol::message::frontend::sasl_response(data, buf)?;
            }
            FrontendMessage::Terminate => {
                postgres_protocol::message::frontend::terminate(buf);
            }
        }
        Ok(())
    }
}
