use bytes::BytesMut;
use postgres_protocol::IsNull;

use super::error::ProtocolError;
use super::types::FormatCode;
use crate::protocol::Oid;

#[derive(Debug, Clone, PartialEq)]
pub enum FrontendMessage {
    Startup {
        params: Vec<(String, String)>,
    },

    SslRequest,

    CancelRequest {
        process_id: i32,
        secret_key: i32,
    },

    Query {
        sql: String,
    },

    Parse {
        name: String,
        sql: String,
        param_types: Vec<Oid>,
    },

    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<FormatCode>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<FormatCode>,
    },

    Describe {
        variant: u8,
        name: String,
    },

    Execute {
        portal: String,
        max_rows: i32,
    },

    Close {
        variant: u8,
        name: String,
    },

    Sync,

    Flush,

    CopyData {
        data: Vec<u8>,
    },

    CopyDone,

    CopyFail {
        message: String,
    },

    PasswordMessage {
        password: Vec<u8>,
    },

    SaslInitialResponse {
        mechanism: String,
        data: Vec<u8>,
    },

    SaslResponse {
        data: Vec<u8>,
    },

    Terminate,
}

#[derive(Debug)]
pub struct MessageEncoder;

impl MessageEncoder {
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
