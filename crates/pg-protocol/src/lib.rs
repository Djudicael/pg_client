//! PostgreSQL wire protocol codec (I/O-free, synchronous).
//!
//! This crate is a thin, ergonomic wrapper around the [`postgres-protocol`](https://docs.rs/postgres-protocol)
//! crate.  It re-exports the battle-tested message serialization / deserialization
//! routines and adds a small ergonomic layer (enums, buffer management, error
//! types) so the rest of the workspace does not need to depend on
//! `postgres-protocol` directly.
//!
//! # Design
//!
//! - **I/O-free** — operates on `bytes::BytesMut` only.
//! - **Async-agnostic** — the caller is responsible for reading / writing bytes
//!   from the network; this crate only frames messages.
//!
//! # Example
//! ```
//! use bytes::BytesMut;
//! use pg_protocol::{FrontendMessage, MessageEncoder, ProtocolError};
//!
//! let mut buf = BytesMut::new();
//! MessageEncoder::encode(
//!     &FrontendMessage::Query { sql: "SELECT 1".into() },
//!     &mut buf,
//! )?;
//! # Ok::<(), ProtocolError>(())
//! ```

pub mod backend;
pub mod buffer;
pub mod error;
pub mod frontend;
pub mod types;

// Re-export commonly used items.
pub use backend::BackendMessage;
pub use buffer::MessageBuffer;
pub use error::ProtocolError;
pub use frontend::{FrontendMessage, MessageEncoder};
pub use postgres_protocol::Oid;
pub use types::*;

// Re-export authentication helpers so `pg-client` does not need a direct
// dependency on `postgres-protocol`.
pub use postgres_protocol::authentication;

/// The protocol version number for PostgreSQL 3.0 (the current protocol).
pub const PROTOCOL_VERSION: i32 = 196_608; // 3.0 in PostgreSQL's encoding (3 << 16)

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use fallible_iterator::FallibleIterator;

    // ========================================================================
    // Frontend encoding tests
    // ========================================================================

    #[test]
    fn encode_query() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::Query {
                sql: "SELECT 1".into(),
            },
            &mut buf,
        )
        .unwrap();

        assert_eq!(buf[0], b'Q');
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        assert_eq!(len, buf.len() - 1);
        assert_eq!(&buf[5..buf.len() - 1], b"SELECT 1");
        assert_eq!(buf[buf.len() - 1], 0);
    }

    #[test]
    fn encode_startup() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::Startup {
                params: vec![
                    ("user".into(), "postgres".into()),
                    ("database".into(), "test".into()),
                ],
            },
            &mut buf,
        )
        .unwrap();

        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(len, buf.len());
        let version = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(version, PROTOCOL_VERSION);
    }

    #[test]
    fn encode_terminate() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(&FrontendMessage::Terminate, &mut buf).unwrap();
        assert_eq!(buf[0], b'X');
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn encode_sync_flush() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(&FrontendMessage::Sync, &mut buf).unwrap();
        assert_eq!(buf[0], b'S');

        let mut buf = BytesMut::new();
        MessageEncoder::encode(&FrontendMessage::Flush, &mut buf).unwrap();
        assert_eq!(buf[0], b'H');
    }

    #[test]
    fn encode_password_message() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::PasswordMessage {
                password: b"secret".to_vec(),
            },
            &mut buf,
        )
        .unwrap();

        assert_eq!(buf[0], b'p');
        assert_eq!(&buf[5..buf.len() - 1], b"secret");
        assert_eq!(buf[buf.len() - 1], 0);
    }

    #[test]
    fn encode_ssl_request() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(&FrontendMessage::SslRequest, &mut buf).unwrap();
        assert_eq!(buf.len(), 8);
        let code = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(code, 80_877_103);
    }

    #[test]
    fn encode_cancel_request() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::CancelRequest {
                process_id: 1234,
                secret_key: 5678,
            },
            &mut buf,
        )
        .unwrap();

        assert_eq!(buf.len(), 16);
        let code = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(code, 80_877_102);
        let pid = i32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        assert_eq!(pid, 1234);
        let key = i32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        assert_eq!(key, 5678);
    }

    #[test]
    fn encode_parse() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::Parse {
                name: "stmt".into(),
                sql: "SELECT $1".into(),
                param_types: vec![23],
            },
            &mut buf,
        )
        .unwrap();

        assert_eq!(buf[0], b'P');
    }

    #[test]
    fn encode_execute() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::Execute {
                portal: "".into(),
                max_rows: 0,
            },
            &mut buf,
        )
        .unwrap();

        assert_eq!(buf[0], b'E');
    }

    #[test]
    fn encode_describe_and_close() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::Describe {
                variant: b'S',
                name: "stmt".into(),
            },
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf[0], b'D');

        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::Close {
                variant: b'P',
                name: "portal".into(),
            },
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf[0], b'C');
    }

    #[test]
    fn encode_sasl_messages() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::SaslInitialResponse {
                mechanism: "SCRAM-SHA-256".into(),
                data: b"client-first".to_vec(),
            },
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf[0], b'p');

        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::SaslResponse {
                data: b"client-final".to_vec(),
            },
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf[0], b'p');
    }

    // ========================================================================
    // Backend decoding tests
    // ========================================================================

    #[test]
    fn decode_authentication_ok() {
        let buf = BytesMut::from(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0][..]);
        let msg = MessageBuffer::from_bytesmut(buf)
            .next_message()
            .unwrap()
            .unwrap();
        assert!(matches!(msg, BackendMessage::AuthenticationOk));
    }

    #[test]
    fn decode_authentication_md5() {
        let buf = BytesMut::from(&[b'R', 0, 0, 0, 12, 0, 0, 0, 5, 1, 2, 3, 4][..]);
        let msg = MessageBuffer::from_bytesmut(buf)
            .next_message()
            .unwrap()
            .unwrap();
        match msg {
            BackendMessage::AuthenticationMd5Password(body) => {
                assert_eq!(body.salt(), [1, 2, 3, 4]);
            }
            _ => panic!("expected AuthenticationMd5Password, got wrong variant"),
        }
    }

    #[test]
    fn decode_parameter_status() {
        let mut raw = vec![b'S', 0, 0, 0, 24];
        raw.extend_from_slice(b"server_version\0");
        raw.extend_from_slice(b"16.0\0");
        let buf = BytesMut::from(&raw[..]);
        let msg = MessageBuffer::from_bytesmut(buf)
            .next_message()
            .unwrap()
            .unwrap();
        match msg {
            BackendMessage::ParameterStatus(body) => {
                assert_eq!(body.name().unwrap(), "server_version");
                assert_eq!(body.value().unwrap(), "16.0");
            }
            _ => panic!("expected ParameterStatus, got wrong variant"),
        }
    }

    #[test]
    fn decode_ready_for_query() {
        let buf = BytesMut::from(&[b'Z', 0, 0, 0, 5, b'I'][..]);
        let msg = MessageBuffer::from_bytesmut(buf)
            .next_message()
            .unwrap()
            .unwrap();
        match msg {
            BackendMessage::ReadyForQuery(body) => {
                assert_eq!(body.status(), b'I');
            }
            _ => panic!("expected ReadyForQuery, got wrong variant"),
        }
    }

    #[test]
    fn decode_error_response() {
        let mut raw = vec![b'E', 0, 0, 0, 18];
        raw.extend_from_slice(&[b'S']);
        raw.extend_from_slice(b"ERROR\0");
        raw.extend_from_slice(&[b'M']);
        raw.extend_from_slice(b"boom\0");
        raw.push(0);
        let buf = BytesMut::from(&raw[..]);
        let msg = MessageBuffer::from_bytesmut(buf)
            .next_message()
            .unwrap()
            .unwrap();
        match msg {
            BackendMessage::ErrorResponse(body) => {
                let fields: Vec<_> = body.fields().collect().unwrap();
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].type_(), b'S');
                assert_eq!(fields[0].value_bytes(), b"ERROR");
                assert_eq!(fields[1].type_(), b'M');
                assert_eq!(fields[1].value_bytes(), b"boom");
            }
            _ => panic!("expected ErrorResponse, got wrong variant"),
        }
    }

    #[test]
    fn decode_command_complete() {
        let mut raw = vec![b'C', 0, 0, 0, 13];
        raw.extend_from_slice(b"SELECT 1\0");
        let buf = BytesMut::from(&raw[..]);
        let msg = MessageBuffer::from_bytesmut(buf)
            .next_message()
            .unwrap()
            .unwrap();
        match msg {
            BackendMessage::CommandComplete(body) => {
                assert_eq!(body.tag().unwrap(), "SELECT 1");
            }
            _ => panic!("expected CommandComplete, got wrong variant"),
        }
    }

    // ========================================================================
    // Buffer management tests
    // ========================================================================

    #[test]
    fn buffer_partial_message() {
        let mut buf = MessageBuffer::new();
        buf.extend(&[b'R', 0, 0]);
        assert!(buf.next_message().unwrap().is_none());
        assert_eq!(buf.len(), 3);

        buf.extend(&[0, 8, 0, 0, 0, 0]);
        let msg = buf.next_message().unwrap().unwrap();
        assert!(matches!(msg, BackendMessage::AuthenticationOk));
        assert!(buf.is_empty());
    }

    #[test]
    fn buffer_multiple_messages() {
        let mut buf = MessageBuffer::new();
        buf.extend(&[
            b'R', 0, 0, 0, 8, 0, 0, 0, 0, // AuthOk
            b'Z', 0, 0, 0, 5, b'I', // ReadyForQuery
        ]);

        let msg1 = buf.next_message().unwrap().unwrap();
        assert!(matches!(msg1, BackendMessage::AuthenticationOk));

        let msg2 = buf.next_message().unwrap().unwrap();
        assert!(matches!(msg2, BackendMessage::ReadyForQuery(_)));

        assert!(buf.is_empty());
    }

    #[test]
    fn buffer_clear() {
        let mut buf = MessageBuffer::new();
        buf.extend(b"hello");
        assert_eq!(buf.len(), 5);
        buf.clear();
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_unknown_message_type() {
        let buf = BytesMut::from(&[b'!', 0, 0, 0, 4][..]);
        let result = (MessageBuffer::from_bytesmut(buf)).next_message();
        match result {
            Err(ProtocolError::Io(_)) => {}
            _ => panic!("expected Io error, got unexpected result"),
        }
    }

    #[test]
    fn format_code_roundtrip() {
        assert_eq!(FormatCode::from_u16(0), Some(FormatCode::Text));
        assert_eq!(FormatCode::from_u16(1), Some(FormatCode::Binary));
        assert_eq!(FormatCode::from_u16(2), None);
        assert_eq!(FormatCode::Text.to_u16(), 0);
        assert_eq!(FormatCode::Binary.to_u16(), 1);
    }

    #[test]
    fn transaction_status_roundtrip() {
        assert_eq!(
            TransactionStatus::from_u8(b'I'),
            Some(TransactionStatus::Idle)
        );
        assert_eq!(
            TransactionStatus::from_u8(b'T'),
            Some(TransactionStatus::InTransaction)
        );
        assert_eq!(
            TransactionStatus::from_u8(b'E'),
            Some(TransactionStatus::Failed)
        );
        assert_eq!(TransactionStatus::from_u8(b'X'), None);
        assert_eq!(TransactionStatus::Idle.to_u8(), b'I');
        assert_eq!(TransactionStatus::InTransaction.to_u8(), b'T');
    }

    #[test]
    fn encode_bind_with_params() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::Bind {
                portal: "".into(),
                statement: "".into(),
                param_formats: vec![FormatCode::Text],
                params: vec![Some(b"42".to_vec()), None],
                result_formats: vec![FormatCode::Binary],
            },
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf[0], b'B');
    }

    #[test]
    fn decode_error_response_followed_by_ready() {
        // ErrorResponse: type(1) + length(4) + S-field(7) + M-field(6) + terminator(1) = 19 bytes
        // length = 19 - 1 = 18 = 0x12 (length excludes type byte)
        let mut raw = vec![b'E', 0, 0, 0, 18];
        raw.extend_from_slice(&[b'S']);
        raw.extend_from_slice(b"ERROR\0");
        raw.extend_from_slice(&[b'M']);
        raw.extend_from_slice(b"boom\0");
        raw.push(0);
        // Followed by ReadyForQuery: type(1) + length(4) + status(1) = 6 bytes
        raw.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
        let buf = BytesMut::from(&raw[..]);
        let mut mb = MessageBuffer::from_bytesmut(buf);

        let msg1 = mb.next_message().unwrap().unwrap();
        assert!(matches!(msg1, BackendMessage::ErrorResponse(_)));
        assert_eq!(
            mb.len(),
            6,
            "ReadyForQuery should remain in buffer, got {} bytes",
            mb.len()
        );

        let msg2 = mb.next_message().unwrap().unwrap();
        assert!(matches!(msg2, BackendMessage::ReadyForQuery(_)));
        assert!(mb.is_empty());
    }

    #[test]
    fn encode_copy_data_done_fail() {
        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::CopyData {
                data: b"hello".to_vec(),
            },
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf[0], b'd');

        let mut buf = BytesMut::new();
        MessageEncoder::encode(&FrontendMessage::CopyDone, &mut buf).unwrap();
        assert_eq!(buf[0], b'c');

        let mut buf = BytesMut::new();
        MessageEncoder::encode(
            &FrontendMessage::CopyFail {
                message: "oops".into(),
            },
            &mut buf,
        )
        .unwrap();
        assert_eq!(buf[0], b'f');
    }
}
