//! Layer 3: Protocol tests (mock transport, full flows)
//!
//! These tests exercise the PostgreSQL wire protocol encoding/decoding
//! without a real server, using the pg-protocol crate's message
//! serialization and buffer management.

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use pg_protocol::{
    BackendMessage, FormatCode, FrontendMessage, MessageBuffer, MessageEncoder,
    ProtocolError, TransactionStatus,
};

// ========================================================================
// Handshake protocol tests
// ========================================================================

#[test]
fn test_startup_message_encoding() {
    let mut buf = BytesMut::new();
    MessageEncoder::encode(
        &FrontendMessage::Startup {
            params: vec![
                ("user".into(), "postgres".into()),
                ("database".into(), "testdb".into()),
            ],
        },
        &mut buf,
    )
    .unwrap();

    // Startup message: length(4) + version(4) + params + terminator
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    assert_eq!(len, buf.len());
    // Protocol version 3.0
    assert_eq!(&buf[4..8], &[0, 3, 0, 0]);
}

#[test]
fn test_ssl_request_encoding() {
    let mut buf = BytesMut::new();
    MessageEncoder::encode(&FrontendMessage::SslRequest, &mut buf).unwrap();
    assert_eq!(buf.len(), 8);
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    assert_eq!(len, 8);
    let code = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    assert_eq!(code, 80_877_103);
}

#[test]
fn test_trust_auth_handshake_flow() {
    // Simulate a trust-auth handshake by encoding startup and decoding AuthOk
    let mut buf = BytesMut::new();

    // Client sends startup
    MessageEncoder::encode(
        &FrontendMessage::Startup {
            params: vec![("user".into(), "postgres".into())],
        },
        &mut buf,
    )
    .unwrap();
    assert!(buf.len() > 8);

    // Server responds with AuthOk
    let server_response = BytesMut::from(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0][..]);
    let mut msg_buf = MessageBuffer::from_bytesmut(server_response);
    let msg = msg_buf.next_message().unwrap().unwrap();
    assert!(matches!(msg, BackendMessage::AuthenticationOk));
}

#[test]
fn test_password_auth_flow() {
    // Client sends startup, server challenges with MD5, client responds
    let mut buf = BytesMut::new();

    // Server sends AuthenticationMd5Password
    let server_challenge = BytesMut::from(&[b'R', 0, 0, 0, 12, 0, 0, 0, 5, 1, 2, 3, 4][..]);
    let mut msg_buf = MessageBuffer::from_bytesmut(server_challenge);
    let msg = msg_buf.next_message().unwrap().unwrap();
    match msg {
        BackendMessage::AuthenticationMd5Password(body) => {
            assert_eq!(body.salt(), [1, 2, 3, 4]);
        }
        _ => panic!("expected AuthenticationMd5Password"),
    }

    // Client sends PasswordMessage
    MessageEncoder::encode(
        &FrontendMessage::PasswordMessage {
            password: b"md5hash".to_vec(),
        },
        &mut buf,
    )
    .unwrap();
    assert_eq!(buf[0], b'p');
}

// ========================================================================
// Simple query flow tests
// ========================================================================

#[test]
fn test_simple_query_encoding() {
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
}

#[test]
fn test_query_response_decoding() {
    // RowDescription + DataRow + CommandComplete + ReadyForQuery
    let mut raw = Vec::new();

    // RowDescription for a single INT4 column
    raw.extend_from_slice(&[b'T', 0, 0, 0, 0]); // placeholder length
    let mut body = Vec::new();
    body.extend_from_slice(&(1i16).to_be_bytes()); // field count
    body.extend_from_slice(b"num\0"); // column name
    body.extend_from_slice(&0i32.to_be_bytes()); // table OID
    body.extend_from_slice(&0i16.to_be_bytes()); // column ID
    body.extend_from_slice(&23u32.to_be_bytes()); // INT4 OID
    body.extend_from_slice(&4i16.to_be_bytes()); // type size
    body.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
    body.extend_from_slice(&0i16.to_be_bytes()); // format code (text)
    let t_len = (body.len() + 4) as i32;
    raw[1..5].copy_from_slice(&t_len.to_be_bytes());
    raw.extend_from_slice(&body);

    // DataRow
    raw.extend_from_slice(&[b'D', 0, 0, 0, 0]); // placeholder length
    let mut dr_body = Vec::new();
    dr_body.extend_from_slice(&(1i16).to_be_bytes()); // column count
    dr_body.extend_from_slice(&(2i32).to_be_bytes()); // value length
    dr_body.extend_from_slice(b"42"); // value
    let dr_len = (dr_body.len() + 4) as i32;
    let dr_start = raw.len();
    raw.extend_from_slice(&dr_body);
    raw[dr_start - 4..dr_start].copy_from_slice(&dr_len.to_be_bytes());

    // CommandComplete
    let mut cc = vec![b'C', 0, 0, 0, 0];
    cc.extend_from_slice(b"SELECT 1\0");
    let cc_len = (cc.len() - 1) as i32;
    cc[1..5].copy_from_slice(&cc_len.to_be_bytes());
    raw.extend_from_slice(&cc);

    // ReadyForQuery
    raw.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);

    let buf = BytesMut::from(&raw[..]);
    let mut msg_buf = MessageBuffer::from_bytesmut(buf);

    // RowDescription
    let msg = msg_buf.next_message().unwrap().unwrap();
    assert!(matches!(msg, BackendMessage::RowDescription(_)));

    // DataRow
    let msg = msg_buf.next_message().unwrap().unwrap();
    assert!(matches!(msg, BackendMessage::DataRow(_)));

    // CommandComplete
    let msg = msg_buf.next_message().unwrap().unwrap();
    assert!(matches!(msg, BackendMessage::CommandComplete(_)));

    // ReadyForQuery
    let msg = msg_buf.next_message().unwrap().unwrap();
    assert!(matches!(msg, BackendMessage::ReadyForQuery(_)));

    assert!(msg_buf.is_empty());
}

#[test]
fn test_error_response_decoding() {
    let mut raw = vec![b'E', 0, 0, 0, 0];
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(b"42601\0");
    body.push(b'M');
    body.extend_from_slice(b"syntax error\0");
    body.push(0);
    let len = (body.len() + 4) as i32;
    raw[1..5].copy_from_slice(&len.to_be_bytes());
    raw.extend_from_slice(&body);

    let buf = BytesMut::from(&raw[..]);
    let mut msg_buf = MessageBuffer::from_bytesmut(buf);
    let msg = msg_buf.next_message().unwrap().unwrap();

    match msg {
        BackendMessage::ErrorResponse(body) => {
            let fields: Vec<_> = body.fields().collect().unwrap();
            assert!(fields.iter().any(|f| f.type_() == b'S' && f.value_bytes() == b"ERROR"));
            assert!(fields.iter().any(|f| f.type_() == b'C' && f.value_bytes() == b"42601"));
            assert!(fields.iter().any(|f| f.type_() == b'M' && f.value_bytes() == b"syntax error"));
        }
        _ => panic!("expected ErrorResponse"),
    }
}

#[test]
fn test_notification_response_decoding() {
    let mut raw = vec![b'A', 0, 0, 0, 0];
    let mut body = Vec::new();
    body.extend_from_slice(&99i32.to_be_bytes()); // process_id
    body.extend_from_slice(b"test_channel\0");
    body.extend_from_slice(b"hello\0");
    let len = (body.len() + 4) as i32;
    raw[1..5].copy_from_slice(&len.to_be_bytes());
    raw.extend_from_slice(&body);

    let buf = BytesMut::from(&raw[..]);
    let mut msg_buf = MessageBuffer::from_bytesmut(buf);
    let msg = msg_buf.next_message().unwrap().unwrap();

    match msg {
        BackendMessage::NotificationResponse(body) => {
            assert_eq!(body.process_id(), 99);
            assert_eq!(body.channel().unwrap(), "test_channel");
            assert_eq!(body.message().unwrap(), "hello");
        }
        _ => panic!("expected NotificationResponse"),
    }
}

// ========================================================================
// Extended query protocol tests
// ========================================================================

#[test]
fn test_extended_query_flow_encoding() {
    let mut buf = BytesMut::new();

    // Parse
    MessageEncoder::encode(
        &FrontendMessage::Parse {
            name: "stmt1".into(),
            sql: "SELECT $1".into(),
            param_types: vec![23], // INT4
        },
        &mut buf,
    )
    .unwrap();
    assert_eq!(buf[0], b'P');

    // Bind
    MessageEncoder::encode(
        &FrontendMessage::Bind {
            portal: "".into(),
            statement: "stmt1".into(),
            param_formats: vec![FormatCode::Binary],
            params: vec![Some(42i32.to_be_bytes().to_vec())],
            result_formats: vec![FormatCode::Binary],
        },
        &mut buf,
    )
    .unwrap();
    assert!(buf.len() > 1);

    // Execute
    MessageEncoder::encode(
        &FrontendMessage::Execute {
            portal: "".into(),
            max_rows: 0,
        },
        &mut buf,
    )
    .unwrap();

    // Sync
    MessageEncoder::encode(&FrontendMessage::Sync, &mut buf).unwrap();
}

#[test]
fn test_copy_protocol_encoding() {
    let mut buf = BytesMut::new();

    // CopyData
    MessageEncoder::encode(
        &FrontendMessage::CopyData {
            data: b"hello\tworld\n".to_vec(),
        },
        &mut buf,
    )
    .unwrap();
    assert_eq!(buf[0], b'd');

    // CopyDone
    MessageEncoder::encode(&FrontendMessage::CopyDone, &mut buf).unwrap();
    assert_eq!(buf[buf.len() - 5], b'c');

    // CopyFail
    MessageEncoder::encode(
        &FrontendMessage::CopyFail {
            message: "oops".into(),
        },
        &mut buf,
    )
    .unwrap();
}

// ========================================================================
// Buffer management tests
// ========================================================================

#[test]
fn test_buffer_incremental_feeding() {
    let mut buf = MessageBuffer::new();

    // Feed AuthOk one byte at a time
    let auth_ok: &[u8] = &[b'R', 0, 0, 0, 8, 0, 0, 0, 0];
    for i in 0..auth_ok.len() - 1 {
        buf.extend(&auth_ok[i..i + 1]);
        assert!(
            buf.next_message().unwrap().is_none(),
            "should not have a complete message after byte {}",
            i + 1
        );
    }
    buf.extend(&auth_ok[auth_ok.len() - 1..]);
    let msg = buf.next_message().unwrap().unwrap();
    assert!(matches!(msg, BackendMessage::AuthenticationOk));
    assert!(buf.is_empty());
}

#[test]
fn test_buffer_multiple_messages_incremental() {
    let mut buf = MessageBuffer::new();

    // Feed two messages at once
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
fn test_buffer_malformed_message_type() {
    let buf = BytesMut::from(&[b'!', 0, 0, 0, 4][..]);
    let mut msg_buf = MessageBuffer::from_bytesmut(buf);
    let result = msg_buf.next_message();
    assert!(result.is_err());
    // The error should be an Io error wrapped in ProtocolError
    match result {
        Err(ProtocolError::Io(_)) => {}
        Err(e) => panic!("expected Io error, got: {:?}", e),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

// ========================================================================
// Transaction status tests
// ========================================================================

#[test]
fn test_transaction_status_from_ready_for_query() {
    // Idle status
    let buf = BytesMut::from(&[b'Z', 0, 0, 0, 5, b'I'][..]);
    let mut msg_buf = MessageBuffer::from_bytesmut(buf);
    let msg = msg_buf.next_message().unwrap().unwrap();
    match msg {
        BackendMessage::ReadyForQuery(body) => {
            let status = TransactionStatus::from_u8(body.status()).unwrap();
            assert_eq!(status, TransactionStatus::Idle);
        }
        _ => panic!("expected ReadyForQuery"),
    }

    // In transaction
    let buf = BytesMut::from(&[b'Z', 0, 0, 0, 5, b'T'][..]);
    let mut msg_buf = MessageBuffer::from_bytesmut(buf);
    let msg = msg_buf.next_message().unwrap().unwrap();
    match msg {
        BackendMessage::ReadyForQuery(body) => {
            let status = TransactionStatus::from_u8(body.status()).unwrap();
            assert_eq!(status, TransactionStatus::InTransaction);
        }
        _ => panic!("expected ReadyForQuery"),
    }

    // Failed transaction
    let buf = BytesMut::from(&[b'Z', 0, 0, 0, 5, b'E'][..]);
    let mut msg_buf = MessageBuffer::from_bytesmut(buf);
    let msg = msg_buf.next_message().unwrap().unwrap();
    match msg {
        BackendMessage::ReadyForQuery(body) => {
            let status = TransactionStatus::from_u8(body.status()).unwrap();
            assert_eq!(status, TransactionStatus::Failed);
        }
        _ => panic!("expected ReadyForQuery"),
    }
}

// ========================================================================
// Format code tests
// ========================================================================

#[test]
fn test_format_code_conversion() {
    assert_eq!(FormatCode::from_u16(0), Some(FormatCode::Text));
    assert_eq!(FormatCode::from_u16(1), Some(FormatCode::Binary));
    assert_eq!(FormatCode::from_u16(2), None);

    assert_eq!(FormatCode::Text.to_u16(), 0);
    assert_eq!(FormatCode::Binary.to_u16(), 1);

    assert_eq!(FormatCode::Text.to_i16(), 0);
    assert_eq!(FormatCode::Binary.to_i16(), 1);
}
