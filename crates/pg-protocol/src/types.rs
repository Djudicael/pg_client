//! Supporting types and constants for the PostgreSQL wire protocol.

/// Re-export OID type.
pub use postgres_protocol::Oid;

/// Format code for data representation.
///
/// PostgreSQL supports text and binary formats for data transmission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatCode {
    /// Text format (UTF-8 strings).
    Text = 0,
    /// Binary format (type-specific binary representation).
    Binary = 1,
}

impl FormatCode {
    /// Converts a `u16` to a `FormatCode`.
    ///
    /// Returns `None` if the value is not `0` or `1`.
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            0 => Some(FormatCode::Text),
            1 => Some(FormatCode::Binary),
            _ => None,
        }
    }

    /// Converts the `FormatCode` to a `u16`.
    pub fn to_u16(self) -> u16 {
        self as u16
    }

    /// Converts the `FormatCode` to an `i16` (used in the wire protocol).
    pub fn to_i16(self) -> i16 {
        self as i16
    }
}

impl TryFrom<i16> for FormatCode {
    type Error = &'static str;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FormatCode::Text),
            1 => Ok(FormatCode::Binary),
            _ => Err("invalid format code"),
        }
    }
}

/// Transaction status indicators returned in the `ReadyForQuery` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Idle (not in a transaction block).
    Idle,
    /// In a transaction block.
    InTransaction,
    /// In a failed transaction block (queries will be rejected until rollback).
    Failed,
}

impl TransactionStatus {
    /// Parse from the single-byte wire representation.
    pub fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            b'I' => Some(TransactionStatus::Idle),
            b'T' => Some(TransactionStatus::InTransaction),
            b'E' => Some(TransactionStatus::Failed),
            _ => None,
        }
    }

    /// Convert to the single-byte wire representation.
    pub fn to_u8(self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::Failed => b'E',
        }
    }
}

/// Constants for message type bytes.
pub mod message_type {

    /// Startup message (no type byte on the wire, but `0` is used internally).
    pub const STARTUP: u8 = 0;
    /// Query (client → server).
    pub const QUERY: u8 = b'Q';
    /// Parse (client → server).
    pub const PARSE: u8 = b'P';
    /// Bind (client → server).
    pub const BIND: u8 = b'B';
    /// Execute (client → server).
    pub const EXECUTE: u8 = b'E';
    /// Describe (client → server).
    pub const DESCRIBE: u8 = b'D';
    /// Sync (client → server).
    pub const SYNC: u8 = b'S';
    /// Close (client → server).
    pub const CLOSE: u8 = b'C';
    /// Flush (client → server).
    pub const FLUSH: u8 = b'H';
    /// Terminate (client → server).
    pub const TERMINATE: u8 = b'X';
    /// Data row (server → client).
    pub const DATA_ROW: u8 = b'D';
    /// Command complete (server → client).
    pub const COMMAND_COMPLETE: u8 = b'C';
    /// Error response (server → client).
    pub const ERROR_RESPONSE: u8 = b'E';
    /// Ready for query (server → client).
    pub const READY_FOR_QUERY: u8 = b'Z';
    /// Authentication request (server → client).
    pub const AUTHENTICATION_REQUEST: u8 = b'R';
    /// Parameter status (server → client).
    pub const PARAMETER_STATUS: u8 = b'S';
    /// Backend key data (server → client).
    pub const BACKEND_KEY_DATA: u8 = b'K';
    /// Notice response (server → client).
    pub const NOTICE_RESPONSE: u8 = b'N';
    /// Notification response (server → client).
    pub const NOTIFICATION_RESPONSE: u8 = b'A';
    /// Row description (server → client).
    pub const ROW_DESCRIPTION: u8 = b'T';
    /// Parameter description (server → client).
    pub const PARAMETER_DESCRIPTION: u8 = b't';
    /// Parse complete (server → client).
    pub const PARSE_COMPLETE: u8 = b'1';
    /// Bind complete (server → client).
    pub const BIND_COMPLETE: u8 = b'2';
    /// Close complete (server → client).
    pub const CLOSE_COMPLETE: u8 = b'3';
    /// No data (server → client).
    pub const NO_DATA: u8 = b'n';
    /// Portal suspended (server → client).
    pub const PORTAL_SUSPENDED: u8 = b's';
    /// Empty query response (server → client).
    pub const EMPTY_QUERY_RESPONSE: u8 = b'I';
    /// Copy data (bidirectional).
    pub const COPY_DATA: u8 = b'd';
    /// Copy done (bidirectional).
    pub const COPY_DONE: u8 = b'c';
    /// Copy fail (client → server).
    pub const COPY_FAIL: u8 = b'f';
    /// Copy in response (server → client).
    pub const COPY_IN_RESPONSE: u8 = b'G';
    /// Copy out response (server → client).
    pub const COPY_OUT_RESPONSE: u8 = b'H';
}

/// Authentication method identifiers.
pub mod auth {
    /// Authentication successful.
    pub const OK: i32 = 0;
    /// Kerberos V5 authentication (not supported in WASI).
    pub const KERBEROS_V5: i32 = 2;
    /// Cleartext password authentication.
    pub const CLEARTEXT_PASSWORD: i32 = 3;
    /// MD5 password authentication.
    pub const MD5_PASSWORD: i32 = 5;
    /// SCM credential authentication (not supported in WASI).
    pub const SCM_CREDENTIAL: i32 = 6;
    /// GSS authentication (not supported in WASI).
    pub const GSS: i32 = 7;
    /// GSS continue (not supported in WASI).
    pub const GSS_CONTINUE: i32 = 8;
    /// SSPI authentication (not supported in WASI).
    pub const SSPI: i32 = 9;
    /// SASL authentication.
    pub const SASL: i32 = 10;
    /// SASL continue.
    pub const SASL_CONTINUE: i32 = 11;
    /// SASL final.
    pub const SASL_FINAL: i32 = 12;
}

/// Constants for error / notice response field codes.
pub mod error_field {
    /// Severity: `ERROR`, `FATAL`, `PANIC`, `WARNING`, `NOTICE`, `DEBUG`, `INFO`, `LOG`.
    pub const SEVERITY: u8 = b'S';
    /// SQLSTATE code.
    pub const SQLSTATE: u8 = b'C';
    /// Primary human-readable message.
    pub const MESSAGE: u8 = b'M';
    /// Detailed secondary message.
    pub const DETAIL: u8 = b'D';
    /// Suggestion for resolution.
    pub const HINT: u8 = b'H';
    /// Cursor position in original query.
    pub const POSITION: u8 = b'P';
    /// Internal cursor position.
    pub const INTERNAL_POSITION: u8 = b'p';
    /// Failed internally-generated command text.
    pub const INTERNAL_QUERY: u8 = b'q';
    /// Context where error occurred.
    pub const WHERE: u8 = b'W';
    /// Schema name.
    pub const SCHEMA_NAME: u8 = b's';
    /// Table name.
    pub const TABLE_NAME: u8 = b't';
    /// Column name.
    pub const COLUMN_NAME: u8 = b'c';
    /// Data type name.
    pub const DATA_TYPE_NAME: u8 = b'd';
    /// Constraint name.
    pub const CONSTRAINT_NAME: u8 = b'n';
    /// Source file.
    pub const FILE: u8 = b'F';
    /// Source line.
    pub const LINE: u8 = b'L';
    /// Source routine.
    pub const ROUTINE: u8 = b'R';
}
