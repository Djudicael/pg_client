pub use postgres_protocol::Oid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatCode {
    Text = 0,
    Binary = 1,
}

impl FormatCode {
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            0 => Some(FormatCode::Text),
            1 => Some(FormatCode::Binary),
            _ => None,
        }
    }

    pub fn to_u16(self) -> u16 {
        self as u16
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Idle,
    InTransaction,
    Failed,
}

impl TransactionStatus {
    pub fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            b'I' => Some(TransactionStatus::Idle),
            b'T' => Some(TransactionStatus::InTransaction),
            b'E' => Some(TransactionStatus::Failed),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::Failed => b'E',
        }
    }
}

pub mod message_type {
    pub const STARTUP: u8 = 0;
    pub const QUERY: u8 = b'Q';
    pub const PARSE: u8 = b'P';
    pub const BIND: u8 = b'B';
    pub const EXECUTE: u8 = b'E';
    pub const DESCRIBE: u8 = b'D';
    pub const SYNC: u8 = b'S';
    pub const CLOSE: u8 = b'C';
    pub const FLUSH: u8 = b'H';
    pub const TERMINATE: u8 = b'X';
    pub const DATA_ROW: u8 = b'D';
    pub const COMMAND_COMPLETE: u8 = b'C';
    pub const ERROR_RESPONSE: u8 = b'E';
    pub const READY_FOR_QUERY: u8 = b'Z';
    pub const AUTHENTICATION_REQUEST: u8 = b'R';
    pub const PARAMETER_STATUS: u8 = b'S';
    pub const BACKEND_KEY_DATA: u8 = b'K';
    pub const NOTICE_RESPONSE: u8 = b'N';
    pub const NOTIFICATION_RESPONSE: u8 = b'A';
    pub const ROW_DESCRIPTION: u8 = b'T';
    pub const PARAMETER_DESCRIPTION: u8 = b't';
    pub const PARSE_COMPLETE: u8 = b'1';
    pub const BIND_COMPLETE: u8 = b'2';
    pub const CLOSE_COMPLETE: u8 = b'3';
    pub const NO_DATA: u8 = b'n';
    pub const PORTAL_SUSPENDED: u8 = b's';
    pub const EMPTY_QUERY_RESPONSE: u8 = b'I';
    pub const COPY_DATA: u8 = b'd';
    pub const COPY_DONE: u8 = b'c';
    pub const COPY_FAIL: u8 = b'f';
    pub const COPY_IN_RESPONSE: u8 = b'G';
    pub const COPY_OUT_RESPONSE: u8 = b'H';
}

pub mod auth {
    pub const OK: i32 = 0;
    pub const KERBEROS_V5: i32 = 2;
    pub const CLEARTEXT_PASSWORD: i32 = 3;
    pub const MD5_PASSWORD: i32 = 5;
    pub const SCM_CREDENTIAL: i32 = 6;
    pub const GSS: i32 = 7;
    pub const GSS_CONTINUE: i32 = 8;
    pub const SSPI: i32 = 9;
    pub const SASL: i32 = 10;
    pub const SASL_CONTINUE: i32 = 11;
    pub const SASL_FINAL: i32 = 12;
}

pub mod error_field {
    pub const SEVERITY: u8 = b'S';
    pub const SQLSTATE: u8 = b'C';
    pub const MESSAGE: u8 = b'M';
    pub const DETAIL: u8 = b'D';
    pub const HINT: u8 = b'H';
    pub const POSITION: u8 = b'P';
    pub const INTERNAL_POSITION: u8 = b'p';
    pub const INTERNAL_QUERY: u8 = b'q';
    pub const WHERE: u8 = b'W';
    pub const SCHEMA_NAME: u8 = b's';
    pub const TABLE_NAME: u8 = b't';
    pub const COLUMN_NAME: u8 = b'c';
    pub const DATA_TYPE_NAME: u8 = b'd';
    pub const CONSTRAINT_NAME: u8 = b'n';
    pub const FILE: u8 = b'F';
    pub const LINE: u8 = b'L';
    pub const ROUTINE: u8 = b'R';
}
