//! Decoding of PostgreSQL wire protocol **backend** messages (server → client).
//!
//! This module re-exports the message types from `postgres_protocol::message::backend`
//! under the simpler name [`BackendMessage`] and adds a few convenience helpers.

pub use postgres_protocol::message::backend::Message as BackendMessage;

// Re-export body structs so callers do not need to depend on postgres-protocol directly.
pub use postgres_protocol::message::backend::{
    AuthenticationMd5PasswordBody,
    AuthenticationSaslBody,
    AuthenticationSaslContinueBody,
    AuthenticationSaslFinalBody,
    BackendKeyDataBody,
    ColumnFormats,
    CommandCompleteBody,
    CopyDataBody,
    CopyInResponseBody,
    CopyOutResponseBody,
    DataRowBody,
    DataRowRanges,
    ErrorField,
    ErrorFields,
    ErrorResponseBody,
    Field,
    Fields,
    NotificationResponseBody,
    NoticeResponseBody,
    ParameterDescriptionBody,
    ParameterStatusBody,
    Parameters,
    ReadyForQueryBody,
    RowDescriptionBody,
    SaslMechanisms,
};

/// Convenience re-exports for message type tags.
pub mod tags {
    //! Single-byte message type identifiers.

    /// `b'R'` — Authentication request.
    pub const AUTHENTICATION: u8 = postgres_protocol::message::backend::AUTHENTICATION_TAG;
    /// `b'K'` — Backend key data.
    pub const BACKEND_KEY_DATA: u8 = postgres_protocol::message::backend::BACKEND_KEY_DATA_TAG;
    /// `b'Z'` — Ready for query.
    pub const READY_FOR_QUERY: u8 = postgres_protocol::message::backend::READY_FOR_QUERY_TAG;
    /// `b'T'` — Row description.
    pub const ROW_DESCRIPTION: u8 = postgres_protocol::message::backend::ROW_DESCRIPTION_TAG;
    /// `b'D'` — Data row.
    pub const DATA_ROW: u8 = postgres_protocol::message::backend::DATA_ROW_TAG;
    /// `b'C'` — Command complete.
    pub const COMMAND_COMPLETE: u8 = postgres_protocol::message::backend::COMMAND_COMPLETE_TAG;
    /// `b'E'` — Error response.
    pub const ERROR_RESPONSE: u8 = postgres_protocol::message::backend::ERROR_RESPONSE_TAG;
    /// `b'N'` — Notice response.
    pub const NOTICE_RESPONSE: u8 = postgres_protocol::message::backend::NOTICE_RESPONSE_TAG;
    /// `b'S'` — Parameter status.
    pub const PARAMETER_STATUS: u8 = postgres_protocol::message::backend::PARAMETER_STATUS_TAG;
    /// `b'1'` — Parse complete.
    pub const PARSE_COMPLETE: u8 = postgres_protocol::message::backend::PARSE_COMPLETE_TAG;
    /// `b'2'` — Bind complete.
    pub const BIND_COMPLETE: u8 = postgres_protocol::message::backend::BIND_COMPLETE_TAG;
    /// `b'3'` — Close complete.
    pub const CLOSE_COMPLETE: u8 = postgres_protocol::message::backend::CLOSE_COMPLETE_TAG;
    /// `b'n'` — No data.
    pub const NO_DATA: u8 = postgres_protocol::message::backend::NO_DATA_TAG;
    /// `b's'` — Portal suspended.
    pub const PORTAL_SUSPENDED: u8 = postgres_protocol::message::backend::PORTAL_SUSPENDED_TAG;
    /// `b'I'` — Empty query response.
    pub const EMPTY_QUERY_RESPONSE: u8 = postgres_protocol::message::backend::EMPTY_QUERY_RESPONSE_TAG;
    /// `b'A'` — Notification response.
    pub const NOTIFICATION_RESPONSE: u8 = postgres_protocol::message::backend::NOTIFICATION_RESPONSE_TAG;
    /// `b't'` — Parameter description.
    pub const PARAMETER_DESCRIPTION: u8 = postgres_protocol::message::backend::PARAMETER_DESCRIPTION_TAG;
    /// `b'd'` — Copy data.
    pub const COPY_DATA: u8 = postgres_protocol::message::backend::COPY_DATA_TAG;
    /// `b'c'` — Copy done.
    pub const COPY_DONE: u8 = postgres_protocol::message::backend::COPY_DONE_TAG;
    /// `b'G'` — Copy in response.
    pub const COPY_IN_RESPONSE: u8 = postgres_protocol::message::backend::COPY_IN_RESPONSE_TAG;
    /// `b'H'` — Copy out response.
    pub const COPY_OUT_RESPONSE: u8 = postgres_protocol::message::backend::COPY_OUT_RESPONSE_TAG;
}
