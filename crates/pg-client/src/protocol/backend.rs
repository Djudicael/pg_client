pub use postgres_protocol::message::backend::Message as BackendMessage;

pub use postgres_protocol::message::backend::{
    AuthenticationMd5PasswordBody, AuthenticationSaslBody, AuthenticationSaslContinueBody,
    AuthenticationSaslFinalBody, BackendKeyDataBody, ColumnFormats, CommandCompleteBody,
    CopyDataBody, CopyInResponseBody, CopyOutResponseBody, DataRowBody, DataRowRanges, ErrorField,
    ErrorFields, ErrorResponseBody, Field, Fields, NoticeResponseBody, NotificationResponseBody,
    ParameterDescriptionBody, ParameterStatusBody, Parameters, ReadyForQueryBody,
    RowDescriptionBody, SaslMechanisms,
};

pub mod tags {
    pub const AUTHENTICATION: u8 = postgres_protocol::message::backend::AUTHENTICATION_TAG;
    pub const BACKEND_KEY_DATA: u8 = postgres_protocol::message::backend::BACKEND_KEY_DATA_TAG;
    pub const READY_FOR_QUERY: u8 = postgres_protocol::message::backend::READY_FOR_QUERY_TAG;
    pub const ROW_DESCRIPTION: u8 = postgres_protocol::message::backend::ROW_DESCRIPTION_TAG;
    pub const DATA_ROW: u8 = postgres_protocol::message::backend::DATA_ROW_TAG;
    pub const COMMAND_COMPLETE: u8 = postgres_protocol::message::backend::COMMAND_COMPLETE_TAG;
    pub const ERROR_RESPONSE: u8 = postgres_protocol::message::backend::ERROR_RESPONSE_TAG;
    pub const NOTICE_RESPONSE: u8 = postgres_protocol::message::backend::NOTICE_RESPONSE_TAG;
    pub const PARAMETER_STATUS: u8 = postgres_protocol::message::backend::PARAMETER_STATUS_TAG;
    pub const PARSE_COMPLETE: u8 = postgres_protocol::message::backend::PARSE_COMPLETE_TAG;
    pub const BIND_COMPLETE: u8 = postgres_protocol::message::backend::BIND_COMPLETE_TAG;
    pub const CLOSE_COMPLETE: u8 = postgres_protocol::message::backend::CLOSE_COMPLETE_TAG;
    pub const NO_DATA: u8 = postgres_protocol::message::backend::NO_DATA_TAG;
    pub const PORTAL_SUSPENDED: u8 = postgres_protocol::message::backend::PORTAL_SUSPENDED_TAG;
    pub const EMPTY_QUERY_RESPONSE: u8 =
        postgres_protocol::message::backend::EMPTY_QUERY_RESPONSE_TAG;
    pub const NOTIFICATION_RESPONSE: u8 =
        postgres_protocol::message::backend::NOTIFICATION_RESPONSE_TAG;
    pub const PARAMETER_DESCRIPTION: u8 =
        postgres_protocol::message::backend::PARAMETER_DESCRIPTION_TAG;
    pub const COPY_DATA: u8 = postgres_protocol::message::backend::COPY_DATA_TAG;
    pub const COPY_DONE: u8 = postgres_protocol::message::backend::COPY_DONE_TAG;
    pub const COPY_IN_RESPONSE: u8 = postgres_protocol::message::backend::COPY_IN_RESPONSE_TAG;
    pub const COPY_OUT_RESPONSE: u8 = postgres_protocol::message::backend::COPY_OUT_RESPONSE_TAG;
}
