//! PostgreSQL server error with full field mapping.
//!
//! This module defines [`PgServerError`] which captures every field from a
//! PostgreSQL `ErrorResponse` message, providing structured access to severity,
//! SQLSTATE code, position, constraint names, and more.

use crate::protocol::backend::{ErrorResponseBody, NoticeResponseBody};
use fallible_iterator::FallibleIterator;

// ---------------------------------------------------------------------------
// PgServerError
// ---------------------------------------------------------------------------

/// A structured error returned by the PostgreSQL server.
///
/// Every field corresponds to a field in the PostgreSQL `ErrorResponse` or
/// `NoticeResponse` message format.  Only `severity`, `code`, and `message`
/// are always present; all other fields are `Option`.
///
/// # SQLSTATE codes
///
/// The `code` field contains the 5-character SQLSTATE error code defined by
/// the SQL standard and extended by PostgreSQL.  See [`sqlstate`](super::sqlstate)
/// for constants and helper methods.
///
/// # Example
///
/// ```ignore
/// match err {
///     PgError::Server(e) => {
///         if e.is_unique_violation() {
///             println!("duplicate key: {}", e.constraint().unwrap_or_default());
///         }
///     }
///     _ => {}
/// }
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PgServerError {
    /// Severity: `ERROR`, `FATAL`, `PANIC`, `WARNING`, `NOTICE`, `DEBUG`, `INFO`, `LOG`.
    pub severity: String,
    /// Localized severity (PostgreSQL ≥ 9.6).
    pub severity_v: Option<String>,
    /// SQLSTATE error code (e.g., `"23505"` for unique violation).
    pub code: String,
    /// Primary error message.
    pub message: String,
    /// Optional detail providing additional context.
    pub detail: Option<String>,
    /// Optional suggestion for resolving the error.
    pub hint: Option<String>,
    /// Error position in the query string (1-based character offset).
    pub position: Option<u32>,
    /// Internal position (position within internally-generated query).
    pub internal_position: Option<u32>,
    /// Internal query (the internally-generated command that led to the error).
    pub internal_query: Option<String>,
    /// Call-stack context (e.g., in PL/pgSQL functions).
    pub where_: Option<String>,
    /// Schema name (if applicable).
    pub schema: Option<String>,
    /// Table name (if applicable).
    pub table: Option<String>,
    /// Column name (if applicable).
    pub column: Option<String>,
    /// Data type name (if applicable).
    pub data_type: Option<String>,
    /// Constraint name (if applicable).
    pub constraint: Option<String>,
    /// Source file in the PostgreSQL server code.
    pub file: Option<String>,
    /// Source line number in the PostgreSQL server code.
    pub line: Option<u32>,
    /// Source routine in the PostgreSQL server code.
    pub routine: Option<String>,
}

impl PgServerError {
    /// Parse a [`PgServerError`] from the raw `(type_byte, value)` field pairs
    /// of a PostgreSQL `ErrorResponse` or `NoticeResponse` message.
    pub fn from_fields(fields: Vec<(u8, String)>) -> Self {
        let mut err = PgServerError::default();
        for (code, value) in fields {
            match code {
                b'S' => err.severity = value,
                b'V' => err.severity_v = Some(value),
                b'C' => err.code = value,
                b'M' => err.message = value,
                b'D' => err.detail = Some(value),
                b'H' => err.hint = Some(value),
                b'P' => err.position = value.parse().ok(),
                b'p' => err.internal_position = value.parse().ok(),
                b'q' => err.internal_query = Some(value),
                b'W' => err.where_ = Some(value),
                b's' => err.schema = Some(value),
                b't' => err.table = Some(value),
                b'c' => err.column = Some(value),
                b'd' => err.data_type = Some(value),
                b'n' => err.constraint = Some(value),
                b'F' => err.file = Some(value),
                b'L' => err.line = value.parse().ok(),
                b'R' => err.routine = Some(value),
                _ => {} // ignore unknown fields (forward-compatible)
            }
        }
        err
    }

    /// Parse a [`PgServerError`] from an [`ErrorResponseBody`] (wire message).
    pub fn from_error_body(body: &ErrorResponseBody) -> Result<Self, std::io::Error> {
        let mut fields = Vec::new();
        let mut iter = body.fields();
        while let Some(field) = iter.next()? {
            let value = std::str::from_utf8(field.value_bytes())
                .unwrap_or("")
                .to_string();
            fields.push((field.type_(), value));
        }
        Ok(Self::from_fields(fields))
    }

    /// Parse a [`PgServerError`] from a [`NoticeResponseBody`] (wire message).
    pub fn from_notice_body(body: &NoticeResponseBody) -> Result<Self, std::io::Error> {
        let mut fields = Vec::new();
        let mut iter = body.fields();
        while let Some(field) = iter.next()? {
            let value = std::str::from_utf8(field.value_bytes())
                .unwrap_or("")
                .to_string();
            fields.push((field.type_(), value));
        }
        Ok(Self::from_fields(fields))
    }

    /// Returns the SQLSTATE error code (e.g., `"23505"` for unique violation).
    ///
    /// This is a convenience accessor equivalent to accessing the `code` field.
    pub fn code(&self) -> &str {
        &self.code
    }

    // =======================================================================
    // SQLSTATE classification helpers
    // =======================================================================

    /// Check if the SQLSTATE code belongs to the given 2-character class.
    ///
    /// SQLSTATE codes are 5 characters.  The first two characters identify
    /// the error class.  For example, class `"23"` is integrity constraint
    /// violations, class `"42"` is syntax error or access violation.
    pub fn is_class(&self, class: &str) -> bool {
        self.code.starts_with(class)
    }

    /// Integrity constraint violation (SQLSTATE class `23`).
    pub fn is_integrity_constraint_violation(&self) -> bool {
        self.is_class("23")
    }

    /// Unique constraint violation (`23505`).
    pub fn is_unique_violation(&self) -> bool {
        self.code == "23505"
    }

    /// Foreign key constraint violation (`23503`).
    pub fn is_foreign_key_violation(&self) -> bool {
        self.code == "23503"
    }

    /// NOT NULL constraint violation (`23502`).
    pub fn is_not_null_violation(&self) -> bool {
        self.code == "23502"
    }

    /// Check constraint violation (`23514`).
    pub fn is_check_violation(&self) -> bool {
        self.code == "23514"
    }

    /// Exclusion constraint violation (`23P01`).
    pub fn is_exclusion_violation(&self) -> bool {
        self.code == "23P01"
    }

    /// Syntax error or access violation (SQLSTATE class `42`).
    pub fn is_syntax_error(&self) -> bool {
        self.is_class("42")
    }

    /// Insufficient privilege (`42501`).
    pub fn is_insufficient_privilege(&self) -> bool {
        self.code == "42501"
    }

    /// Undefined table (`42P01`).
    pub fn is_undefined_table(&self) -> bool {
        self.code == "42P01"
    }

    /// Undefined column (`42703`).
    pub fn is_undefined_column(&self) -> bool {
        self.code == "42703"
    }

    /// Serialization failure (`40001`).
    pub fn is_serialization_failure(&self) -> bool {
        self.code == "40001"
    }

    /// Deadlock detected (`40P01`).
    pub fn is_deadlock_detected(&self) -> bool {
        self.code == "40P01"
    }

    /// Connection exception (SQLSTATE class `08`).
    pub fn is_connection_exception(&self) -> bool {
        self.is_class("08")
    }

    /// Connection does not exist (`08003`).
    pub fn is_connection_does_not_exist(&self) -> bool {
        self.code == "08003"
    }

    /// Connection failure (`08006`).
    pub fn is_connection_failure(&self) -> bool {
        self.code == "08006"
    }

    /// SQL client unable to establish SQL connection (`08001`).
    pub fn is_sqlclient_unable_to_establish_sqlconnection(&self) -> bool {
        self.code == "08001"
    }

    /// Query canceled (`57014`).
    pub fn is_query_canceled(&self) -> bool {
        self.code == "57014"
    }

    /// Admin shutdown (`57P01`).
    pub fn is_admin_shutdown(&self) -> bool {
        self.code == "57P01"
    }

    /// Crash shutdown (`57P02`).
    pub fn is_crash_shutdown(&self) -> bool {
        self.code == "57P02"
    }

    /// Cannot connect now (`57P03`).
    pub fn is_cannot_connect_now(&self) -> bool {
        self.code == "57P03"
    }

    /// Database dropped (`57P04`).
    pub fn is_database_dropped(&self) -> bool {
        self.code == "57P04"
    }

    /// Idle session timeout (`57P05`).
    pub fn is_idle_session_timeout(&self) -> bool {
        self.code == "57P05"
    }

    /// Returns `true` if the error severity is `FATAL` or `PANIC`.
    pub fn is_fatal(&self) -> bool {
        self.severity == "FATAL" || self.severity == "PANIC"
    }

    /// Returns `true` if the error severity is `WARNING` or lower
    /// (`NOTICE`, `DEBUG`, `INFO`, `LOG`).
    pub fn is_warning_or_less(&self) -> bool {
        matches!(
            self.severity.as_str(),
            "WARNING" | "NOTICE" | "DEBUG" | "INFO" | "LOG"
        )
    }

    // =======================================================================
    // Convenience accessors
    // =======================================================================

    /// Returns the schema name, if available.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Returns the table name, if available.
    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }

    /// Returns the column name, if available.
    pub fn column(&self) -> Option<&str> {
        self.column.as_deref()
    }

    /// Returns the constraint name, if available.
    pub fn constraint(&self) -> Option<&str> {
        self.constraint.as_deref()
    }

    /// Returns the detail, if available.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    /// Returns the hint, if available.
    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    /// Returns the error position in the query, if available.
    pub fn position(&self) -> Option<u32> {
        self.position
    }
}

impl std::fmt::Display for PgServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (SQLSTATE {})",
            self.severity, self.message, self.code
        )?;
        if let Some(detail) = &self.detail {
            write!(f, "\nDETAIL: {}", detail)?;
        }
        if let Some(hint) = &self.hint {
            write!(f, "\nHINT: {}", hint)?;
        }
        if let Some(position) = self.position {
            write!(f, "\nPOSITION: {}", position)?;
        }
        Ok(())
    }
}

impl std::error::Error for PgServerError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::field_reassign_with_default)]

    use super::*;

    #[test]
    fn test_from_fields_all_fields() {
        let fields = vec![
            (b'S', "ERROR".to_string()),
            (b'V', "ERROR".to_string()),
            (b'C', "23505".to_string()),
            (
                b'M',
                "duplicate key value violates unique constraint".to_string(),
            ),
            (b'D', "Key (id)=(1) already exists.".to_string()),
            (b'H', "Try a different value.".to_string()),
            (b'P', "42".to_string()),
            (b'p', "10".to_string()),
            (b'q', "SELECT ...".to_string()),
            (b'W', "PL/pgSQL function ...".to_string()),
            (b's', "public".to_string()),
            (b't', "users".to_string()),
            (b'c', "id".to_string()),
            (b'd', "integer".to_string()),
            (b'n', "users_pkey".to_string()),
            (b'F', "nbtinsert.c".to_string()),
            (b'L', "532".to_string()),
            (b'R', "_bt_check_unique".to_string()),
        ];

        let err = PgServerError::from_fields(fields);
        assert_eq!(err.severity, "ERROR");
        assert_eq!(err.severity_v.as_deref(), Some("ERROR"));
        assert_eq!(err.code, "23505");
        assert_eq!(
            err.message,
            "duplicate key value violates unique constraint"
        );
        assert_eq!(err.detail.as_deref(), Some("Key (id)=(1) already exists."));
        assert_eq!(err.hint.as_deref(), Some("Try a different value."));
        assert_eq!(err.position, Some(42));
        assert_eq!(err.internal_position, Some(10));
        assert_eq!(err.internal_query.as_deref(), Some("SELECT ..."));
        assert_eq!(err.where_.as_deref(), Some("PL/pgSQL function ..."));
        assert_eq!(err.schema.as_deref(), Some("public"));
        assert_eq!(err.table.as_deref(), Some("users"));
        assert_eq!(err.column.as_deref(), Some("id"));
        assert_eq!(err.data_type.as_deref(), Some("integer"));
        assert_eq!(err.constraint.as_deref(), Some("users_pkey"));
        assert_eq!(err.file.as_deref(), Some("nbtinsert.c"));
        assert_eq!(err.line, Some(532));
        assert_eq!(err.routine.as_deref(), Some("_bt_check_unique"));
    }

    #[test]
    fn test_from_fields_minimal() {
        let fields = vec![
            (b'S', "ERROR".to_string()),
            (b'C', "42601".to_string()),
            (b'M', "syntax error".to_string()),
        ];

        let err = PgServerError::from_fields(fields);
        assert_eq!(err.severity, "ERROR");
        assert_eq!(err.code, "42601");
        assert_eq!(err.message, "syntax error");
        assert!(err.detail.is_none());
        assert!(err.hint.is_none());
        assert!(err.position.is_none());
    }

    #[test]
    fn test_from_fields_unknown_field_ignored() {
        let fields = vec![
            (b'S', "ERROR".to_string()),
            (b'C', "42601".to_string()),
            (b'M', "syntax error".to_string()),
            (b'X', "unknown field".to_string()), // should be ignored
        ];

        let err = PgServerError::from_fields(fields);
        assert_eq!(err.message, "syntax error");
    }

    #[test]
    fn test_sqlstate_classification() {
        let mut err = PgServerError::default();

        // Unique violation
        err.code = "23505".to_string();
        assert!(err.is_unique_violation());
        assert!(err.is_integrity_constraint_violation());
        assert!(!err.is_syntax_error());

        // Syntax error
        err.code = "42601".to_string();
        assert!(err.is_syntax_error());
        assert!(!err.is_integrity_constraint_violation());

        // Foreign key
        err.code = "23503".to_string();
        assert!(err.is_foreign_key_violation());

        // NOT NULL
        err.code = "23502".to_string();
        assert!(err.is_not_null_violation());

        // Check
        err.code = "23514".to_string();
        assert!(err.is_check_violation());

        // Exclusion
        err.code = "23P01".to_string();
        assert!(err.is_exclusion_violation());

        // Insufficient privilege
        err.code = "42501".to_string();
        assert!(err.is_insufficient_privilege());

        // Undefined table
        err.code = "42P01".to_string();
        assert!(err.is_undefined_table());

        // Undefined column
        err.code = "42703".to_string();
        assert!(err.is_undefined_column());

        // Serialization failure
        err.code = "40001".to_string();
        assert!(err.is_serialization_failure());

        // Deadlock
        err.code = "40P01".to_string();
        assert!(err.is_deadlock_detected());

        // Connection exception
        err.code = "08006".to_string();
        assert!(err.is_connection_exception());
        assert!(err.is_connection_failure());

        // Query canceled
        err.code = "57014".to_string();
        assert!(err.is_query_canceled());
    }

    #[test]
    fn test_severity_checks() {
        let mut err = PgServerError::default();

        err.severity = "FATAL".to_string();
        assert!(err.is_fatal());
        assert!(!err.is_warning_or_less());

        err.severity = "PANIC".to_string();
        assert!(err.is_fatal());

        err.severity = "ERROR".to_string();
        assert!(!err.is_fatal());
        assert!(!err.is_warning_or_less());

        err.severity = "WARNING".to_string();
        assert!(err.is_warning_or_less());

        err.severity = "DEBUG".to_string();
        assert!(err.is_warning_or_less());

        err.severity = "INFO".to_string();
        assert!(err.is_warning_or_less());

        err.severity = "LOG".to_string();
        assert!(err.is_warning_or_less());
    }

    #[test]
    fn test_display_format() {
        let err = PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "23505".to_string()),
            (b'M', "duplicate key".to_string()),
            (b'D', "Key (id)=(1) already exists.".to_string()),
            (b'H', "Try a different value.".to_string()),
            (b'P', "42".to_string()),
        ]);

        let display = err.to_string();
        assert!(display.contains("ERROR: duplicate key (SQLSTATE 23505)"));
        assert!(display.contains("DETAIL: Key (id)=(1) already exists."));
        assert!(display.contains("HINT: Try a different value."));
        assert!(display.contains("POSITION: 42"));
    }

    #[test]
    fn test_display_format_minimal() {
        let err = PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "42601".to_string()),
            (b'M', "syntax error".to_string()),
        ]);

        let display = err.to_string();
        assert_eq!(display, "ERROR: syntax error (SQLSTATE 42601)");
    }

    #[test]
    fn test_convenience_accessors() {
        let err = PgServerError::from_fields(vec![
            (b'S', "ERROR".to_string()),
            (b'C', "23505".to_string()),
            (b'M', "duplicate key".to_string()),
            (b'D', "some detail".to_string()),
            (b'H', "some hint".to_string()),
            (b's', "public".to_string()),
            (b't', "users".to_string()),
            (b'c', "id".to_string()),
            (b'n', "users_pkey".to_string()),
            (b'P', "42".to_string()),
        ]);

        assert_eq!(err.schema(), Some("public"));
        assert_eq!(err.table(), Some("users"));
        assert_eq!(err.column(), Some("id"));
        assert_eq!(err.constraint(), Some("users_pkey"));
        assert_eq!(err.detail(), Some("some detail"));
        assert_eq!(err.hint(), Some("some hint"));
        assert_eq!(err.position(), Some(42));
    }

    #[test]
    fn test_connection_related_codes() {
        let mut err = PgServerError::default();

        err.code = "08003".to_string();
        assert!(err.is_connection_does_not_exist());

        err.code = "08001".to_string();
        assert!(err.is_sqlclient_unable_to_establish_sqlconnection());

        err.code = "57P01".to_string();
        assert!(err.is_admin_shutdown());

        err.code = "57P02".to_string();
        assert!(err.is_crash_shutdown());

        err.code = "57P03".to_string();
        assert!(err.is_cannot_connect_now());

        err.code = "57P04".to_string();
        assert!(err.is_database_dropped());

        err.code = "57P05".to_string();
        assert!(err.is_idle_session_timeout());
    }
}
