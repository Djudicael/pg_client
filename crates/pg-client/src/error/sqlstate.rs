//! SQLSTATE error code constants and helpers.
//!
//! PostgreSQL uses five-character SQLSTATE codes defined by the SQL standard
//! and extended by PostgreSQL.  The first two characters identify the error
//! class; the last three identify the specific condition within that class.
//!
//! Reference: <https://www.postgresql.org/docs/current/errcodes-appendix.html>
//!
//! # Example
//!
//! ```ignore
//! use wasi_pg_client::error::sqlstate;
//!
//! if e.code() == sqlstate::UNIQUE_VIOLATION {
//!     println!("duplicate key!");
//! }
//! ```

// ===========================================================================
// Class prefixes (first 2 characters)
// ===========================================================================

/// Successful completion (class `00`).
pub const CLASS_SUCCESSFUL_COMPLETION: &str = "00";
/// Warning (class `01`).
pub const CLASS_WARNING: &str = "01";
/// No data (class `02`).
pub const CLASS_NO_DATA: &str = "02";
/// SQL statement not yet complete (class `03`).
pub const CLASS_SQL_STATEMENT_NOT_YET_COMPLETE: &str = "03";
/// Connection exception (class `08`).
pub const CLASS_CONNECTION_EXCEPTION: &str = "08";
/// Triggered action exception (class `09`).
pub const CLASS_TRIGGERED_ACTION_EXCEPTION: &str = "09";
/// Feature not supported (class `0A`).
pub const CLASS_FEATURE_NOT_SUPPORTED: &str = "0A";
/// Invalid transaction initiation (class `0B`).
pub const CLASS_INVALID_TRANSACTION_INITIATION: &str = "0B";
/// Locator exception (class `0F`).
pub const CLASS_LOCATOR_EXCEPTION: &str = "0F";
/// Invalid grantor (class `0L`).
pub const CLASS_INVALID_GRANTOR: &str = "0L";
/// Invalid role specification (class `0P`).
pub const CLASS_INVALID_ROLE_SPECIFICATION: &str = "0P";
/// Diagnostics exception (class `0Z`).
pub const CLASS_DIAGNOSTICS_EXCEPTION: &str = "0Z";
/// Case not found (class `20`).
pub const CLASS_CASE_NOT_FOUND: &str = "20";
/// Cardinality violation (class `21`).
pub const CLASS_CARDINALITY_VIOLATION: &str = "21";
/// Data exception (class `22`).
pub const CLASS_DATA_EXCEPTION: &str = "22";
/// Integrity constraint violation (class `23`).
pub const CLASS_INTEGRITY_CONSTRAINT_VIOLATION: &str = "23";
/// Invalid cursor state (class `24`).
pub const CLASS_INVALID_CURSOR_STATE: &str = "24";
/// Invalid transaction state (class `25`).
pub const CLASS_INVALID_TRANSACTION_STATE: &str = "25";
/// Invalid SQL statement name (class `26`).
pub const CLASS_INVALID_SQL_STATEMENT_NAME: &str = "26";
/// Triggered data change violation (class `27`).
pub const CLASS_TRIGGERED_DATA_CHANGE_VIOLATION: &str = "27";
/// Invalid authorization specification (class `28`).
pub const CLASS_INVALID_AUTHORIZATION_SPECIFICATION: &str = "28";
/// Dependent privilege descriptors still exist (class `2B`).
pub const CLASS_DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST: &str = "2B";
/// Invalid transaction termination (class `2D`).
pub const CLASS_INVALID_TRANSACTION_TERMINATION: &str = "2D";
/// SQL routine exception (class `2F`).
pub const CLASS_SQL_ROUTINE_EXCEPTION: &str = "2F";
/// Invalid cursor name (class `34`).
pub const CLASS_INVALID_CURSOR_NAME: &str = "34";
/// External routine exception (class `38`).
pub const CLASS_EXTERNAL_ROUTINE_EXCEPTION: &str = "38";
/// External routine invocation exception (class `39`).
pub const CLASS_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION: &str = "39";
/// Savepoint exception (class `3B`).
pub const CLASS_SAVEPOINT_EXCEPTION: &str = "3B";
/// Invalid catalog name (class `3D`).
pub const CLASS_INVALID_CATALOG_NAME: &str = "3D";
/// Invalid schema name (class `3F`).
pub const CLASS_INVALID_SCHEMA_NAME: &str = "3F";
/// Transaction rollback (class `40`).
pub const CLASS_TRANSACTION_ROLLBACK: &str = "40";
/// Syntax error or access rule violation (class `42`).
pub const CLASS_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION: &str = "42";
/// With check option violation (class `44`).
pub const CLASS_WITH_CHECK_OPTION_VIOLATION: &str = "44";
/// Insufficient resources (class `53`).
pub const CLASS_INSUFFICIENT_RESOURCES: &str = "53";
/// Program limit exceeded (class `54`).
pub const CLASS_PROGRAM_LIMIT_EXCEEDED: &str = "54";
/// Object not in prerequisite state (class `55`).
pub const CLASS_OBJECT_NOT_IN_PREREQUISITE_STATE: &str = "55";
/// Operator intervention (class `57`).
pub const CLASS_OPERATOR_INTERVENTION: &str = "57";
/// System error (class `58`).
pub const CLASS_SYSTEM_ERROR: &str = "58";
/// Configuration file error (class `F0`).
pub const CLASS_CONFIGURATION_FILE_ERROR: &str = "F0";
/// Foreign data wrapper error (class `HV`).
pub const CLASS_FDW_ERROR: &str = "HV";
/// PL/pgSQL error (class `P0`).
pub const CLASS_PLPGSQL_ERROR: &str = "P0";
/// Internal error (class `XX`).
pub const CLASS_INTERNAL_ERROR: &str = "XX";

// ===========================================================================
// Specific error codes (5 characters)
// ===========================================================================

// --- Class 08 - Connection Exception ---
pub const CONNECTION_DOES_NOT_EXIST: &str = "08003";
pub const CONNECTION_FAILURE: &str = "08006";
pub const SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION: &str = "08001";
pub const SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION: &str = "08004";
pub const TRANSACTION_RESOLUTION_UNKNOWN: &str = "08007";
pub const PROTOCOL_VIOLATION: &str = "08P01";

// --- Class 22 - Data Exception ---
pub const ARRAY_SUBSCRIPT_ERROR: &str = "2202E";
pub const CHARACTER_NOT_IN_REPERTOIRE: &str = "22021";
pub const DATETIME_FIELD_OVERFLOW: &str = "22008";
pub const DIVISION_BY_ZERO: &str = "22012";
pub const ERROR_IN_ASSIGNMENT: &str = "22005";
pub const ESCAPE_CHARACTER_CONFLICT: &str = "2200B";
pub const INDICATOR_OVERFLOW: &str = "22022";
pub const INTERVAL_FIELD_OVERFLOW: &str = "22015";
pub const INVALID_ARGUMENT_FOR_LOGARITHM: &str = "2201E";
pub const INVALID_ARGUMENT_FOR_NTILE_FUNCTION: &str = "22014";
pub const INVALID_ARGUMENT_FOR_NTH_VALUE_FUNCTION: &str = "22016";
pub const INVALID_ARGUMENT_FOR_POWER_FUNCTION: &str = "2201F";
pub const INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION: &str = "2201G";
pub const INVALID_CHARACTER_VALUE_FOR_CAST: &str = "22018";
pub const INVALID_DATETIME_FORMAT: &str = "22007";
pub const INVALID_ESCAPE_CHARACTER: &str = "22019";
pub const INVALID_ESCAPE_OCTET: &str = "2200D";
pub const INVALID_ESCAPE_SEQUENCE: &str = "22025";
pub const INVALID_INDICATOR_PARAMETER_VALUE: &str = "22010";
pub const INVALID_LIMIT_VALUE: &str = "22020";
pub const INVALID_PARAMETER_VALUE: &str = "22023";
pub const INVALID_PRECEDING_OR_FOLLOWING_SIZE: &str = "22013";
pub const INVALID_REGULAR_EXPRESSION: &str = "2201B";
pub const INVALID_ROW_COUNT_IN_LIMIT_CLAUSE: &str = "2201W";
pub const INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE: &str = "2201X";
pub const INVALID_TABLESAMPLE_ARGUMENT: &str = "2202H";
pub const INVALID_TABLESAMPLE_REPEAT: &str = "2202G";
pub const INVALID_TIME_ZONE_DISPLACEMENT_VALUE: &str = "22009";
pub const INVALID_USE_OF_ESCAPE_CHARACTER: &str = "2200C";
pub const MOST_SPECIFIC_TYPE_MISMATCH: &str = "2200G";
pub const NULL_VALUE_NOT_ALLOWED: &str = "22004";
pub const NULL_VALUE_NO_INDICATOR_PARAMETER: &str = "22002";
pub const NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";
pub const STRING_DATA_RIGHT_TRUNCATION: &str = "22001";
pub const STRING_DATA_LENGTH_MISMATCH: &str = "22026";
pub const SUBSTRING_ERROR: &str = "22011";
pub const TRIM_ERROR: &str = "22027";
pub const UNTERMINATED_C_STRING: &str = "22024";
pub const ZERO_LENGTH_CHARACTER_STRING: &str = "2200F";
pub const FLOATING_POINT_EXCEPTION: &str = "22P01";
pub const INVALID_TEXT_REPRESENTATION: &str = "22P02";
pub const INVALID_BINARY_REPRESENTATION: &str = "22P03";
pub const BAD_COPY_FILE_FORMAT: &str = "22P04";
pub const UNTRANSLATABLE_CHARACTER: &str = "22P05";
pub const NOT_AN_XML_DOCUMENT: &str = "2200L";
pub const INVALID_XML_DOCUMENT: &str = "2200M";
pub const INVALID_XML_CONTENT: &str = "2200N";
pub const INVALID_XML_COMMENT: &str = "2200S";
pub const INVALID_XML_PROCESSING_INSTRUCTION: &str = "2200T";

// --- Class 23 - Integrity Constraint Violation ---
pub const INTEGRITY_CONSTRAINT_VIOLATION: &str = "23000";
pub const RESTRICT_VIOLATION: &str = "23001";
pub const NOT_NULL_VIOLATION: &str = "23502";
pub const FOREIGN_KEY_VIOLATION: &str = "23503";
pub const UNIQUE_VIOLATION: &str = "23505";
pub const CHECK_VIOLATION: &str = "23514";
pub const EXCLUSION_VIOLATION: &str = "23P01";

// --- Class 25 - Invalid Transaction State ---
pub const ACTIVE_SQL_TRANSACTION: &str = "25001";
pub const BRANCH_TRANSACTION_ALREADY_ACTIVE: &str = "25002";
pub const HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL: &str = "25008";
pub const INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION: &str = "25003";
pub const INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION: &str = "25004";
pub const NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION: &str = "25005";
pub const READ_ONLY_SQL_TRANSACTION: &str = "25006";
pub const SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED: &str = "25007";
pub const NO_ACTIVE_SQL_TRANSACTION: &str = "25P01";
pub const IN_FAILED_SQL_TRANSACTION: &str = "25P02";
pub const IDLE_IN_TRANSACTION_SESSION_TIMEOUT: &str = "25P03";

// --- Class 40 - Transaction Rollback ---
pub const SERIALIZATION_FAILURE: &str = "40001";
pub const TRANSACTION_INTEGRITY_CONSTRAINT_VIOLATION: &str = "40002";
pub const STATEMENT_COMPLETION_UNKNOWN: &str = "40003";
pub const DEADLOCK_DETECTED: &str = "40P01";

// --- Class 42 - Syntax Error or Access Rule Violation ---
pub const SYNTAX_ERROR: &str = "42601";
pub const INSUFFICIENT_PRIVILEGE: &str = "42501";
pub const CANNOT_COERCE: &str = "42846";
pub const GROUPING_ERROR: &str = "42803";
pub const WINDOWING_ERROR: &str = "42P20";
pub const INVALID_RECURSION: &str = "42P19";
pub const INVALID_FOREIGN_KEY: &str = "42830";
pub const INVALID_FUNCTION_DEFINITION: &str = "42P13";
pub const NAME_TOO_LONG: &str = "42622";
pub const DUPLICATE_COLUMN: &str = "42701";
pub const DUPLICATE_CURSOR: &str = "42P03";
pub const DUPLICATE_DATABASE: &str = "42P04";
pub const DUPLICATE_FUNCTION: &str = "42P05";
pub const DUPLICATE_PREPARED_STATEMENT: &str = "42P06";
pub const DUPLICATE_SCHEMA: &str = "42P07";
pub const DUPLICATE_TABLE: &str = "42P08";
pub const DUPLICATE_ALIAS: &str = "42712";
pub const DUPLICATE_OBJECT: &str = "42710";
pub const AMBIGUOUS_COLUMN: &str = "42702";
pub const AMBIGUOUS_FUNCTION: &str = "42725";
pub const AMBIGUOUS_PARAMETER: &str = "42P09";
pub const UNDEFINED_COLUMN: &str = "42703";
pub const UNDEFINED_FUNCTION: &str = "42883";
pub const UNDEFINED_TABLE: &str = "42P01";
pub const UNDEFINED_PARAMETER: &str = "42P02";
pub const UNDEFINED_OBJECT: &str = "42704";
pub const WRONG_OBJECT_TYPE: &str = "42809";

// --- Class 53 - Insufficient Resources ---
pub const DISK_FULL: &str = "53100";
pub const OUT_OF_MEMORY: &str = "53200";
pub const TOO_MANY_CONNECTIONS: &str = "53300";
pub const CONFIGURATION_LIMIT_EXCEEDED: &str = "53400";

// --- Class 54 - Program Limit Exceeded ---
pub const STATEMENT_TOO_COMPLEX: &str = "54001";
pub const TOO_MANY_COLUMNS: &str = "54011";
pub const TOO_MANY_ARGUMENTS: &str = "54023";

// --- Class 55 - Object Not In Prerequisite State ---
pub const OBJECT_IN_USE: &str = "55006";
pub const CANT_CHANGE_RUNTIME_PARAM: &str = "55P02";
pub const LOCK_NOT_AVAILABLE: &str = "55P03";

// --- Class 57 - Operator Intervention ---
pub const QUERY_CANCELED: &str = "57014";
pub const ADMIN_SHUTDOWN: &str = "57P01";
pub const CRASH_SHUTDOWN: &str = "57P02";
pub const CANNOT_CONNECT_NOW: &str = "57P03";
pub const DATABASE_DROPPED: &str = "57P04";
pub const IDLE_SESSION_TIMEOUT: &str = "57P05";

// --- Class 58 - System Error ---
pub const IO_ERROR: &str = "58030";
pub const UNDEFINED_FILE: &str = "58P01";
pub const DUPLICATE_FILE: &str = "58P02";

// --- Class XX - Internal Error ---
pub const INTERNAL_ERROR: &str = "XX000";
pub const DATA_CORRUPTED: &str = "XX001";
pub const INDEX_CORRUPTED: &str = "XX002";

// ===========================================================================
// Helper functions
// ===========================================================================

/// Returns the 2-character class prefix of a SQLSTATE code.
///
/// Returns an empty string if the code is shorter than 2 characters.
pub fn class_of(code: &str) -> &str {
    if code.len() >= 2 {
        &code[..2]
    } else {
        ""
    }
}

/// Returns `true` if the given SQLSTATE code belongs to the specified class.
pub fn is_class(code: &str, class: &str) -> bool {
    code.starts_with(class)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_class_of() {
        assert_eq!(class_of("23505"), "23");
        assert_eq!(class_of("42601"), "42");
        assert_eq!(class_of("08006"), "08");
        assert_eq!(class_of("5"), "");
        assert_eq!(class_of(""), "");
    }

    #[test]
    fn test_is_class() {
        assert!(is_class("23505", "23"));
        assert!(is_class("42601", "42"));
        assert!(!is_class("23505", "42"));
    }

    #[test]
    fn test_well_known_codes() {
        assert_eq!(UNIQUE_VIOLATION, "23505");
        assert_eq!(FOREIGN_KEY_VIOLATION, "23503");
        assert_eq!(NOT_NULL_VIOLATION, "23502");
        assert_eq!(CHECK_VIOLATION, "23514");
        assert_eq!(SERIALIZATION_FAILURE, "40001");
        assert_eq!(DEADLOCK_DETECTED, "40P01");
        assert_eq!(SYNTAX_ERROR, "42601");
        assert_eq!(INSUFFICIENT_PRIVILEGE, "42501");
        assert_eq!(UNDEFINED_TABLE, "42P01");
        assert_eq!(UNDEFINED_COLUMN, "42703");
        assert_eq!(QUERY_CANCELED, "57014");
        assert_eq!(ADMIN_SHUTDOWN, "57P01");
        assert_eq!(CONNECTION_FAILURE, "08006");
        assert_eq!(PROTOCOL_VIOLATION, "08P01");
    }
}
