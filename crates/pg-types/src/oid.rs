//! PostgreSQL Object Identifiers (OIDs) and type information.
//!
//! This module re-exports convenience constants for PostgreSQL built-in type OIDs.
//! For full type metadata use [`postgres_types::Type`] directly.

// Hard-coded OID constants for PostgreSQL built-in types.
// These match the values used in postgres-protocol / postgres-types.

/// OID for the `bool` type.
pub const BOOL_OID: u32 = 16;
/// OID for the `bytea` type.
pub const BYTEA_OID: u32 = 17;
/// OID for the `char` type (single character).
pub const CHAR_OID: u32 = 18;
/// OID for the `name` type (internal type for system identifiers).
pub const NAME_OID: u32 = 19;
/// OID for the `int8` type (bigint).
pub const INT8_OID: u32 = 20;
/// OID for the `int2` type (smallint).
pub const INT2_OID: u32 = 21;
/// OID for the `int4` type (integer).
pub const INT4_OID: u32 = 23;
/// OID for the `text` type.
pub const TEXT_OID: u32 = 25;
/// OID for the `oid` type (Object Identifier).
pub const OID_OID: u32 = 26;
/// OID for the `json` type.
pub const JSON_OID: u32 = 114;
/// OID for the `jsonb` type.
pub const JSONB_OID: u32 = 3802;
/// OID for the `float4` type (real).
pub const FLOAT4_OID: u32 = 700;
/// OID for the `float8` type (double precision).
pub const FLOAT8_OID: u32 = 701;
/// OID for the `varchar` type (variable-length string).
pub const VARCHAR_OID: u32 = 1043;
/// OID for the `date` type.
pub const DATE_OID: u32 = 1082;
/// OID for the `time` type (without time zone).
pub const TIME_OID: u32 = 1083;
/// OID for the `timestamp` type (without time zone).
pub const TIMESTAMP_OID: u32 = 1114;
/// OID for the `timestamptz` type (with time zone).
pub const TIMESTAMPTZ_OID: u32 = 1184;
/// OID for the `uuid` type.
pub const UUID_OID: u32 = 2950;
