//! PostgreSQL type system: ToSql/FromSql, OID mapping, encoding.
//!
//! This crate wraps the battle-tested [`postgres-types`](https://docs.rs/postgres-types)
//! crate and adds **format-aware** `ToSql` / `FromSql` traits so the same
//! traits can be used for both the **text** format (simple query) and the
//! **binary** format (extended query).
//!
//! # Design
//!
//! - **Type metadata** — `Type`, `Oid`, `Kind`, `Field` come from `postgres-types`.
//! - **Conversion traits** — `pg_types::ToSql` and `pg_types::FromSql` accept a
//!   [`Format`] argument and delegate binary work to `postgres-protocol::types`.
//! - **Optional integrations** — `uuid`, `chrono`, `serde_json` via feature flags.
//!
//! # Example
//! ```
//! use pg_types::{ToSql, FromSql, Type, Format};
//!
//! // Text format
//! let mut buf = Vec::new();
//! 42i32.to_sql(&Type::INT4, &mut buf, Format::Text).unwrap();
//! assert_eq!(&buf, b"42");
//!
//! // Binary format
//! let mut buf = Vec::new();
//! 42i32.to_sql(&Type::INT4, &mut buf, Format::Binary).unwrap();
//! assert_eq!(&buf, &[0, 0, 0, 42]);
//! ```

pub use postgres_types::{
    Date, Field, IsNull, Kind, Oid, PgLsn, Timestamp, Type, WasNull, WrongType,
};

mod decode;
mod encode;
mod extern_types;
mod oid;
mod types;

#[cfg(test)]
mod tests;

pub use decode::FromSql;
pub use encode::ToSql;
pub use oid::*;
pub use types::Format;

/// Errors that can occur during type conversion or encoding/decoding.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The type is not supported for conversion.
    #[error("unsupported type: {0}")]
    UnsupportedType(String),
    /// The value cannot be converted to the target type.
    #[error("conversion error: {0}")]
    Conversion(String),
    /// The data format is invalid.
    #[error("invalid data format: {0}")]
    InvalidDataFormat(String),
    /// An error occurred during UTF-8 conversion.
    #[error("utf-8 error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    /// An error occurred while parsing an integer.
    #[error("parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    /// An error occurred while parsing a float.
    #[error("parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
    /// An error occurred while parsing a bool.
    #[error("parse bool error: {0}")]
    ParseBoolError(#[from] std::str::ParseBoolError),
    /// An error occurred while parsing a date/time.
    #[error("parse datetime error: {0}")]
    ParseDateTimeError(String),
    /// The OID is not recognized.
    #[error("unknown OID: {0}")]
    UnknownOid(u32),
    /// The encoding format is not supported for the given type.
    #[error("unsupported encoding format for type: {0}")]
    UnsupportedEncoding(String),
    /// A generic postgres-types error.
    #[error("postgres-types error: {0}")]
    PostgresTypes(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Conversion(e.to_string())
    }
}

/// A specialized `Result` type for type system operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Look up a `Type` by its OID.
///
/// This is a thin wrapper around [`postgres_types::Type::from_oid`].
pub fn type_from_oid(oid: Oid) -> Option<Type> {
    Type::from_oid(oid)
}
