//! PostgreSQL type system: ToSql/FromSql, OID mapping, encoding.
//!
//! This module wraps the battle-tested [`postgres-types`](https://docs.rs/postgres-types)
//! crate and adds **format-aware** `ToSql` / `FromSql` traits so the same
//! traits can be used for both the **text** format (simple query) and the
//! **binary** format (extended query).

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
#[cfg(feature = "serde-json")]
pub use extern_types::serde_json_impl::JsonB;
pub use oid::*;
pub use types::Format;

/// Errors that can occur during type conversion or encoding/decoding.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unsupported type: {0}")]
    UnsupportedType(String),
    #[error("conversion error: {0}")]
    Conversion(String),
    #[error("invalid data format: {0}")]
    InvalidDataFormat(String),
    #[error("utf-8 error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error("parse bool error: {0}")]
    ParseBoolError(#[from] std::str::ParseBoolError),
    #[error("parse datetime error: {0}")]
    ParseDateTimeError(String),
    #[error("unknown OID: {0}")]
    UnknownOid(u32),
    #[error("unsupported encoding format for type: {0}")]
    UnsupportedEncoding(String),
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
pub fn type_from_oid(oid: Oid) -> Option<Type> {
    Type::from_oid(oid)
}
