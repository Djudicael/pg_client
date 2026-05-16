//! Decoding of PostgreSQL values into Rust types.
//!
//! This module defines the `FromSql` trait and provides implementations for
//! common Rust types.  Both **text** (simple query) and **binary** (extended
//! query) formats are supported.

use postgres_protocol::types;
use postgres_types::Type;

use crate::{Error, Format, Result};

/// A trait for types that can be created from a PostgreSQL value.
///
/// This trait is used to convert values received from the PostgreSQL server
/// (in either text or binary format) into Rust types.
///
/// If the value is NULL, `raw` will be `None`. Implementations should
/// return an error for NULL unless the type is optional (e.g., `Option<T>`).
pub trait FromSql: Sized {
    /// Converts a PostgreSQL value into an instance of `Self`.
    fn from_sql(ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self>;
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn unexpected_null<T>() -> Result<T> {
    Err(Error::Conversion("unexpected NULL".into()))
}

fn decode_hex(s: &str) -> Result<Vec<u8>> {
    let chars: Vec<char> = s.chars().filter(|c| !c.is_whitespace()).collect();
    if chars.len() % 2 != 0 {
        return Err(Error::InvalidDataFormat(
            "hex string has an odd number of digits".into(),
        ));
    }

    let mut out = Vec::with_capacity(chars.len() / 2);
    let mut i = 0;
    while i < chars.len() {
        let h = chars[i];
        let l = chars[i + 1];
        let hi = h
            .to_digit(16)
            .ok_or_else(|| Error::Conversion(format!("invalid hex char: {h}")))?;
        let lo = l
            .to_digit(16)
            .ok_or_else(|| Error::Conversion(format!("invalid hex char: {l}")))?;
        out.push((hi * 16 + lo) as u8);
        i += 2;
    }
    Ok(out)
}

fn decode_bytea_escape(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] != b'\\' {
            out.push(bytes[i]);
            i += 1;
            continue;
        }

        i += 1;
        if i >= bytes.len() {
            return Err(Error::InvalidDataFormat(
                "trailing backslash in bytea escape format".into(),
            ));
        }

        match bytes[i] {
            b'\\' => {
                out.push(b'\\');
                i += 1;
            }
            b'0'..=b'7' => {
                if i + 2 >= bytes.len() {
                    return Err(Error::InvalidDataFormat(
                        "incomplete octal escape in bytea text format".into(),
                    ));
                }
                let oct = &bytes[i..i + 3];
                if !oct.iter().all(|b| matches!(b, b'0'..=b'7')) {
                    return Err(Error::InvalidDataFormat(
                        "invalid octal escape in bytea text format".into(),
                    ));
                }
                let value = ((oct[0] - b'0') << 6) | ((oct[1] - b'0') << 3) | (oct[2] - b'0');
                out.push(value);
                i += 3;
            }
            other => {
                return Err(Error::InvalidDataFormat(format!(
                    "invalid bytea escape sequence: \\{}",
                    other as char
                )));
            }
        }
    }

    Ok(out)
}

fn as_text(raw: Option<&[u8]>) -> Result<&str> {
    match raw {
        Some(b) => std::str::from_utf8(b).map_err(Error::Utf8Error),
        None => unexpected_null(),
    }
}

// ---------------------------------------------------------------------------
// bool
// ---------------------------------------------------------------------------

impl FromSql for bool {
    fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match format {
            Format::Text => match as_text(raw)? {
                "t" | "true" | "TRUE" | "True" | "1" | "yes" | "YES" | "on" | "ON" => Ok(true),
                "f" | "false" | "FALSE" | "False" | "0" | "no" | "NO" | "off" | "OFF" => Ok(false),
                s => Err(Error::Conversion(format!("invalid bool text: {s}"))),
            },
            Format::Binary => types::bool_from_sql(raw.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected NULL")
            })?)
            .map_err(|e| Error::Conversion(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// i8  (PostgreSQL "char" — a single byte)
// ---------------------------------------------------------------------------

impl FromSql for i8 {
    fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match format {
            Format::Text => {
                let s = as_text(raw)?;
                s.parse::<i8>().map_err(Error::ParseIntError)
            }
            Format::Binary => types::char_from_sql(raw.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected NULL")
            })?)
            .map_err(|e| Error::Conversion(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// i16
// ---------------------------------------------------------------------------

impl FromSql for i16 {
    fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match format {
            Format::Text => {
                let s = as_text(raw)?;
                s.parse::<i16>().map_err(Error::ParseIntError)
            }
            Format::Binary => types::int2_from_sql(raw.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected NULL")
            })?)
            .map_err(|e| Error::Conversion(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// i32
// ---------------------------------------------------------------------------

impl FromSql for i32 {
    fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match format {
            Format::Text => {
                let s = as_text(raw)?;
                s.parse::<i32>().map_err(Error::ParseIntError)
            }
            Format::Binary => types::int4_from_sql(raw.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected NULL")
            })?)
            .map_err(|e| Error::Conversion(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// i64
// ---------------------------------------------------------------------------

impl FromSql for i64 {
    fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match format {
            Format::Text => {
                let s = as_text(raw)?;
                s.parse::<i64>()
                    .map_err(|e| Error::Conversion(e.to_string()))
            }
            Format::Binary => types::int8_from_sql(raw.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected NULL")
            })?)
            .map_err(|e| Error::Conversion(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// u32  (OID)
// ---------------------------------------------------------------------------

impl FromSql for u32 {
    fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match format {
            Format::Text => {
                let s = as_text(raw)?;
                s.parse::<u32>()
                    .map_err(|e| Error::Conversion(e.to_string()))
            }
            Format::Binary => types::oid_from_sql(raw.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected NULL")
            })?)
            .map_err(|e| Error::Conversion(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// f32
// ---------------------------------------------------------------------------

impl FromSql for f32 {
    fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match format {
            Format::Text => {
                let s = as_text(raw)?;
                s.parse::<f32>().map_err(Error::ParseFloatError)
            }
            Format::Binary => types::float4_from_sql(raw.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected NULL")
            })?)
            .map_err(|e| Error::Conversion(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// f64
// ---------------------------------------------------------------------------

impl FromSql for f64 {
    fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match format {
            Format::Text => {
                let s = as_text(raw)?;
                s.parse::<f64>().map_err(Error::ParseFloatError)
            }
            Format::Binary => types::float8_from_sql(raw.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected NULL")
            })?)
            .map_err(|e| Error::Conversion(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// String
// ---------------------------------------------------------------------------

impl FromSql for String {
    fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match raw {
            Some(bytes) => match format {
                Format::Text => Ok(std::str::from_utf8(bytes)
                    .map_err(Error::Utf8Error)?
                    .to_string()),
                Format::Binary => types::text_from_sql(bytes)
                    .map(|s| s.to_string())
                    .map_err(|e| Error::Conversion(e.to_string())),
            },
            None => unexpected_null(),
        }
    }
}

// ---------------------------------------------------------------------------
// Vec<u8>  (BYTEA)
// ---------------------------------------------------------------------------

impl FromSql for Vec<u8> {
    fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match raw {
            Some(bytes) => match format {
                Format::Text => {
                    // PostgreSQL bytea text format: either hex (\x...) or escape.
                    // For simplicity we handle the common hex prefix.
                    if bytes.len() >= 2 && bytes[0] == b'\\' && bytes[1] == b'x' {
                        let hex = std::str::from_utf8(&bytes[2..]).map_err(Error::Utf8Error)?;
                        decode_hex(hex.trim())
                            .map_err(|e| Error::Conversion(format!("invalid bytea hex: {e}")))
                    } else {
                        decode_bytea_escape(bytes)
                    }
                }
                Format::Binary => Ok(types::bytea_from_sql(bytes).to_vec()),
            },
            None => unexpected_null(),
        }
    }
}

// ---------------------------------------------------------------------------
// Option<T>
// ---------------------------------------------------------------------------

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql(ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
        match raw {
            Some(bytes) => T::from_sql(ty, Some(bytes), format).map(Some),
            None => Ok(None),
        }
    }
}
