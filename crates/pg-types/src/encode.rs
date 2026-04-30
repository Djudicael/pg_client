//! Encoding of Rust types into PostgreSQL values.
//!
//! This module defines the `ToSql` trait and provides implementations for
//! common Rust types.  Both **text** (simple query) and **binary** (extended
//! query) formats are supported.

use postgres_types::Type;

use crate::{Format, Result};

/// A trait for types that can be converted into a PostgreSQL value.
///
/// The `ty` parameter indicates the PostgreSQL type that the value should be
/// encoded as. The `out` buffer should be filled with the encoded value.
///
/// The `format` parameter indicates whether the value should be encoded in
/// text or binary format.
pub trait ToSql {
    /// Converts `self` into a PostgreSQL value.
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()>;

    /// Returns whether this type can be encoded as the given PostgreSQL type.
    fn accepts(ty: &Type) -> bool
    where
        Self: Sized;
}

// ---------------------------------------------------------------------------
// bool
// ---------------------------------------------------------------------------

impl ToSql for bool {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match format {
            Format::Text => out.extend_from_slice(if *self { b"t" } else { b"f" }),
            Format::Binary => out.push(if *self { 1 } else { 0 }),
        }
        Ok(())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BOOL
    }
}

// ---------------------------------------------------------------------------
// i8
// ---------------------------------------------------------------------------

impl ToSql for i8 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.push(*self as u8),
        }
        Ok(())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::CHAR
    }
}

// ---------------------------------------------------------------------------
// i16
// ---------------------------------------------------------------------------

impl ToSql for i16 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT2
    }
}

// ---------------------------------------------------------------------------
// i32
// ---------------------------------------------------------------------------

impl ToSql for i32 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT4 || *ty == Type::INT2
    }
}

// ---------------------------------------------------------------------------
// i64
// ---------------------------------------------------------------------------

impl ToSql for i64 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT8
    }
}

// ---------------------------------------------------------------------------
// u32 (OID)
// ---------------------------------------------------------------------------

impl ToSql for u32 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::OID
    }
}

// ---------------------------------------------------------------------------
// f32
// ---------------------------------------------------------------------------

impl ToSql for f32 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::FLOAT4
    }
}

// ---------------------------------------------------------------------------
// f64
// ---------------------------------------------------------------------------

impl ToSql for f64 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::FLOAT8
    }
}

// ---------------------------------------------------------------------------
// String
// ---------------------------------------------------------------------------

impl ToSql for String {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match format {
            Format::Text | Format::Binary => out.extend_from_slice(self.as_bytes()),
        }
        Ok(())
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN
        )
    }
}

impl ToSql for &str {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        (*self).to_string().to_sql(ty, out, format)
    }

    fn accepts(ty: &Type) -> bool {
        String::accepts(ty)
    }
}

// ---------------------------------------------------------------------------
// Vec<u8> (BYTEA)
// ---------------------------------------------------------------------------

impl ToSql for Vec<u8> {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match format {
            Format::Text => {
                // Hex format: \xDEADBEEF
                out.push(b'\\');
                out.push(b'x');
                for byte in self {
                    out.push(hex_digit(byte >> 4));
                    out.push(hex_digit(byte & 0x0F));
                }
            }
            Format::Binary => out.extend_from_slice(self),
        }
        Ok(())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BYTEA
    }
}

impl ToSql for &[u8] {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        (*self).to_vec().to_sql(ty, out, format)
    }

    fn accepts(ty: &Type) -> bool {
        Vec::<u8>::accepts(ty)
    }
}

fn hex_digit(n: u8) -> u8 {
    match n {
        0..=9 => b'0' + n,
        10..=15 => b'A' + (n - 10),
        _ => b'0',
    }
}

// ---------------------------------------------------------------------------
// Option<T>
// ---------------------------------------------------------------------------

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<()> {
        match self {
            Some(v) => v.to_sql(ty, out, format),
            None => {
                // Caller (protocol layer) must set length to -1 for NULL.
                Ok(())
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        T::accepts(ty)
    }
}
