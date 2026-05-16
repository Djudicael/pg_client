use postgres_types::Type;

use super::{Format, IsNull, Result};

pub trait ToSql: Send + Sync {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull>;

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized;
}

impl ToSql for bool {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match format {
            Format::Text => out.extend_from_slice(if *self { b"t" } else { b"f" }),
            Format::Binary => out.push(if *self { 1 } else { 0 }),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BOOL
    }
}

impl ToSql for i8 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.push(*self as u8),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::CHAR
    }
}

impl ToSql for i16 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT2
    }
}

impl ToSql for i32 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT4
    }
}

impl ToSql for i64 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT8
    }
}

impl ToSql for u32 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::OID
    }
}

impl ToSql for f32 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::FLOAT4
    }
}

impl ToSql for f64 {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match format {
            Format::Text => out.extend_from_slice(self.to_string().as_bytes()),
            Format::Binary => out.extend_from_slice(&self.to_be_bytes()),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::FLOAT8
    }
}

impl ToSql for String {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match format {
            Format::Text | Format::Binary => out.extend_from_slice(self.as_bytes()),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN
        )
    }
}

impl ToSql for &str {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        (*self).to_string().to_sql(ty, out, format)
    }

    fn accepts(ty: &Type) -> bool {
        String::accepts(ty)
    }
}

impl ToSql for Vec<u8> {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match format {
            Format::Text => {
                out.push(b'\\');
                out.push(b'x');
                for byte in self {
                    out.push(hex_digit(byte >> 4));
                    out.push(hex_digit(byte & 0x0F));
                }
            }
            Format::Binary => out.extend_from_slice(self),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BYTEA
    }
}

impl ToSql for &[u8] {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
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

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
        match self {
            Some(v) => v.to_sql(ty, out, format),
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(ty: &Type) -> bool {
        T::accepts(ty)
    }
}
