//! Optional `ToSql` / `FromSql` implementations for third-party crate types.
//!
//! Each integration is gated behind its own feature flag:
//!
//! - `uuid` — `uuid::Uuid`
//! - `serde-json` — `serde_json::Value` and `JsonB` wrapper
//! - `chrono` — `chrono::DateTime<Utc>`

// ---------------------------------------------------------------------------
// uuid
// ---------------------------------------------------------------------------

#[cfg(feature = "uuid")]
mod uuid_impl {
    use uuid::Uuid;

    use crate::{Error, Format, FromSql, IsNull, Result, ToSql, Type};

    impl ToSql for Uuid {
        fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
            match format {
                Format::Text | Format::Binary => {
                    let s = self.hyphenated().to_string();
                    out.extend_from_slice(s.as_bytes());
                    Ok(IsNull::No)
                }
            }
        }

        fn accepts(ty: &Type) -> bool {
            *ty == Type::UUID
        }
    }

    impl FromSql for Uuid {
        fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
            let bytes = raw.ok_or_else(|| Error::Conversion("unexpected NULL for Uuid".into()))?;
            match format {
                Format::Text | Format::Binary => {
                    let s = std::str::from_utf8(bytes).map_err(Error::Utf8Error)?;
                    s.parse::<Uuid>()
                        .map_err(|e| Error::Conversion(format!("UUID parse: {e}")))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// serde_json
// ---------------------------------------------------------------------------

#[cfg(feature = "serde-json")]
pub mod serde_json_impl {
    use serde_json::Value;

    use crate::{Error, Format, FromSql, IsNull, Result, ToSql, Type};

    impl ToSql for Value {
        fn to_sql(&self, ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
            let json_str = serde_json::to_string(&self)
                .map_err(|e| Error::Conversion(format!("JSON serialize: {e}")))?;
            match format {
                Format::Text => {
                    out.extend_from_slice(json_str.as_bytes());
                }
                Format::Binary => {
                    // JSONB binary format: 1-byte version header (0x01) + JSON text
                    if *ty == Type::JSONB {
                        out.push(0x01);
                    }
                    out.extend_from_slice(json_str.as_bytes());
                }
            }
            Ok(IsNull::No)
        }

        fn accepts(ty: &Type) -> bool {
            *ty == Type::JSONB || *ty == Type::JSON
        }
    }

    impl FromSql for Value {
        fn from_sql(ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
            let bytes = raw.ok_or_else(|| Error::Conversion("unexpected NULL for Value".into()))?;
            match format {
                Format::Text => {
                    let s = std::str::from_utf8(bytes).map_err(Error::Utf8Error)?;
                    serde_json::from_str(s)
                        .map_err(|e| Error::Conversion(format!("JSON parse: {e}")))
                }
                Format::Binary => {
                    // JSONB binary format: 1-byte version header (0x01) + JSON text
                    // JSON binary format: plain JSON text (no header)
                    let json_bytes = if *ty == Type::JSONB {
                        if bytes.is_empty() {
                            return Err(Error::Conversion("empty JSONB value".into()));
                        }
                        if bytes[0] != 0x01 {
                            return Err(Error::Conversion(format!(
                                "unsupported JSONB version: {}",
                                bytes[0]
                            )));
                        }
                        &bytes[1..]
                    } else {
                        bytes
                    };
                    let s = std::str::from_utf8(json_bytes).map_err(Error::Utf8Error)?;
                    serde_json::from_str(s)
                        .map_err(|e| Error::Conversion(format!("JSON parse: {e}")))
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // JsonB wrapper
    // -----------------------------------------------------------------------

    /// A wrapper that serializes a `serde_json::Value` as a PostgreSQL JSONB value.
    ///
    /// Use this when you need to write a `Value` to a JSONB column with the
    /// correct binary format (version header byte).
    /// For reading JSONB columns, use `FromSql` on `Value` directly.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use pg_types::JsonB;
    ///
    /// let value = serde_json::json!({"key": 42});
    /// conn.query_params(sql, &[&JsonB(&value)]).await?;
    /// ```
    pub struct JsonB<T>(pub T);

    impl ToSql for JsonB<&Value> {
        fn to_sql(&self, ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
            self.0.to_sql(ty, out, format)
        }

        fn accepts(ty: &Type) -> bool {
            *ty == Type::JSONB || *ty == Type::JSON
        }
    }
}

// ---------------------------------------------------------------------------
// chrono
// ---------------------------------------------------------------------------

#[cfg(feature = "chrono")]
mod chrono_impl {
    use chrono::{DateTime, Utc};

    use crate::{Error, Format, FromSql, IsNull, Result, ToSql, Type};

    impl ToSql for DateTime<Utc> {
        fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>, format: Format) -> Result<IsNull> {
            match format {
                Format::Text | Format::Binary => {
                    let s = self.to_rfc3339();
                    out.extend_from_slice(s.as_bytes());
                    Ok(IsNull::No)
                }
            }
        }

        fn accepts(ty: &Type) -> bool {
            *ty == Type::TIMESTAMPTZ || *ty == Type::TIMESTAMP
        }
    }

    impl FromSql for DateTime<Utc> {
        fn from_sql(_ty: &Type, raw: Option<&[u8]>, format: Format) -> Result<Self> {
            let bytes =
                raw.ok_or_else(|| Error::Conversion("unexpected NULL for DateTime".into()))?;
            match format {
                Format::Text | Format::Binary => {
                    let s = std::str::from_utf8(bytes).map_err(Error::Utf8Error)?;
                    // Try RFC 3339 first
                    DateTime::parse_from_rfc3339(s)
                        .map(|dt| dt.with_timezone(&Utc))
                        .or_else(|_| {
                            // Try PostgreSQL's text format: "2024-01-15 10:30:00+00"
                            DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z")
                                .map(|dt| dt.with_timezone(&Utc))
                        })
                        .or_else(|_| {
                            // Try without timezone: "2024-01-15 10:30:00"
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                .map(|dt| dt.and_utc())
                        })
                        .or_else(|_| {
                            // Try with fractional seconds: "2024-01-15 10:30:00.123456+00"
                            DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
                                .map(|dt| dt.with_timezone(&Utc))
                        })
                        .map_err(|e| Error::ParseDateTimeError(format!("DateTime parse: {e}")))
                }
            }
        }
    }
}
