//! Type system definitions for PostgreSQL.
//!
//! This module defines the `Format` enum used in the `FromSql` and `ToSql` traits.

/// Format code for data representation.
///
/// PostgreSQL supports text and binary formats for data transmission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Text format (UTF-8 strings).
    Text = 0,
    /// Binary format (type-specific binary representation).
    Binary = 1,
}

impl Format {
    /// Converts a u16 to a Format.
    ///
    /// Returns `None` if the value is not 0 or 1.
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            0 => Some(Format::Text),
            1 => Some(Format::Binary),
            _ => None,
        }
    }

    /// Converts the Format to a u16.
    pub fn to_u16(self) -> u16 {
        self as u16
    }
}
