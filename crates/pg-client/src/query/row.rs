//! Row representation and column metadata.
//!
//! This module defines [`FieldDescription`] (metadata for a single column)
//! and [`Row`] (a single result row with typed accessors).

use std::sync::Arc;

use pg_protocol::Oid;
use pg_types::{Format, FromSql};

use crate::error::{PgError, Result};

// ---------------------------------------------------------------------------
// FieldDescription
// ---------------------------------------------------------------------------

/// Metadata describing a single column in a query result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDescription {
    /// Column name.
    name: String,
    /// OID of the table the column belongs to (0 if not a table column).
    table_oid: Oid,
    /// Column number within the table (0 if not a table column).
    column_id: i16,
    /// OID of the column's data type.
    type_oid: Oid,
    /// Type size (negative for variable-length types).
    type_size: i16,
    /// Type modifier.
    type_modifier: i32,
    /// Format code used for this column (text = 0, binary = 1).
    format: i16,
}

impl FieldDescription {
    /// Create a new field description.
    pub fn new(
        name: String,
        table_oid: Oid,
        column_id: i16,
        type_oid: Oid,
        type_size: i16,
        type_modifier: i32,
        format: i16,
    ) -> Self {
        Self {
            name,
            table_oid,
            column_id,
            type_oid,
            type_size,
            type_modifier,
            format,
        }
    }

    /// Column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Table OID.
    pub fn table_oid(&self) -> Oid {
        self.table_oid
    }

    /// Column ID within the table.
    pub fn column_id(&self) -> i16 {
        self.column_id
    }

    /// Data type OID.
    pub fn type_oid(&self) -> Oid {
        self.type_oid
    }

    /// Type size.
    pub fn type_size(&self) -> i16 {
        self.type_size
    }

    /// Type modifier.
    pub fn type_modifier(&self) -> i32 {
        self.type_modifier
    }

    /// Format code.
    pub fn format(&self) -> i16 {
        self.format
    }
}

// ---------------------------------------------------------------------------
// Row
// ---------------------------------------------------------------------------

/// A single row from a query result.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Arc<Vec<FieldDescription>>,
    values: Vec<Option<Vec<u8>>>,
}

impl Row {
    /// Create a new row from column descriptions and raw values.
    pub(crate) fn new(columns: Arc<Vec<FieldDescription>>, values: Vec<Option<Vec<u8>>>) -> Self {
        Self { columns, values }
    }

    /// Number of columns in the row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the column descriptions.
    pub fn columns(&self) -> &[FieldDescription] {
        &self.columns
    }

    /// Returns `true` if the value at `index` is SQL NULL.
    pub fn is_null(&self, index: usize) -> bool {
        self.values.get(index).map_or(true, |v| v.is_none())
    }

    /// Returns the raw bytes for the column at `index`, or `None` if NULL.
    pub fn get_raw(&self, index: usize) -> Option<&[u8]> {
        self.values.get(index).and_then(|v| v.as_deref())
    }

    /// Decode the column at `index` as type `T`.
    pub fn get<T: FromSql>(&self, index: usize) -> Result<T> {
        let raw = self.get_raw(index);
        let field = self
            .columns
            .get(index)
            .ok_or_else(|| PgError::ColumnIndexOutOfBounds {
                index,
                count: self.columns.len(),
            })?;
        let ty = pg_types::Type::from_oid(field.type_oid).unwrap_or_else(|| {
            pg_types::Type::new(
                "unknown".into(),
                0,
                pg_types::Kind::Pseudo,
                "pg_catalog".into(),
            )
        });
        let format = if field.format() == 1 {
            Format::Binary
        } else {
            Format::Text
        };
        T::from_sql(&ty, raw, format).map_err(PgError::TypeConversion)
    }

    /// Decode a column by name as type `T`.
    pub fn get_by_name<T: FromSql>(&self, name: &str) -> Result<T> {
        let index = self
            .columns
            .iter()
            .position(|c| c.name() == name)
            .ok_or_else(|| PgError::ColumnNotFound {
                name: name.to_string(),
            })?;
        self.get(index)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_get_raw() {
        let cols = Arc::new(vec![FieldDescription::new(
            "id".into(),
            0,
            0,
            pg_types::INT4_OID,
            4,
            -1,
            0,
        )]);
        let row = Row::new(cols.clone(), vec![Some(vec![b'1', b'2', b'3'])]);
        assert_eq!(row.get_raw(0), Some(b"123".as_slice()));
        assert!(!row.is_null(0));
    }

    #[test]
    fn test_row_null() {
        let cols = Arc::new(vec![FieldDescription::new(
            "name".into(),
            0,
            0,
            pg_types::TEXT_OID,
            -1,
            -1,
            0,
        )]);
        let row = Row::new(cols, vec![None]);
        assert!(row.is_null(0));
        assert_eq!(row.get_raw(0), None);
    }

    #[test]
    fn test_row_get_i32() {
        let cols = Arc::new(vec![FieldDescription::new(
            "id".into(),
            0,
            0,
            pg_types::INT4_OID,
            4,
            -1,
            0,
        )]);
        let row = Row::new(cols, vec![Some(b"42".to_vec())]);
        let val: i32 = row.get(0).unwrap();
        assert_eq!(val, 42);
    }

    #[test]
    fn test_row_get_by_name() {
        let cols = Arc::new(vec![
            FieldDescription::new("id".into(), 0, 0, pg_types::INT4_OID, 4, -1, 0),
            FieldDescription::new("name".into(), 0, 0, pg_types::TEXT_OID, -1, -1, 0),
        ]);
        let row = Row::new(cols, vec![Some(b"1".to_vec()), Some(b"alice".to_vec())]);
        let id: i32 = row.get_by_name("id").unwrap();
        assert_eq!(id, 1);
        let name: String = row.get_by_name("name").unwrap();
        assert_eq!(name, "alice");
    }

    #[test]
    fn test_row_get_by_name_missing() {
        let cols = Arc::new(vec![]);
        let row = Row::new(cols, vec![]);
        assert!(row.get_by_name::<i32>("missing").is_err());
    }
}
