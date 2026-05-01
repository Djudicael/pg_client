//! Query result types.
//!
//! This module defines [`CommandTag`], [`QueryResult`], and [`ExecuteResult`].

use std::sync::Arc;

use crate::query::row::{FieldDescription, Row};

// ---------------------------------------------------------------------------
// CommandTag
// ---------------------------------------------------------------------------

/// The command tag returned by PostgreSQL for a completed command
/// (e.g. `"SELECT 3"`, `"INSERT 0 1"`, `"UPDATE 4"`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct CommandTag {
    tag: String,
}

impl CommandTag {
    /// Create a new command tag from the raw string.
    pub fn new(tag: String) -> Self {
        Self { tag }
    }

    /// The raw tag string.
    pub fn as_str(&self) -> &str {
        &self.tag
    }

    /// Parse the number of rows affected from the tag.
    ///
    /// Returns `None` for commands that do not have a row count
    /// (e.g. `CREATE TABLE`).
    pub fn rows_affected(&self) -> Option<u64> {
        // Tags have the form:
        //   INSERT 0 <rows>
        //   UPDATE <rows>
        //   DELETE <rows>
        //   SELECT <rows>
        //   MOVE <rows>
        //   FETCH <rows>
        //   COPY <rows>
        let parts: Vec<&str> = self.tag.split_whitespace().collect();
        match parts.as_slice() {
            ["INSERT", _, n] => n.parse().ok(),
            ["UPDATE", n] => n.parse().ok(),
            ["DELETE", n] => n.parse().ok(),
            ["SELECT", n] => n.parse().ok(),
            ["MOVE", n] => n.parse().ok(),
            ["FETCH", n] => n.parse().ok(),
            ["COPY", n] => n.parse().ok(),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// QueryResult
// ---------------------------------------------------------------------------

/// Result of a query that returns rows.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct QueryResult {
    rows: Vec<Row>,
    command_tag: CommandTag,
    columns: Arc<Vec<FieldDescription>>,
}

impl QueryResult {
    pub(crate) fn new(
        rows: Vec<Row>,
        command_tag: CommandTag,
        columns: Arc<Vec<FieldDescription>>,
    ) -> Self {
        Self {
            rows,
            command_tag,
            columns,
        }
    }

    /// Returns the number of rows in the result.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` if the result contains no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns the rows.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Consumes the result and returns the rows vector.
    pub fn into_rows(self) -> Vec<Row> {
        self.rows
    }

    /// Returns the command tag.
    pub fn command_tag(&self) -> &CommandTag {
        &self.command_tag
    }

    /// Returns the column descriptions.
    pub fn columns(&self) -> &[FieldDescription] {
        &self.columns
    }

    /// Returns the number of rows affected (if parseable from the tag).
    pub fn rows_affected(&self) -> Option<u64> {
        self.command_tag.rows_affected()
    }

    /// Returns an iterator over the rows.
    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }
}

// ---------------------------------------------------------------------------
// ExecuteResult
// ---------------------------------------------------------------------------

/// Result for statements that do not return rows (INSERT, UPDATE, DELETE, DDL).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExecuteResult {
    command_tag: CommandTag,
}

impl ExecuteResult {
    pub(crate) fn new(command_tag: CommandTag) -> Self {
        Self { command_tag }
    }

    /// Returns the command tag.
    pub fn command_tag(&self) -> &CommandTag {
        &self.command_tag
    }

    /// Returns the number of rows affected (if parseable from the tag).
    pub fn rows_affected(&self) -> Option<u64> {
        self.command_tag.rows_affected()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_tag_rows_affected() {
        assert_eq!(CommandTag::new("SELECT 3".into()).rows_affected(), Some(3));
        assert_eq!(
            CommandTag::new("INSERT 0 1".into()).rows_affected(),
            Some(1)
        );
        assert_eq!(CommandTag::new("UPDATE 4".into()).rows_affected(), Some(4));
        assert_eq!(CommandTag::new("DELETE 0".into()).rows_affected(), Some(0));
        assert_eq!(CommandTag::new("CREATE TABLE".into()).rows_affected(), None);
        assert_eq!(CommandTag::new("".into()).rows_affected(), None);
    }

    #[test]
    fn test_query_result_empty() {
        let qr = QueryResult::new(
            Vec::new(),
            CommandTag::new("SELECT 0".into()),
            Arc::new(Vec::new()),
        );
        assert!(qr.is_empty());
        assert_eq!(qr.len(), 0);
        assert_eq!(qr.rows_affected(), Some(0));
    }

    #[test]
    fn test_execute_result_rows_affected() {
        let er = ExecuteResult::new(CommandTag::new("UPDATE 5".into()));
        assert_eq!(er.rows_affected(), Some(5));
    }
}
