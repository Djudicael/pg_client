//! Transaction options: isolation levels, read-only, deferrable.

/// Isolation level for a PostgreSQL transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IsolationLevel {
    /// Read Uncommitted (in PostgreSQL, this behaves like Read Committed).
    ReadUncommitted,
    /// Read Committed (default).
    ReadCommitted,
    /// Repeatable Read.
    RepeatableRead,
    /// Serializable.
    Serializable,
}

impl IsolationLevel {
    /// Returns the SQL fragment for this isolation level.
    pub fn as_str(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

/// Options for beginning a transaction.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct TransactionOptions {
    /// Desired isolation level.
    pub isolation_level: Option<IsolationLevel>,
    /// `true` for read-only, `false` for read-write, `None` to omit.
    pub read_only: Option<bool>,
    /// `true` for deferrable (only valid with SERIALIZABLE and READ ONLY).
    pub deferrable: Option<bool>,
}

impl TransactionOptions {
    /// Create a new `TransactionOptions` with all fields set to `None`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the isolation level.
    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = Some(level);
        self
    }

    /// Set read-only mode.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = Some(read_only);
        self
    }

    /// Set deferrable mode.
    pub fn deferrable(mut self, deferrable: bool) -> Self {
        self.deferrable = Some(deferrable);
        self
    }

    /// Build the `BEGIN ...` SQL statement from these options.
    pub fn to_begin_sql(&self) -> String {
        let mut sql = String::from("BEGIN");
        if let Some(level) = self.isolation_level {
            sql.push_str(" ISOLATION LEVEL ");
            sql.push_str(level.as_str());
        }
        if let Some(read_only) = self.read_only {
            sql.push_str(if read_only {
                " READ ONLY"
            } else {
                " READ WRITE"
            });
        }
        if self.deferrable == Some(true) {
            sql.push_str(" DEFERRABLE");
        }
        sql
    }
}
