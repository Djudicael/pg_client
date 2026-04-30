//! Prepared statement cache with LRU eviction.
//!
//! Automatically caches prepared statements to avoid redundant `Parse`
//! round-trips when the same SQL is executed multiple times.

use std::collections::{HashMap, VecDeque};

use crate::query::prepared::PreparedStatement;

// ---------------------------------------------------------------------------
// StatementCache
// ---------------------------------------------------------------------------

/// A least-recently-used (LRU) cache for prepared statements.
///
/// The cache is keyed by SQL text. When a statement is looked up that is
/// not in the cache, it is prepared on the server and stored. If the cache
/// is at capacity, the least-recently-used statement is evicted (closed on
/// the server) before the new one is inserted.
#[derive(Debug, Clone)]
pub struct StatementCache {
    cache: HashMap<String, PreparedStatement>,
    capacity: usize,
    order: VecDeque<String>,
}

impl StatementCache {
    /// Create a new cache with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(capacity),
            capacity,
            order: VecDeque::with_capacity(capacity),
        }
    }

    /// Returns the number of statements currently cached.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Returns true if the cache contains no statements.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Look up a prepared statement by SQL text.
    ///
    /// Returns `Some` if the statement is cached. The statement is moved to
    /// the front of the LRU order.
    pub fn get(&mut self, sql: &str) -> Option<&PreparedStatement> {
        if self.cache.contains_key(sql) {
            // Move to front (most recently used)
            self.order.retain(|s| s != sql);
            self.order.push_front(sql.to_string());
            self.cache.get(sql)
        } else {
            None
        }
    }

    /// Insert a prepared statement into the cache.
    ///
    /// If the cache is at capacity, the LRU entry is removed from the map
    /// (but **not** closed on the server — the caller must do that if needed).
    /// Returns the evicted statement, if any.
    pub fn insert(&mut self, stmt: PreparedStatement) -> Option<PreparedStatement> {
        let sql = stmt.sql().to_string();

        // Remove old entry if present
        self.order.retain(|s| s != &sql);

        let evicted = if self.cache.len() >= self.capacity && !self.cache.contains_key(&sql) {
            self.order.pop_back().and_then(|old_sql| self.cache.remove(&old_sql))
        } else {
            None
        };

        self.order.push_front(sql.clone());
        self.cache.insert(sql, stmt);
        evicted
    }

    /// Remove a statement from the cache by SQL text.
    ///
    /// Returns the removed statement, if any.
    pub fn remove(&mut self, sql: &str) -> Option<PreparedStatement> {
        self.order.retain(|s| s != sql);
        self.cache.remove(sql)
    }

    /// Clear all entries from the cache.
    ///
    /// Returns the evicted statements. The caller is responsible for closing
    /// them on the server if needed.
    pub fn clear(&mut self) -> Vec<PreparedStatement> {
        self.order.clear();
        self.cache.drain().map(|(_, v)| v).collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn dummy_stmt(sql: &str, name: &str) -> PreparedStatement {
        PreparedStatement {
            name: name.into(),
            sql: sql.into(),
            param_types: vec![],
            columns: Arc::new(vec![]),
        }
    }

    #[test]
    fn test_cache_insert_and_get() {
        let mut cache = StatementCache::new(2);
        let stmt = dummy_stmt("SELECT 1", "s1");
        cache.insert(stmt);

        assert_eq!(cache.len(), 1);
        assert!(cache.get("SELECT 1").is_some());
        assert!(cache.get("SELECT 2").is_none());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let mut cache = StatementCache::new(2);
        cache.insert(dummy_stmt("SELECT 1", "s1"));
        cache.insert(dummy_stmt("SELECT 2", "s2"));

        // Access SELECT 1 to make it MRU
        let _ = cache.get("SELECT 1");

        // Insert SELECT 3 — should evict SELECT 2 (LRU)
        let evicted = cache.insert(dummy_stmt("SELECT 3", "s3"));
        assert!(evicted.is_some());
        assert_eq!(evicted.unwrap().sql(), "SELECT 2");

        assert!(cache.get("SELECT 1").is_some());
        assert!(cache.get("SELECT 2").is_none());
        assert!(cache.get("SELECT 3").is_some());
    }

    #[test]
    fn test_cache_remove() {
        let mut cache = StatementCache::new(2);
        cache.insert(dummy_stmt("SELECT 1", "s1"));

        let removed = cache.remove("SELECT 1");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().sql(), "SELECT 1");
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = StatementCache::new(2);
        cache.insert(dummy_stmt("SELECT 1", "s1"));
        cache.insert(dummy_stmt("SELECT 2", "s2"));

        let cleared = cache.clear();
        assert_eq!(cleared.len(), 2);
        assert!(cache.is_empty());
    }
}
