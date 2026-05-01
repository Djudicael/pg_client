//! Pool status and metrics.
//!
//! This module defines the `PoolStatus` struct which provides information
//! about the current state of the connection pool.

/// Status and metrics of the connection pool.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PoolStatus {
    /// Number of idle connections in the pool.
    pub idle: usize,
    /// Number of currently active (checked out) connections.
    pub active: usize,
    /// Total number of connections ever created by this pool.
    pub total_created: u64,
    /// Maximum number of connections the pool can hold.
    pub max_size: usize,
    /// Whether the pool is closed.
    pub closed: bool,
}

impl PoolStatus {
    /// Total number of connections (idle + active).
    pub fn total(&self) -> usize {
        self.idle + self.active
    }

    /// Number of available slots for new connections.
    pub fn available(&self) -> usize {
        self.max_size.saturating_sub(self.total())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_status_total() {
        let status = PoolStatus {
            idle: 3,
            active: 5,
            total_created: 10,
            max_size: 20,
            closed: false,
        };
        assert_eq!(status.total(), 8);
    }

    #[test]
    fn test_pool_status_available() {
        let status = PoolStatus {
            idle: 3,
            active: 5,
            total_created: 10,
            max_size: 20,
            closed: false,
        };
        assert_eq!(status.available(), 12);
    }

    #[test]
    fn test_pool_status_available_saturating() {
        let status = PoolStatus {
            idle: 10,
            active: 15,
            total_created: 25,
            max_size: 20,
            closed: false,
        };
        // total > max_size shouldn't happen in practice, but available should be 0
        assert_eq!(status.available(), 0);
    }
}
