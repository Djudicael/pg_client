//! Configuration for connection pooling.
//!
//! This module defines the `PoolConfig` struct which holds parameters for
//! configuring a connection pool.

use std::time::Duration;

/// Configuration for a connection pool.
///
/// Use `PoolConfig::default()` to create a default configuration and then
/// set fields using the builder methods.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool.
    pub(crate) max_size: usize,
    /// Minimum number of idle connections to maintain.
    pub(crate) min_idle: usize,
    /// Maximum lifetime of a connection in the pool.
    pub(crate) max_lifetime: Option<Duration>,
    /// Idle timeout for connections in the pool.
    pub(crate) idle_timeout: Option<Duration>,
    /// Connection timeout for establishing new connections.
    pub(crate) connect_timeout: Duration,
    /// Whether to test the connection on checkout.
    pub(crate) test_on_checkout: bool,
}

impl PoolConfig {
    /// Creates a new `PoolConfig` with default values.
    ///
    /// Defaults:
    /// - max_size: 10
    /// - min_idle: 0
    /// - max_lifetime: 30 minutes
    /// - idle_timeout: 10 minutes
    /// - connect_timeout: 30 seconds
    /// - test_on_checkout: false
    pub fn new() -> Self {
        Self {
            max_size: 10,
            min_idle: 0,
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            connect_timeout: Duration::from_secs(30),
            test_on_checkout: false,
        }
    }

    /// Sets the maximum number of connections in the pool.
    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self
    }

    /// Sets the minimum number of idle connections to maintain.
    pub fn min_idle(mut self, min_idle: usize) -> Self {
        self.min_idle = min_idle;
        self
    }

    /// Sets the maximum lifetime of a connection in the pool.
    ///
    /// If set to `None`, connections will not be expired based on lifetime.
    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Self {
        self.max_lifetime = max_lifetime;
        self
    }

    /// Sets the idle timeout for connections in the pool.
    ///
    /// If set to `None`, idle connections will not be expired.
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets the connection timeout for establishing new connections.
    pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self
    }

    /// Sets whether to test the connection on checkout.
    pub fn test_on_checkout(mut self, test_on_checkout: bool) -> Self {
        self.test_on_checkout = test_on_checkout;
        self
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self::new()
    }
}
