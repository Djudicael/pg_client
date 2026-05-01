//! Configuration for connection pooling.
//!
//! This module defines the `PoolConfig` struct which holds parameters for
//! configuring a connection pool.

use std::time::Duration;
use wasi_pg_client::Config;

/// Configuration for a connection pool.
///
/// Use `PoolConfig::default()` to create a default configuration and then
/// set fields using the builder methods.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PoolConfig {
    /// Database connection configuration.
    pub(crate) connection: Config,

    /// Minimum number of idle connections to maintain.
    /// The pool will pre-create this many connections on startup.
    /// Default: 0 (no pre-warming).
    pub(crate) min_idle: usize,

    /// Maximum number of connections in the pool (idle + active).
    /// Default: 10.
    pub(crate) max_size: usize,

    /// Maximum time to wait for a connection from the pool when
    /// all connections are busy and max_size is reached.
    /// Default: 30 seconds.
    pub(crate) acquire_timeout: Option<Duration>,

    /// Maximum lifetime of a connection from creation.
    /// Connections older than this are discarded on return to the pool.
    /// Default: 30 minutes.
    pub(crate) max_lifetime: Option<Duration>,

    /// Maximum time a connection can sit idle in the pool.
    /// Idle connections older than this are discarded during acquire.
    /// Default: 10 minutes.
    pub(crate) idle_timeout: Option<Duration>,

    /// Whether to test connections with a ping before lending them out.
    /// Adds a round-trip per acquire but guarantees the connection is alive.
    /// Default: true.
    pub(crate) test_on_acquire: bool,

    /// SQL to run when a new connection is created.
    /// Useful for session-level settings like `SET timezone = 'UTC'`.
    /// Default: None.
    pub(crate) after_connect: Option<String>,

    /// SQL to run when a connection is returned to the pool.
    /// Useful for resetting session state like `RESET ALL`.
    /// Default: None.
    pub(crate) before_return: Option<String>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            connection: Config::default(),
            min_idle: 0,
            max_size: 10,
            acquire_timeout: Some(Duration::from_secs(30)),
            max_lifetime: Some(Duration::from_secs(1800)),
            idle_timeout: Some(Duration::from_secs(600)),
            test_on_acquire: true,
            after_connect: None,
            before_return: None,
        }
    }
}

impl PoolConfig {
    /// Sets the database connection configuration.
    pub fn connection(mut self, connection: Config) -> Self {
        self.connection = connection;
        self
    }

    /// Sets the minimum number of idle connections to maintain.
    pub fn min_idle(mut self, min_idle: usize) -> Self {
        self.min_idle = min_idle;
        self
    }

    /// Sets the maximum number of connections in the pool.
    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self
    }

    /// Sets the maximum time to wait for a connection from the pool.
    pub fn acquire_timeout(mut self, acquire_timeout: Option<Duration>) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }

    /// Sets the maximum lifetime of a connection from creation.
    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Self {
        self.max_lifetime = max_lifetime;
        self
    }

    /// Sets the idle timeout for connections in the pool.
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets whether to test connections with a ping before lending them out.
    pub fn test_on_acquire(mut self, test_on_acquire: bool) -> Self {
        self.test_on_acquire = test_on_acquire;
        self
    }

    /// Sets SQL to run when a new connection is created.
    pub fn after_connect(mut self, sql: impl Into<String>) -> Self {
        self.after_connect = Some(sql.into());
        self
    }

    /// Sets SQL to run when a connection is returned to the pool.
    pub fn before_return(mut self, sql: impl Into<String>) -> Self {
        self.before_return = Some(sql.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PoolConfig::default();
        assert_eq!(config.max_size, 10);
        assert_eq!(config.min_idle, 0);
        assert_eq!(
            config.acquire_timeout,
            Some(std::time::Duration::from_secs(30))
        );
        assert_eq!(
            config.max_lifetime,
            Some(std::time::Duration::from_secs(1800))
        );
        assert_eq!(
            config.idle_timeout,
            Some(std::time::Duration::from_secs(600))
        );
        assert!(config.test_on_acquire);
        assert!(config.after_connect.is_none());
        assert!(config.before_return.is_none());
    }

    #[test]
    fn test_builder_methods() {
        let config = PoolConfig::default()
            .max_size(20)
            .min_idle(5)
            .acquire_timeout(Some(std::time::Duration::from_secs(10)))
            .max_lifetime(Some(std::time::Duration::from_secs(3600)))
            .idle_timeout(Some(std::time::Duration::from_secs(300)))
            .test_on_acquire(false)
            .after_connect("SET timezone = 'UTC'")
            .before_return("RESET ALL");

        assert_eq!(config.max_size, 20);
        assert_eq!(config.min_idle, 5);
        assert_eq!(
            config.acquire_timeout,
            Some(std::time::Duration::from_secs(10))
        );
        assert_eq!(
            config.max_lifetime,
            Some(std::time::Duration::from_secs(3600))
        );
        assert_eq!(
            config.idle_timeout,
            Some(std::time::Duration::from_secs(300))
        );
        assert!(!config.test_on_acquire);
        assert_eq!(
            config.after_connect.as_deref(),
            Some("SET timezone = 'UTC'")
        );
        assert_eq!(config.before_return.as_deref(), Some("RESET ALL"));
    }
}
