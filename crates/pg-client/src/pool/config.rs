use crate::{Config, PgError};
use std::time::Duration;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PoolConfig {
    pub(crate) connection: Config,
    pub(crate) min_idle: usize,
    pub(crate) max_size: usize,
    pub(crate) acquire_timeout: Option<Duration>,
    pub(crate) max_lifetime: Option<Duration>,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) test_on_acquire: bool,
    pub(crate) after_connect: Option<String>,
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
    pub fn connection(mut self, connection: Config) -> Self {
        self.connection = connection;
        self
    }

    pub fn min_idle(mut self, min_idle: usize) -> Self {
        self.min_idle = min_idle;
        self
    }

    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self
    }

    pub fn acquire_timeout(mut self, acquire_timeout: Option<Duration>) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }

    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Self {
        self.max_lifetime = max_lifetime;
        self
    }

    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    pub fn test_on_acquire(mut self, test_on_acquire: bool) -> Self {
        self.test_on_acquire = test_on_acquire;
        self
    }

    pub fn after_connect(mut self, sql: impl Into<String>) -> Self {
        self.after_connect = Some(sql.into());
        self
    }

    pub fn before_return(mut self, sql: impl Into<String>) -> Self {
        self.before_return = Some(sql.into());
        self
    }

    pub fn validate(&self) -> Result<(), PgError> {
        if self.max_size == 0 {
            return Err(PgError::Config(
                "pool max_size must be greater than zero".to_string(),
            ));
        }

        if self.min_idle > self.max_size {
            return Err(PgError::Config(format!(
                "pool min_idle ({}) cannot exceed max_size ({})",
                self.min_idle, self.max_size
            )));
        }

        Ok(())
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
        assert_eq!(config.acquire_timeout, Some(Duration::from_secs(30)));
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(1800)));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(600)));
        assert!(config.test_on_acquire);
        assert!(config.after_connect.is_none());
        assert!(config.before_return.is_none());
    }

    #[test]
    fn test_builder_methods() {
        let config = PoolConfig::default()
            .max_size(20)
            .min_idle(5)
            .acquire_timeout(Some(Duration::from_secs(10)))
            .max_lifetime(Some(Duration::from_secs(3600)))
            .idle_timeout(Some(Duration::from_secs(300)))
            .test_on_acquire(false)
            .after_connect("SET timezone = 'UTC'")
            .before_return("RESET ALL");

        assert_eq!(config.max_size, 20);
        assert_eq!(config.min_idle, 5);
        assert_eq!(config.acquire_timeout, Some(Duration::from_secs(10)));
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(3600)));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(300)));
        assert!(!config.test_on_acquire);
        assert_eq!(
            config.after_connect.as_deref(),
            Some("SET timezone = 'UTC'")
        );
        assert_eq!(config.before_return.as_deref(), Some("RESET ALL"));
    }

    #[test]
    fn test_validate_rejects_zero_max_size() {
        let err = PoolConfig::default().max_size(0).validate().unwrap_err();
        assert!(matches!(err, PgError::Config(_)));
        assert_eq!(
            err.to_string(),
            "configuration error: pool max_size must be greater than zero"
        );
    }

    #[test]
    fn test_validate_rejects_min_idle_above_max_size() {
        let err = PoolConfig::default()
            .max_size(4)
            .min_idle(5)
            .validate()
            .unwrap_err();
        assert!(matches!(err, PgError::Config(_)));
        assert_eq!(
            err.to_string(),
            "configuration error: pool min_idle (5) cannot exceed max_size (4)"
        );
    }

    #[test]
    fn test_validate_accepts_equal_min_idle_and_max_size() {
        PoolConfig::default()
            .max_size(4)
            .min_idle(4)
            .validate()
            .unwrap();
    }
}
