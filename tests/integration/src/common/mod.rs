//! Shared test helpers for integration and protocol tests.

use wasi_pg_client::pool::PoolConfig;
use wasi_pg_client::{Config, PgError};

/// Create a test configuration from environment variables.
pub fn test_config() -> Config {
    let url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string());
    Config::from_uri(&url).expect("invalid TEST_DATABASE_URL")
}

/// Create a test pool configuration.
pub fn test_pool_config() -> PoolConfig {
    PoolConfig::default().connection(test_config()).max_size(5)
}

/// Create a test pool configuration with fast settings (no health checks).
pub fn test_pool_config_fast() -> PoolConfig {
    PoolConfig::default()
        .connection(test_config())
        .max_size(5)
        .test_on_acquire(false)
}

/// Assert that a PgError is a specific SQLSTATE code.
pub fn assert_sqlstate(err: &PgError, expected_code: &str) {
    match err {
        PgError::Server(e) => assert_eq!(
            e.code, expected_code,
            "Expected SQLSTATE {}, got {}",
            expected_code, e.code
        ),
        other => panic!("Expected PgError::Server, got: {:?}", other),
    }
}
