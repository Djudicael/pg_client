//! Stub integration test for step-01 project setup.
//!
//! These tests run natively (not via WASI) using the `test-native` feature.
//! They validate that the client compiles, basic types work, and (once TCP
//! transport is implemented) can talk to a real PostgreSQL instance.
//!
//! The CI job `integration-tests` spins up a PostgreSQL container and sets
//! `TEST_DATABASE_URL`.  For now we only exercise the Config / Connection
//! scaffolding because the wire-protocol handshake is not yet implemented.

use std::time::Duration;
use wasi_pg_client::{Config, Connection};

fn db_config() -> Config {
    // When CI runs, TEST_DATABASE_URL is set.  For local runs we use defaults.
    Config::new()
        .host("localhost")
        .port(5432)
        .user("postgres")
        .password("postgres")
        .database("test")
        .connect_timeout(Duration::from_secs(5))
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL server (use e2e_tls test with --ignored for real integration testing)"]
async fn test_connect_and_close() {
    // This test attempts a real TCP connection + auth handshake.
    // Run with a PostgreSQL container available on localhost:5432.
    let config = db_config();
    let mut conn = Connection::connect(&config)
        .await
        .expect("connect should succeed");
    assert!(!conn.is_closed());
    conn.close().await.expect("close should succeed");
    assert!(conn.is_closed());
}

#[tokio::test]
async fn test_config_builder() {
    let config = Config::new()
        .host("my-host")
        .port(15432)
        .user("my-user")
        .password("secret")
        .database("my-db")
        .use_tls(false)
        .connect_timeout(Duration::from_secs(10));

    assert_eq!(config.get_host(), "my-host");
    assert_eq!(config.get_port(), 15432);
    assert_eq!(config.get_user(), "my-user");
    assert_eq!(config.get_password(), Some("secret"));
    assert_eq!(config.get_database(), Some("my-db"));
    assert!(!config.get_use_tls());
    assert_eq!(config.get_connect_timeout(), Some(Duration::from_secs(10)));
}
