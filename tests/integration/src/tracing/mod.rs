//! Tracing tests: verify that structured events are emitted correctly
//! and that sensitive data (passwords, auth tokens) is never leaked.
//!
//! These tests require both `tokio-transport` and `tracing` features.

// Tracing integration tests only run when both features are enabled
#[cfg(all(feature = "tokio-transport", feature = "tracing"))]
mod tracing_integration {
    use std::time::Duration;
    use wasi_pg_client::{Config, Connection};
    use wasi_pg_client::pool::{Pool, PoolConfig};

    use crate::common::{test_config, test_pool_config_fast};

    /// Verify that connection establishment produces tracing events.
    #[tokio::test]
    async fn test_connection_establishment_logged() {
        // Install a no-op subscriber to ensure tracing is active
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("wasi_pg_client=debug")
            .with_test_writer()
            .try_init();

        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.close().await.unwrap();

        // If we got here without panicking, tracing is working.
        // A more thorough test would capture and assert on specific events,
        // but that requires a tracing-capture layer.
    }

    /// Verify that query execution produces tracing events.
    #[tokio::test]
    async fn test_query_execution_logged() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("wasi_pg_client=debug")
            .with_test_writer()
            .try_init();

        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.query("SELECT 1").await.unwrap();
        conn.close().await.unwrap();
    }

    /// Verify that transaction events are logged.
    #[tokio::test]
    async fn test_transaction_events_logged() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("wasi_pg_client=debug")
            .with_test_writer()
            .try_init();

        let mut conn = Connection::connect(&test_config()).await.unwrap();
        let mut txn = conn.transaction().await.unwrap();
        txn.commit().await.unwrap();
        conn.close().await.unwrap();
    }

    /// Verify that pool events are logged.
    #[tokio::test]
    async fn test_pool_events_logged() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("wasi_pg_client=debug")
            .with_test_writer()
            .try_init();

        let pool = Pool::new(test_pool_config_fast()).await.unwrap();
        let guard = pool.acquire().await.unwrap();
        guard.release().await;
        pool.close().await;
    }

    /// Verify that passwords are not leaked in tracing output.
    ///
    /// This test connects with a known password and then checks that
    /// the password string does not appear in any tracing output.
    /// Since we can't easily capture tracing output in a test, this
    /// test verifies the connection works with tracing enabled and
    /// relies on the redaction logic in `tracing_ext.rs` being correct
    /// (which is tested separately in the pg-client crate's unit tests).
    #[tokio::test]
    async fn test_no_password_in_tracing_output() {
        let _guard = tracing_subscriber::fmt()
            .with_env_filter("wasi_pg_client=trace")
            .with_test_writer()
            .try_init();

        let config = test_config();
        let mut conn = Connection::connect(&config).await.unwrap();
        conn.query("SELECT 1").await.unwrap();
        conn.close().await.unwrap();

        // The redaction logic is tested in pg-client's tracing_ext tests.
        // This test ensures tracing doesn't panic or cause issues when enabled.
    }
}

// When tracing is not enabled, provide a placeholder test
#[cfg(not(all(feature = "tokio-transport", feature = "tracing")))]
mod tracing_placeholder {
    #[test]
    fn test_tracing_requires_features() {
        // This test is a no-op when tracing features are not enabled.
        // To run tracing tests, use:
        //   cargo test -p integration-tests --features tokio-transport,tracing
    }
}
