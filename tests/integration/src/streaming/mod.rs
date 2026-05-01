//! Streaming tests: verify memory-efficient row processing, early termination,
//! and connection recovery.
//!
//! These tests require the `tokio-transport` feature and a running PostgreSQL instance.

#[cfg(feature = "tokio-transport")]
mod integration {
    use wasi_pg_client::Connection;

    use crate::common::test_config;

    /// Verify that streaming large results works correctly.
    #[tokio::test]
    async fn test_streaming_large_result() {
        let mut conn = Connection::connect(test_config()).await.unwrap();

        // Scope the stream so conn is released after
        let count = {
            let mut stream = conn
                .query_stream("SELECT generate_series(1, 10000)")
                .await
                .unwrap();

            let mut count = 0;
            while let Some(_row) = stream.next().await.unwrap() {
                count += 1;
            }
            count
        }; // stream dropped here

        assert_eq!(count, 10000);
        conn.close().await.unwrap();
    }

    /// Verify that early termination of a stream marks the connection
    /// as needing recovery.
    #[tokio::test]
    async fn test_streaming_early_termination() {
        let mut conn = Connection::connect(test_config()).await.unwrap();

        // Read only 5 rows from a stream, then drop it
        {
            let mut stream = conn
                .query_stream("SELECT generate_series(1, 10000)")
                .await
                .unwrap();

            for _ in 0..5 {
                let _ = stream.next().await.unwrap();
            }
            // Drop the stream (incomplete consumption)
            // RowStream's Drop impl should mark the connection as needing recovery
        }

        // Connection should need recovery
        assert!(conn.needs_recovery());
        conn.recover().await.unwrap();
        assert!(!conn.needs_recovery());

        // Connection should be usable again
        let result = conn.query("SELECT 1").await.unwrap();
        assert_eq!(result.len(), 1);
        conn.close().await.unwrap();
    }

    /// Verify that query_params_stream works correctly.
    #[tokio::test]
    async fn test_query_params_stream() {
        let mut conn = Connection::connect(test_config()).await.unwrap();

        let count = {
            let mut stream = conn
                .query_params_stream("SELECT generate_series(1, $1::int)", &[&100i32])
                .await
                .unwrap();

            let mut count = 0;
            while let Some(_row) = stream.next().await.unwrap() {
                count += 1;
            }
            count
        }; // stream dropped here

        assert_eq!(count, 100);
        conn.close().await.unwrap();
    }

    /// Verify that query_each_async works for row-by-row processing.
    #[tokio::test]
    async fn test_query_each_async() {
        let mut conn = Connection::connect(test_config()).await.unwrap();

        let tag = conn
            .query_each_async("SELECT generate_series(1, 50)", |_row| async {
                Ok::<(), wasi_pg_client::PgError>(())
            })
            .await
            .unwrap();

        assert_eq!(tag.rows_affected(), Some(50));
        conn.close().await.unwrap();
    }
}

// Placeholder when tokio-transport is not enabled
#[cfg(not(feature = "tokio-transport"))]
mod streaming_placeholder {
    #[test]
    fn test_streaming_requires_tokio_transport() {
        // This test is a no-op when tokio-transport is not enabled.
        // To run streaming tests, use:
        //   cargo test -p integration-tests --features tokio-transport
    }
}
