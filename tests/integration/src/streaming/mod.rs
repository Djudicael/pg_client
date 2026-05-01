//! Streaming tests: verify memory-efficient row processing, early termination,
//! connection recovery, and cursor-based batch fetching.
//!
//! These tests require the `tokio-transport` feature and a running PostgreSQL instance.

#[cfg(feature = "tokio-transport")]
mod integration {
    use wasi_pg_client::Connection;

    use crate::common::test_config;

    /// Verify that streaming large results works correctly.
    #[tokio::test]
    async fn test_streaming_large_result() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();

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
        let mut conn = Connection::connect(&test_config()).await.unwrap();

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
        let mut conn = Connection::connect(&test_config()).await.unwrap();

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
        let mut conn = Connection::connect(&test_config()).await.unwrap();

        let tag = conn
            .query_each_async("SELECT generate_series(1, 50)", |_row| async {
                Ok::<(), wasi_pg_client::PgError>(())
            })
            .await
            .unwrap();

        assert_eq!(tag.rows_affected(), Some(50));
        conn.close().await.unwrap();
    }

    // ========================================================================
    // Cursor streaming tests (batch fetching with fetch_size)
    // ========================================================================

    /// Verify that cursor streaming works with batch fetching.
    #[tokio::test]
    async fn test_cursor_streaming() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();

        let count = {
            let mut cursor = conn
                .query_cursor_stream("SELECT generate_series(1, 5000)", &[], 500)
                .await
                .unwrap();

            let mut count = 0;
            while let Some(_row) = cursor.next().await.unwrap() {
                count += 1;
            }
            assert!(cursor.is_done());
            count
        }; // cursor dropped here

        assert_eq!(count, 5000);
        conn.close().await.unwrap();
    }

    /// Verify that cursor streaming with a small fetch size works.
    #[tokio::test]
    async fn test_cursor_streaming_small_fetch_size() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();

        let count = {
            let mut cursor = conn
                .query_cursor_stream("SELECT generate_series(1, 100)", &[], 10)
                .await
                .unwrap();

            let mut count = 0;
            while let Some(_row) = cursor.next().await.unwrap() {
                count += 1;
            }
            assert!(cursor.is_done());
            count
        };

        assert_eq!(count, 100);
        conn.close().await.unwrap();
    }

    /// Verify that cursor streaming with parameterized queries works.
    #[tokio::test]
    async fn test_cursor_streaming_with_params() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();

        let count = {
            let mut cursor = conn
                .query_cursor_stream("SELECT generate_series(1, $1::int)", &[&50i32], 25)
                .await
                .unwrap();

            let mut count = 0;
            while let Some(_row) = cursor.next().await.unwrap() {
                count += 1;
            }
            assert!(cursor.is_done());
            count
        };

        assert_eq!(count, 50);
        conn.close().await.unwrap();
    }

    /// Verify that cursor consume() discards remaining rows and closes the portal.
    #[tokio::test]
    async fn test_cursor_streaming_consume() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();

        {
            let mut cursor = conn
                .query_cursor_stream("SELECT generate_series(1, 1000)", &[], 100)
                .await
                .unwrap();

            // Read a few rows
            for _ in 0..5 {
                let _ = cursor.next().await.unwrap();
            }

            // Consume the rest (discard remaining rows)
            let tag = cursor.consume().await.unwrap();
            // The command tag from a cursor Execute contains the total row count.
            // What matters is that all rows were consumed and the connection is clean.
            assert!(tag.as_str().starts_with("SELECT"));
        }

        // Connection should be usable again (transaction committed by consume)
        let result = conn.query("SELECT 1").await.unwrap();
        assert_eq!(result.len(), 1);
        conn.close().await.unwrap();
    }

    /// Verify that the Cursor (manual fetch) API works.
    #[tokio::test]
    async fn test_cursor_manual_fetch() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();

        {
            let mut cursor = conn
                .query_cursor("SELECT generate_series(1, 100)", &[], 25)
                .await
                .unwrap();

            let mut total_rows = 0;
            while !cursor.is_done() {
                let rows = cursor.fetch_next().await.unwrap();
                total_rows += rows.len();
            }
            assert_eq!(total_rows, 100);

            cursor.close().await.unwrap();
        }

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
