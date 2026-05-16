//! End-to-end example for `wasi-pg-client` on WASI Preview 2 or native test runs.
//!
//! This example exercises a representative slice of the PostgreSQL client API surface:
//! 1. Connect and simple query (`SELECT 1`)
//! 2. Parameterized query (`SELECT $1::text || ' ' || $2::text`)
//! 3. Transaction (CREATE TEMP TABLE, INSERT, COMMIT, verify count)
//! 4. Streaming (generate_series(1, 100))
//! 5. COPY IN (CSV data)
//! 6. Prepared statement (prepare, query_prepared, close_statement)
//! 7. Close connection
//!
//! Run it as a WASI P2 component or natively with the appropriate transport feature enabled.
//! It is intended as an API demonstration and smoke-style verification tool rather than a
//! minimal application example.

use wasi_pg_client::types::ToSql;
use wasi_pg_client::{Config, Connection, PreparedStatement};

/// Read the test database URL from the environment, with a local-development default.
fn database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string())
}

#[wstd::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install a tracing subscriber if the `tracing` feature is enabled on wasi-pg-client.
    #[cfg(feature = "tracing")]
    {
        use tracing_subscriber::{fmt, EnvFilter};
        fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("wasi_pg_client=info")),
            )
            .with_writer(std::io::stderr)
            .init();
    }

    let url = database_url();
    eprintln!("[e2e] Connecting using TEST_DATABASE_URL={url}");

    let config = Config::from_uri(&url)?;
    let mut conn = Connection::connect(&config).await?;
    eprintln!("[e2e] Connected!");

    // ── 1. Simple query: SELECT 1 ──────────────────────────────────────────
    eprintln!("[e2e] Test 1: Simple query (SELECT 1)");
    {
        let result = conn.query("SELECT 1 AS val").await?;
        assert_eq!(result.len(), 1, "expected exactly one row");
        let val: i32 = result.rows()[0].get(0)?;
        assert_eq!(val, 1, "expected val = 1");
        eprintln!("[e2e]   ✓ SELECT 1 => {val}");
    }

    // ── 2. Parameterized query ─────────────────────────────────────────────
    eprintln!("[e2e] Test 2: Parameterized query");
    {
        let result = conn
            .query_params(
                "SELECT $1::text || ' ' || $2::text AS greeting",
                &[&"hello" as &dyn ToSql, &"world"],
            )
            .await?;
        assert_eq!(result.len(), 1);
        let greeting: String = result.rows()[0].get(0)?;
        assert_eq!(greeting, "hello world", "expected greeting = 'hello world'");
        eprintln!("[e2e]   ✓ Parameterized query => '{greeting}'");
    }

    // ── 3. Transaction ─────────────────────────────────────────────────────
    eprintln!("[e2e] Test 3: Transaction");
    {
        let count: i64 = conn
            .with_transaction(async |txn| {
                txn.execute("CREATE TEMP TABLE e2e_test_items (id INT, label TEXT)")
                    .await?;
                txn.execute_params(
                    "INSERT INTO e2e_test_items (id, label) VALUES ($1, $2)",
                    &[&1i32 as &dyn ToSql, &"alpha"],
                )
                .await?;
                txn.execute_params(
                    "INSERT INTO e2e_test_items (id, label) VALUES ($1, $2)",
                    &[&2i32 as &dyn ToSql, &"beta"],
                )
                .await?;

                let result = txn.query("SELECT COUNT(*) FROM e2e_test_items").await?;
                let count: i64 = result.rows()[0].get(0)?;
                Ok::<i64, wasi_pg_client::PgError>(count)
            })
            .await?;

        assert_eq!(count, 2, "expected 2 rows after transaction commit");
        eprintln!("[e2e]   ✓ Transaction: inserted and counted {count} rows");

        // Verify data persists after commit (temp table is session-scoped)
        let result = conn.query("SELECT COUNT(*) FROM e2e_test_items").await?;
        let count_after: i64 = result.rows()[0].get(0)?;
        assert_eq!(count_after, 2, "expected 2 rows after commit");
        eprintln!("[e2e]   ✓ Data persists after commit: {count_after} rows");
    }

    // ── 4. Streaming ───────────────────────────────────────────────────────
    eprintln!("[e2e] Test 4: Streaming (generate_series(1, 100))");
    {
        let mut stream = conn
            .query_stream("SELECT generate_series(1, 100) AS n")
            .await?;
        let mut count = 0u32;
        let mut sum: i64 = 0;
        while let Some(row) = stream.next().await? {
            let n: i32 = row.get(0)?;
            count += 1;
            sum += n as i64;
        }
        assert_eq!(count, 100, "expected 100 rows from generate_series");
        assert_eq!(sum, 5050, "expected sum = 5050 (1+2+...+100)");
        eprintln!("[e2e]   ✓ Streamed {count} rows, sum = {sum}");
    }

    // ── 5. COPY IN ─────────────────────────────────────────────────────────
    eprintln!("[e2e] Test 5: COPY IN (CSV data)");
    {
        // Create a temp table for COPY
        conn.execute("CREATE TEMP TABLE e2e_copy_test (id INT, name TEXT)")
            .await?;

        let mut copy = conn
            .copy_in("COPY e2e_copy_test (id, name) FROM STDIN WITH (FORMAT csv)")
            .await?;

        copy.write_csv_row(&["1", "alice"], ',', '"').await?;
        copy.write_csv_row(&["2", "bob"], ',', '"').await?;
        copy.write_csv_row(&["3", "charlie"], ',', '"').await?;

        let rows_copied = copy.finish().await?;
        assert_eq!(rows_copied, 3, "expected 3 rows copied");
        eprintln!("[e2e]   ✓ COPY IN: {rows_copied} rows");

        // Verify the data
        let result = conn
            .query("SELECT id, name FROM e2e_copy_test ORDER BY id")
            .await?;
        assert_eq!(result.len(), 3);
        let id: i32 = result.rows()[0].get(0)?;
        let name: String = result.rows()[0].get(1)?;
        assert_eq!(id, 1);
        assert_eq!(name, "alice");
        eprintln!("[e2e]   ✓ COPY IN data verified: id={id}, name='{name}'");
    }

    // ── 6. Prepared statement ──────────────────────────────────────────────
    eprintln!("[e2e] Test 6: Prepared statement");
    {
        let stmt: PreparedStatement = conn.prepare("SELECT $1::int + $2::int AS sum").await?;
        eprintln!(
            "[e2e]   Prepared statement: name='{}', params={:?}",
            stmt.name(),
            stmt.param_types()
        );

        let result = conn
            .query_prepared(&stmt, &[&10i32 as &dyn ToSql, &20])
            .await?;
        assert_eq!(result.len(), 1);
        let sum: i32 = result.rows()[0].get(0)?;
        assert_eq!(sum, 30, "expected 10 + 20 = 30");
        eprintln!("[e2e]   ✓ query_prepared: 10 + 20 = {sum}");

        // Execute again with different params
        let result2 = conn
            .query_prepared(&stmt, &[&100i32 as &dyn ToSql, &200])
            .await?;
        let sum2: i32 = result2.rows()[0].get(0)?;
        assert_eq!(sum2, 300, "expected 100 + 200 = 300");
        eprintln!("[e2e]   ✓ query_prepared (2nd): 100 + 200 = {sum2}");

        conn.close_statement(&stmt).await?;
        eprintln!("[e2e]   ✓ Statement closed");
    }

    // ── 7. Close connection ────────────────────────────────────────────────
    eprintln!("[e2e] Test 7: Close connection");
    {
        conn.close().await?;
        eprintln!("[e2e]   ✓ Connection closed");
    }

    eprintln!("[e2e] All E2E tests passed!");
    Ok(())
}
