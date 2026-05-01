//! End-to-end connection pool tests using a real PostgreSQL container.
//!
//! These tests require a container runtime (Podman or Docker). They follow
//! the same pattern as `crates/pg-client/tests/e2e_tls.rs`.
//!
//! Run explicitly with:
//!   cargo test -p wasi-pg-pool --features wasi-pg-client/tokio-transport --test e2e_pool -- --ignored

use std::env;
use std::time::Duration;

use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::sync::OnceCell;

use wasi_pg_client::Config;
use wasi_pg_pool::{Pool, PoolConfig};

// ---------------------------------------------------------------------------
// Container infrastructure (shared with pg-client e2e tests pattern)
// ---------------------------------------------------------------------------

struct SharedContainer {
    host: String,
    port: u16,
    #[allow(dead_code)]
    container_id: String,
}

static CONTAINER: OnceCell<SharedContainer> = OnceCell::const_new();

async fn get_container() -> &'static SharedContainer {
    CONTAINER
        .get_or_init(|| async {
            ensure_container_runtime();
            let container = start_postgres().await;
            let host = container.get_host().await.expect("get host").to_string();
            let port = get_mapped_host_port(container.id(), "5432/tcp")
                .await
                .expect("get mapped host port");
            let id = container.id().to_string();
            std::mem::forget(container);
            SharedContainer {
                host,
                port,
                container_id: id,
            }
        })
        .await
}

fn make_connection_config(container: &SharedContainer) -> Config {
    Config::new()
        .host(&container.host)
        .port(container.port)
        .user("postgres")
        .password("postgres")
        .database("test")
        .use_tls(false)
}

fn make_pool_config(container: &SharedContainer) -> PoolConfig {
    PoolConfig::default()
        .connection(make_connection_config(container))
        .max_size(5)
        .min_idle(0)
        .test_on_acquire(false) // skip ping for speed
        .acquire_timeout(Some(Duration::from_secs(5)))
        .max_lifetime(None) // don't expire during tests
        .idle_timeout(None)
}

// ---------------------------------------------------------------------------
// Container helpers (same pattern as pg-client e2e_tls.rs)
// ---------------------------------------------------------------------------

fn ensure_container_runtime() {
    if env::var("DOCKER_HOST").is_ok() || env::var("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE").is_ok()
    {
        return;
    }

    let cli = runtime_cli();

    // Try to detect Podman socket in WSL
    if cli == "podman" {
        let candidates = [
            "/run/user/1000/podman/podman.sock",
            "/run/user/1001/podman/podman.sock",
            "/var/run/docker.sock",
        ];

        for sock in &candidates {
            if std::path::Path::new(sock).exists() {
                env::set_var("DOCKER_HOST", format!("unix://{}", sock));
                return;
            }
        }

        // Try starting podman.socket via systemd
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "start", "podman.socket"])
            .output();

        for sock in &candidates {
            if std::path::Path::new(sock).exists() {
                env::set_var("DOCKER_HOST", format!("unix://{}", sock));
                return;
            }
        }
    }
}

fn runtime_cli() -> &'static str {
    if env::var("DOCKER_HOST")
        .unwrap_or_default()
        .contains("podman")
    {
        "podman"
    } else {
        "docker"
    }
}

async fn get_mapped_host_port(container_id: &str, container_port: &str) -> Option<u16> {
    let cli = runtime_cli();
    let format_str = format!(
        "{{{{ (index (index .NetworkSettings.Ports \"{}\") 0).HostPort }}}}",
        container_port
    );

    let output = tokio::process::Command::new(cli)
        .args(["inspect", container_id, "--format", &format_str])
        .output()
        .await
        .ok()?;

    if !output.status.success() {
        return None;
    }

    String::from_utf8(output.stdout)
        .ok()
        .and_then(|s| s.trim().parse::<u16>().ok())
}

async fn start_postgres() -> testcontainers::ContainerAsync<GenericImage> {
    let image = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_wait_for(WaitFor::seconds(3))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "test")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_mapped_port(0, 5432.tcp());

    image
        .start()
        .await
        .expect("failed to start PostgreSQL container")
}

// Helper: acquire and immediately release via async release()
async fn acquire_and_release(pool: &Pool) {
    let guard = pool.acquire().await.expect("acquire should succeed");
    guard.release().await;
}

// Helper: acquire, run a query, then release
async fn acquire_query_and_release(pool: &Pool, sql: &str) {
    let mut guard = pool.acquire().await.expect("acquire should succeed");
    guard.query(sql).await.expect("query should succeed");
    guard.release().await;
}

/// Assert that a pool acquire result is a Pool error containing the given substring.
fn assert_pool_error(
    result: Result<wasi_pg_pool::PoolGuard<'_>, wasi_pg_client::PgError>,
    expected_substring: &str,
) {
    match result {
        Err(wasi_pg_client::PgError::Pool(msg)) => {
            assert!(
                msg.contains(expected_substring),
                "expected error containing '{}', got: {}",
                expected_substring,
                msg
            );
        }
        Err(other) => panic!("expected Pool error, got: {:?}", other),
        Ok(_) => panic!("expected error, but acquire succeeded"),
    }
}

// ===========================================================================
// E2E Pool tests
// ===========================================================================

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_basic_acquire_release() {
    let container = get_container().await;
    let pool_config = make_pool_config(container);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire and release via async release()
    let mut guard = pool.acquire().await.expect("first acquire should succeed");
    guard.query("SELECT 1").await.expect("query should succeed");
    guard.release().await;

    // Status should show the connection returned to idle
    let status = pool.status();
    assert_eq!(status.idle, 1);
    assert_eq!(status.active, 0);

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_connection_reuse() {
    let container = get_container().await;
    let pool_config = make_pool_config(container);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire and release multiple times — should reuse the same connection
    for i in 0..5 {
        let mut guard = pool.acquire().await.expect("acquire should succeed");
        guard.query("SELECT 1").await.expect("query should succeed");
        guard.release().await;
        eprintln!("[e2e] Iteration {} done", i);
    }

    // Only 1 connection should have been created total
    let status = pool.status();
    assert_eq!(
        status.total_created, 1,
        "should have created only 1 connection"
    );
    assert_eq!(status.idle, 1);

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_max_size_enforced() {
    let container = get_container().await;
    let pool_config = make_pool_config(container)
        .max_size(2)
        .acquire_timeout(None); // no timeout — should fail immediately
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire 2 connections (max_size)
    let _g1 = pool.acquire().await.expect("first acquire should succeed");
    let _g2 = pool.acquire().await.expect("second acquire should succeed");

    // Third acquire should fail (pool exhausted)
    assert_pool_error(pool.acquire().await, "exhausted");
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_status_while_guards_alive() {
    let container = get_container().await;
    let pool_config = make_pool_config(container).max_size(5);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire two connections
    let _guard1 = pool.acquire().await.expect("first acquire");
    let _guard2 = pool.acquire().await.expect("second acquire");

    // Status should be callable while guards are alive
    let status = pool.status();
    assert_eq!(status.active, 2);
    assert_eq!(status.idle, 0);
    assert_eq!(status.total(), 2);
    assert_eq!(status.available(), 3);
    assert!(!status.closed);
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_close_discards_idle() {
    let container = get_container().await;
    let pool_config = make_pool_config(container);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Create a connection and return it to idle
    acquire_and_release(&pool).await;

    let status = pool.status();
    assert_eq!(status.idle, 1);

    // Close the pool
    pool.close().await;

    // Idle connections should be discarded
    let status = pool.status();
    assert_eq!(status.idle, 0);
    assert!(status.closed);

    // New acquisitions should fail
    assert_pool_error(pool.acquire().await, "closed");
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_detach() {
    let container = get_container().await;
    let pool_config = make_pool_config(container);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire and detach
    let guard = pool.acquire().await.expect("acquire should succeed");
    let mut conn = guard.detach();
    assert!(!conn.is_closed());

    // Active count should be decremented
    let status = pool.status();
    assert_eq!(status.active, 0);
    assert_eq!(status.idle, 0);

    // Clean up the detached connection
    conn.close().await.expect("close should succeed");
    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_after_connect_hook() {
    let container = get_container().await;
    let pool_config = make_pool_config(container).after_connect("SET timezone = 'UTC'");
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    let mut guard = pool.acquire().await.expect("acquire should succeed");
    let result = guard
        .query("SHOW timezone")
        .await
        .expect("query should succeed");

    // The timezone should be UTC (set by after_connect hook)
    let row = result
        .into_rows()
        .into_iter()
        .next()
        .expect("should have a row");
    let tz: String = row.get(0).expect("should get timezone value");
    assert_eq!(
        tz, "UTC",
        "after_connect hook should have set timezone to UTC"
    );

    guard.release().await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_before_return_hook() {
    let container = get_container().await;
    let pool_config = make_pool_config(container).before_return("RESET ALL");
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Set a session variable
    {
        let mut guard = pool.acquire().await.expect("acquire should succeed");
        guard
            .execute("SET timezone = 'Asia/Tokyo'")
            .await
            .expect("set timezone");
        // Release should run before_return hook (RESET ALL)
        guard.release().await;
    }

    // Acquire again — timezone should be reset
    {
        let mut guard = pool.acquire().await.expect("acquire should succeed");
        let result = guard
            .query("SHOW timezone")
            .await
            .expect("query should succeed");
        let row = result
            .into_rows()
            .into_iter()
            .next()
            .expect("should have a row");
        let tz: String = row.get(0).expect("should get timezone value");
        // RESET ALL resets timezone to default (not Asia/Tokyo)
        assert_ne!(
            tz, "Asia/Tokyo",
            "before_return hook should have reset session state"
        );
        guard.release().await;
    }

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_transaction_cleanup_on_release() {
    let container = get_container().await;
    let pool_config = make_pool_config(container);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Start a transaction and release without committing
    {
        let mut guard = pool.acquire().await.expect("acquire should succeed");
        guard.execute("BEGIN").await.expect("BEGIN");
        guard
            .execute("CREATE TEMP TABLE test_cleanup (id int)")
            .await
            .expect("CREATE TEMP TABLE");
        // Release should ROLLBACK the transaction
        guard.release().await;
    }

    // Acquire again — should not be in a transaction
    {
        let mut guard = pool.acquire().await.expect("acquire should succeed");
        let status = guard.transaction_status();
        assert_eq!(
            status,
            pg_protocol::TransactionStatus::Idle,
            "transaction should have been rolled back on release"
        );
        guard.release().await;
    }

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_drop_based_return() {
    let container = get_container().await;
    let pool_config = make_pool_config(container);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire and drop (without calling release)
    {
        let mut guard = pool.acquire().await.expect("acquire should succeed");
        guard.execute("BEGIN").await.expect("BEGIN");
        // Drop without release — connection goes back to pool without async cleanup
        drop(guard);
    }

    // The connection should be back in the idle queue (Drop pushes it back)
    let status = pool.status();
    assert_eq!(
        status.idle, 1,
        "connection should be returned to idle on drop"
    );

    // Next acquire should work (may need cleanup, but the connection is usable)
    {
        let mut guard = pool
            .acquire()
            .await
            .expect("acquire after drop should succeed");
        // The dirty transaction state should be cleaned up by the pool
        guard.query("SELECT 1").await.expect("query should succeed");
        guard.release().await;
    }

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_min_idle_pre_warm() {
    let container = get_container().await;
    let pool_config = make_pool_config(container).min_idle(3).max_size(10);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Pool should have pre-warmed min_idle connections
    let status = pool.status();
    assert!(
        status.idle >= 3,
        "expected at least 3 idle connections after pre-warming, got {}",
        status.idle
    );
    assert!(status.total_created >= 3);

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_maintain_discards_expired() {
    let container = get_container().await;
    let pool_config = make_pool_config(container)
        .idle_timeout(Some(Duration::from_millis(100))) // very short idle timeout
        .max_lifetime(None);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Create a connection and return it to idle
    acquire_and_release(&pool).await;
    assert_eq!(pool.status().idle, 1);

    // Wait for the idle timeout to elapse
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Maintain should discard the expired connection
    pool.maintain().await;
    assert_eq!(
        pool.status().idle,
        0,
        "expired idle connection should be discarded"
    );

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_created_at_preserved() {
    let container = get_container().await;
    let pool_config = make_pool_config(container)
        .max_lifetime(None)
        .idle_timeout(None);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire and release — the connection should be reused
    {
        let mut guard = pool.acquire().await.expect("acquire should succeed");
        guard.query("SELECT 1").await.expect("query");
        guard.release().await;
    }

    {
        let mut guard = pool.acquire().await.expect("acquire should succeed");
        guard.query("SELECT 2").await.expect("query");
        guard.release().await;
    }

    // Only 1 connection should have been created (created_at preserved across cycles)
    let status = pool.status();
    assert_eq!(
        status.total_created, 1,
        "connection should be reused, not recreated"
    );

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_multiple_guards_coexist() {
    let container = get_container().await;
    let pool_config = make_pool_config(container).max_size(5);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire multiple guards simultaneously
    let guard1 = pool.acquire().await.expect("acquire 1");
    let guard2 = pool.acquire().await.expect("acquire 2");
    let guard3 = pool.acquire().await.expect("acquire 3");

    // All should be active
    let status = pool.status();
    assert_eq!(status.active, 3);

    // Can still call status() while guards are alive
    assert!(pool.status().available() >= 2);

    // Release them
    guard1.release().await;
    guard2.release().await;
    guard3.release().await;

    let status = pool.status();
    assert_eq!(status.active, 0);
    assert_eq!(status.idle, 3);

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_health_check_on_acquire() {
    let container = get_container().await;
    let pool_config = make_pool_config(container)
        .test_on_acquire(true) // enable health check
        .max_lifetime(None)
        .idle_timeout(None);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire and release — connection goes to idle
    acquire_query_and_release(&pool, "SELECT 1").await;

    // Acquire again — should pass health check (ping)
    acquire_query_and_release(&pool, "SELECT 2").await;

    // Should still have only 1 connection created
    assert_eq!(pool.status().total_created, 1);

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_is_closed() {
    let container = get_container().await;
    let pool_config = make_pool_config(container);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    assert!(!pool.is_closed());
    pool.close().await;
    assert!(pool.is_closed());
}
