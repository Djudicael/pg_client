//! End-to-end connection pool tests using a real PostgreSQL container.
//!
//! These tests require a container runtime (Podman or Docker). They follow
//! the same pattern as `crates/pg-client/tests/e2e_tls.rs`.
//!
//! Run explicitly with:
//!   cargo test -p wasi-pg-client --features pool,tokio-transport --test e2e_pool -- --ignored

use std::env;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;

use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::sync::OnceCell;

use wasi_pg_client::pool::{Pool, PoolConfig};
use wasi_pg_client::{Config, Connection, PgError, ReconnectConfig};

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

fn make_reconnecting_connection_config(container: &SharedContainer) -> Config {
    make_connection_config(container).reconnect(
        ReconnectConfig::enabled()
            .max_attempts(3)
            .initial_delay(Duration::from_millis(50))
            .max_delay(Duration::from_millis(250)),
    )
}

fn make_reconnecting_pool_config(container: &SharedContainer) -> PoolConfig {
    PoolConfig::default()
        .connection(make_reconnecting_connection_config(container))
        .max_size(5)
        .min_idle(0)
        .test_on_acquire(false)
        .acquire_timeout(Some(Duration::from_secs(5)))
        .max_lifetime(None)
        .idle_timeout(None)
}

// ---------------------------------------------------------------------------
// Container helpers (same pattern as pg-client e2e_tls.rs)
// ---------------------------------------------------------------------------

fn maybe_start_podman_socket(socket_hint: &str) {
    if socket_hint.contains("podman.sock") {
        let _ = Command::new("systemctl")
            .args(["--user", "start", "podman.socket"])
            .output();
        thread::sleep(Duration::from_millis(800));
    }
}

fn ensure_container_runtime() -> bool {
    if let Ok(host) = env::var("DOCKER_HOST") {
        maybe_start_podman_socket(&host);
        return true;
    }

    if env::var("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE").is_ok() {
        return true;
    }

    let candidates = [
        "/run/user/1000/podman/podman.sock",
        "/run/user/1001/podman/podman.sock",
        "/var/run/docker.sock",
    ];

    for sock in &candidates {
        if Path::new(sock).exists() {
            maybe_start_podman_socket(sock);
            env::set_var("DOCKER_HOST", format!("unix://{}", sock));
            eprintln!("[e2e] Using container runtime socket: {}", sock);
            return true;
        }
    }

    eprintln!("[e2e] No container socket found; trying to start podman.socket ...");
    let _ = Command::new("systemctl")
        .args(["--user", "start", "podman.socket"])
        .output();

    thread::sleep(Duration::from_millis(800));

    for sock in &candidates {
        if Path::new(sock).exists() {
            env::set_var("DOCKER_HOST", format!("unix://{}", sock));
            eprintln!("[e2e] Started and using podman.socket: {}", sock);
            return true;
        }
    }

    eprintln!(
        "[e2e] ERROR: No container runtime socket available.\n\
         Please start Podman/Docker or run:\n\
         systemctl --user start podman.socket"
    );
    false
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

async fn query_timezone(conn: &mut Connection) -> String {
    let row = conn
        .query_one("SHOW timezone")
        .await
        .expect("SHOW timezone should succeed")
        .expect("SHOW timezone should return one row");
    row.get(0).expect("timezone should decode as String")
}

async fn query_one_i32_with_retry(
    conn: &mut Connection,
    sql: &'static str,
) -> Result<i32, PgError> {
    let conn_ptr = conn as *mut Connection;
    conn.with_retry(|_| async move {
        let conn = unsafe { &mut *conn_ptr };
        let row = conn
            .query_one(sql)
            .await?
            .expect("query should return one row");
        row.get(0)
    })
    .await
}

async fn terminate_backend(container: &SharedContainer, pid: i32) {
    let mut admin = Connection::connect(&make_connection_config(container))
        .await
        .expect("admin connection should succeed");
    let row = admin
        .query_one(&format!("SELECT pg_terminate_backend({pid})"))
        .await
        .expect("pg_terminate_backend query should succeed")
        .expect("pg_terminate_backend should return one row");
    let terminated: bool = row.get(0).expect("terminate result should decode as bool");
    assert!(terminated, "pg_terminate_backend should return true");
    admin.close().await.expect("admin close should succeed");
}

/// Assert that a pool acquire result is a Pool error containing the given substring.
fn assert_pool_error(
    result: Result<wasi_pg_client::pool::PoolGuard<'_>, wasi_pg_client::PgError>,
    expected_substring: &str,
) {
    match result {
        Err(wasi_pg_client::PgError::Pool(err)) => {
            let msg = err.to_string();
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
async fn test_pool_close_discards_active_guard_on_drop() {
    let container = get_container().await;
    let pool_config = make_pool_config(container);
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    let guard = pool.acquire().await.expect("acquire should succeed");
    assert_eq!(pool.status().active, 1);

    pool.close().await;
    assert!(pool.is_closed());

    drop(guard);

    let status = pool.status();
    assert_eq!(
        status.active, 0,
        "active guard should be discarded on drop after close"
    );
    assert_eq!(
        status.idle, 0,
        "closed pool must not retain dropped active connections"
    );

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
    let tz = query_timezone(&mut guard).await;
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
        let guard = pool.acquire().await.expect("acquire should succeed");
        let status = guard.transaction_status();
        assert_eq!(
            status,
            wasi_pg_client::TransactionStatus::Idle,
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

// ===========================================================================
// Reconnection E2E tests
// ===========================================================================

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_reconnect_config_from_uri() {
    let container = get_container().await;
    let config = wasi_pg_client::Config::from_uri(&format!(
        "postgresql://postgres:postgres@{}:{}/test?reconnect=true&reconnect_max_attempts=5&stale_threshold_secs=60",
        container.host, container.port
    ))
    .expect("parse URI");

    assert!(config.get_reconnect().enabled);
    assert_eq!(config.get_reconnect().max_attempts, 5);
    assert_eq!(
        config.get_stale().stale_threshold,
        std::time::Duration::from_secs(60)
    );
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_connection_is_alive() {
    let container = get_container().await;
    let config = make_connection_config(container);
    let mut conn = wasi_pg_client::Connection::connect(&config)
        .await
        .expect("connect");

    // Fresh connection should be alive
    assert!(conn.is_alive());

    // After a successful query, should still be alive
    conn.query("SELECT 1").await.expect("query");
    assert!(conn.is_alive());

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_connection_is_stale() {
    let container = get_container().await;
    let config =
        make_connection_config(container).stale_threshold(std::time::Duration::from_millis(100));
    let mut conn = wasi_pg_client::Connection::connect(&config)
        .await
        .expect("connect");

    // Fresh connection should not be stale with a reasonable threshold
    assert!(!conn.is_stale(std::time::Duration::from_secs(30)));

    // With a very short threshold, it might be stale
    // (depends on timing, so we just check the method works)
    let _ = conn.is_stale(std::time::Duration::from_micros(1));

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_connection_with_retry_transient_error() {
    use wasi_pg_client::{Connection, ReconnectConfig};

    let container = get_container().await;
    let config =
        make_connection_config(container).reconnect(ReconnectConfig::enabled().max_attempts(3));

    let mut conn = Connection::connect(&config).await.expect("connect");

    // Create a table and a function that will cause a serialization failure
    conn.execute("CREATE TEMP TABLE test_retry (id int PRIMARY KEY)")
        .await
        .expect("create table");

    // with_retry should work for normal operations
    let result: i32 = conn
        .with_retry(|c| {
            let c = c as *mut Connection;
            async move {
                // Safety: we're in a single-threaded context
                let c = unsafe { &mut *c };
                let rows = c.query("SELECT 42").await?;
                let val: i32 = rows.into_rows().into_iter().next().unwrap().get(0)?;
                Ok(val)
            }
        })
        .await
        .expect("with_retry should succeed");

    assert_eq!(result, 42);
    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_connection_ensure_alive() {
    let container = get_container().await;
    let config =
        make_connection_config(container).stale_threshold(std::time::Duration::from_secs(30));

    let mut conn = wasi_pg_client::Connection::connect(&config)
        .await
        .expect("connect");

    // ensure_alive should succeed on a healthy connection
    conn.ensure_alive()
        .await
        .expect("ensure_alive should succeed");
    assert!(conn.is_alive());

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_acquire_resilient() {
    let container = get_container().await;
    let pool_config = make_pool_config(container)
        .test_on_acquire(false) // disable normal health check
        .max_size(3);

    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire resiliently — should work
    let mut guard = pool.acquire_resilient().await.expect("acquire_resilient");
    guard.query("SELECT 1").await.expect("query");
    guard.release().await;

    // Should have created 1 connection
    assert_eq!(pool.status().total_created, 1);

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_with_reconnect_enabled() {
    let container = get_container().await;
    let connection_config = make_reconnecting_connection_config(container);

    let pool_config = PoolConfig::default()
        .connection(connection_config)
        .max_size(3)
        .test_on_acquire(false);

    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    // Acquire should work
    let mut guard = pool.acquire().await.expect("acquire");
    guard.query("SELECT 1").await.expect("query");
    guard.release().await;

    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_session_state_tracking() {
    let container = get_container().await;
    let config = make_connection_config(container);
    let mut conn = wasi_pg_client::Connection::connect(&config)
        .await
        .expect("connect");

    // Initially, session state should be empty
    assert!(!conn.session_state().has_state());
    assert!(conn.session_state().is_reconnect_safe());

    // Start a transaction
    conn.execute("BEGIN").await.expect("BEGIN");
    // Note: session_state.in_transaction is updated when ReadyForQuery is received
    // which happens during the execute call

    conn.execute("ROLLBACK").await.expect("ROLLBACK");

    // After rollback, should be safe again
    assert!(conn.session_state().is_reconnect_safe());

    conn.close().await.expect("close");
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_error_classification() {
    use wasi_pg_client::{classify_error, ErrorClass, PgError};

    // Test various error classifications
    assert_eq!(
        classify_error(&PgError::ConnectionClosed),
        ErrorClass::Broken
    );
    assert_eq!(classify_error(&PgError::Timeout), ErrorClass::Transient);

    // Create a server error for testing
    let server_err = wasi_pg_client::PgServerError::from_fields(vec![
        (b'S', "ERROR".to_string()),
        (b'C', "23505".to_string()),
        (b'M', "duplicate key".to_string()),
    ]);
    assert_eq!(
        classify_error(&PgError::Server(Box::new(server_err))),
        ErrorClass::Permanent
    );
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_after_connect_survives_reconnect() {
    let container = get_container().await;
    let pool_config = make_reconnecting_pool_config(container)
        .max_size(1)
        .after_connect("SET timezone = 'UTC'");
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    let mut guard = pool.acquire().await.expect("acquire should succeed");
    assert_eq!(query_timezone(&mut guard).await, "UTC");

    let old_pid = guard.process_id();
    assert!(
        old_pid > 0,
        "backend pid should be positive before reconnect"
    );
    terminate_backend(container, old_pid).await;

    let value = query_one_i32_with_retry(&mut guard, "SELECT 1")
        .await
        .expect("with_retry should reconnect and retry");
    assert_eq!(value, 1);
    assert_ne!(
        guard.process_id(),
        old_pid,
        "backend pid should change after reconnect"
    );
    assert_eq!(
        query_timezone(&mut guard).await,
        "UTC",
        "pool after_connect SQL should be replayed after reconnect"
    );

    guard.release().await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "e2e test: requires podman (or docker)"]
async fn test_pool_after_connect_reconnect_preserves_runtime_override() {
    let container = get_container().await;
    let pool_config = make_reconnecting_pool_config(container)
        .max_size(1)
        .after_connect("SET timezone = 'UTC'");
    let pool = Pool::new(pool_config)
        .await
        .expect("pool creation should succeed");

    let mut guard = pool.acquire().await.expect("acquire should succeed");
    assert_eq!(query_timezone(&mut guard).await, "UTC");

    guard
        .set_param("timezone", "Asia/Tokyo")
        .await
        .expect("set_param should succeed");
    assert_eq!(
        query_timezone(&mut guard).await,
        "Asia/Tokyo",
        "runtime override should take effect before reconnect"
    );

    let old_pid = guard.process_id();
    terminate_backend(container, old_pid).await;

    let value = query_one_i32_with_retry(&mut guard, "SELECT 1")
        .await
        .expect("with_retry should reconnect and retry");
    assert_eq!(value, 1);
    assert_ne!(guard.process_id(), old_pid);
    assert_eq!(
        query_timezone(&mut guard).await,
        "Asia/Tokyo",
        "tracked runtime session state should override pool after_connect after reconnect"
    );

    guard.release().await;
    pool.close().await;
}
