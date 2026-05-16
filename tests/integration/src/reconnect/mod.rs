#![cfg(test)]

//! Reconnection tests: retry policy, backoff, session state, stale detection.
//!
//! These tests exercise the reconnection and retry infrastructure without
//! requiring a real PostgreSQL server (unit-level) or with a real server
//! (integration-level, behind `tokio-transport` feature).

use std::time::Duration;
use wasi_pg_client::reconnect::{
    classify_error, ErrorClass, ReconnectConfig, RetryPolicy, SessionState,
};
use wasi_pg_client::transport::TransportError;
use wasi_pg_client::{PgError, PgServerError};

// ========================================================================
// RetryPolicy tests (pure logic, no I/O)
// ========================================================================

#[test]
fn test_retry_policy_default() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.max_attempts, 3);
    assert_eq!(policy.initial_delay, Duration::from_millis(100));
    assert_eq!(policy.max_delay, Duration::from_secs(10));
}

#[test]
fn test_retry_policy_no_retry() {
    let policy = RetryPolicy::no_retry();
    assert_eq!(policy.max_attempts, 1);
    assert_eq!(policy.initial_delay, Duration::ZERO);
    assert_eq!(policy.max_delay, Duration::ZERO);
}

#[test]
fn test_retry_policy_fixed_delay() {
    let policy = RetryPolicy::fixed_delay(5, Duration::from_millis(500));
    assert_eq!(policy.max_attempts, 5);
    assert_eq!(policy.initial_delay, Duration::from_millis(500));
    assert_eq!(policy.max_delay, Duration::from_millis(500));
}

#[test]
fn test_retry_policy_exponential_backoff() {
    let policy =
        RetryPolicy::exponential_backoff(5, Duration::from_millis(100), Duration::from_secs(10));
    assert_eq!(policy.max_attempts, 5);
    assert_eq!(policy.initial_delay, Duration::from_millis(100));
    assert_eq!(policy.max_delay, Duration::from_secs(10));
}

#[test]
fn test_retry_policy_backoff_calculation() {
    let policy =
        RetryPolicy::exponential_backoff(5, Duration::from_millis(100), Duration::from_secs(10));

    // attempt 1: 100ms * 2^0 = 100ms
    assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(100));
    // attempt 2: 100ms * 2^1 = 200ms
    assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(200));
    // attempt 3: 100ms * 2^2 = 400ms
    assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(400));
    // attempt 4: 100ms * 2^3 = 800ms
    assert_eq!(policy.delay_for_attempt(4), Duration::from_millis(800));
    // attempt 5: 100ms * 2^4 = 1600ms
    assert_eq!(policy.delay_for_attempt(5), Duration::from_millis(1600));
}

#[test]
fn test_retry_policy_capped_at_max() {
    let policy =
        RetryPolicy::exponential_backoff(10, Duration::from_secs(1), Duration::from_secs(5));

    assert_eq!(policy.delay_for_attempt(1), Duration::from_secs(1));
    assert_eq!(policy.delay_for_attempt(2), Duration::from_secs(2));
    assert_eq!(policy.delay_for_attempt(3), Duration::from_secs(4));
    assert_eq!(policy.delay_for_attempt(4), Duration::from_secs(5)); // capped
    assert_eq!(policy.delay_for_attempt(5), Duration::from_secs(5)); // capped
    assert_eq!(policy.delay_for_attempt(10), Duration::from_secs(5)); // still capped
}

// ========================================================================
// ReconnectConfig tests (pure logic, no I/O)
// ========================================================================

#[test]
fn test_reconnect_config_default() {
    let config = ReconnectConfig::default();
    assert!(!config.enabled);
    assert_eq!(config.max_attempts, 3);
    assert_eq!(config.initial_delay, Duration::from_millis(100));
    assert_eq!(config.max_delay, Duration::from_secs(10));
    assert!(config.rebuild_session);
    assert!(!config.allow_mid_transaction);
}

#[test]
fn test_reconnect_config_enabled() {
    let config = ReconnectConfig::enabled();
    assert!(config.enabled);
}

#[test]
fn test_reconnect_config_builder() {
    let config = ReconnectConfig::enabled()
        .max_attempts(5)
        .initial_delay(Duration::from_millis(200))
        .max_delay(Duration::from_secs(30))
        .rebuild_session(false)
        .allow_mid_transaction(true);

    assert!(config.enabled);
    assert_eq!(config.max_attempts, 5);
    assert_eq!(config.initial_delay, Duration::from_millis(200));
    assert_eq!(config.max_delay, Duration::from_secs(30));
    assert!(!config.rebuild_session);
    assert!(config.allow_mid_transaction);
}

#[test]
fn test_reconnect_config_delay_for_attempt() {
    let config = ReconnectConfig::default();

    // attempt 1: 100ms * 2^0 = 100ms
    assert_eq!(config.delay_for_attempt(1), Duration::from_millis(100));
    // attempt 2: 100ms * 2^1 = 200ms
    assert_eq!(config.delay_for_attempt(2), Duration::from_millis(200));
    // attempt 3: 100ms * 2^2 = 400ms
    assert_eq!(config.delay_for_attempt(3), Duration::from_millis(400));
    // attempt 8: 100ms * 2^7 = 12800ms, capped at 10s
    assert_eq!(config.delay_for_attempt(8), Duration::from_secs(10));
}

// ========================================================================
// Error classification tests (pure logic, no I/O)
// ========================================================================

fn make_server_error(code: &str, message: &str) -> PgError {
    PgError::Server(Box::new(PgServerError::from_fields(vec![
        (b'S', "ERROR".to_string()),
        (b'C', code.to_string()),
        (b'M', message.to_string()),
    ])))
}

#[test]
fn test_classify_broken_errors() {
    assert_eq!(
        classify_error(&PgError::ConnectionClosed),
        ErrorClass::Broken
    );
    assert_eq!(
        classify_error(&PgError::Transport(TransportError::ConnectionReset)),
        ErrorClass::Broken
    );
    assert_eq!(
        classify_error(&PgError::Transport(TransportError::UnexpectedEof)),
        ErrorClass::Broken
    );
    assert_eq!(
        classify_error(&PgError::Transport(TransportError::ConnectionRefused)),
        ErrorClass::Broken
    );

    // Connection exception (080xx SQLSTATE class)
    let err = make_server_error("08006", "connection failure");
    assert_eq!(classify_error(&err), ErrorClass::Broken);

    // Admin shutdown
    let err = make_server_error("57P01", "admin shutdown");
    assert_eq!(classify_error(&err), ErrorClass::Broken);

    // Crash shutdown
    let err = make_server_error("57P02", "crash shutdown");
    assert_eq!(classify_error(&err), ErrorClass::Broken);
}

#[test]
fn test_classify_transient_errors() {
    // Serialization failure
    let err = make_server_error("40001", "could not serialize access");
    assert_eq!(classify_error(&err), ErrorClass::Transient);

    // Deadlock detected
    let err = make_server_error("40P01", "deadlock detected");
    assert_eq!(classify_error(&err), ErrorClass::Transient);

    // Transport timeout
    assert_eq!(
        classify_error(&PgError::Transport(TransportError::Timeout)),
        ErrorClass::Transient
    );

    // Generic timeout
    assert_eq!(classify_error(&PgError::Timeout), ErrorClass::Transient);
}

#[test]
fn test_classify_permanent_errors() {
    // Unique violation
    let err = make_server_error("23505", "duplicate key");
    assert_eq!(classify_error(&err), ErrorClass::Permanent);

    // Syntax error
    let err = make_server_error("42601", "syntax error");
    assert_eq!(classify_error(&err), ErrorClass::Permanent);

    // Config error
    assert_eq!(
        classify_error(&PgError::Config("bad config".into())),
        ErrorClass::Permanent
    );

    // Auth error
    assert_eq!(
        classify_error(&PgError::Auth("bad password".into())),
        ErrorClass::Permanent
    );
}

#[test]
fn test_classify_io_errors() {
    let broken = PgError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "reset",
    ));
    assert_eq!(classify_error(&broken), ErrorClass::Broken);

    let broken = PgError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe"));
    assert_eq!(classify_error(&broken), ErrorClass::Broken);

    let transient = PgError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
    assert_eq!(classify_error(&transient), ErrorClass::Transient);

    let permanent = PgError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "bad input",
    ));
    assert_eq!(classify_error(&permanent), ErrorClass::Permanent);
}

// ========================================================================
// SessionState tests (pure logic, no I/O)
// ========================================================================

#[test]
fn test_session_state_empty() {
    let state = SessionState::new();
    assert!(!state.has_state());
    assert!(state.is_reconnect_safe());
}

#[test]
fn test_session_state_with_prepared_statement() {
    let mut state = SessionState::new();
    state.track_prepare("stmt1", "SELECT 1");
    assert!(state.has_state());
    assert!(!state.is_reconnect_safe());
    assert_eq!(state.get_statement_sql("stmt1"), Some("SELECT 1"));
}

#[test]
fn test_session_state_with_listen_channel() {
    let mut state = SessionState::new();
    state.track_listen("events");
    assert!(state.has_state());
    assert!(!state.is_reconnect_safe());
    assert!(state.listen_channels().contains("events"));
}

#[test]
fn test_session_state_with_temp_table() {
    let mut state = SessionState::new();
    state.track_temp_table("tmp_data");
    assert!(state.has_state());
    assert!(!state.is_reconnect_safe());
}

#[test]
fn test_session_state_with_guc() {
    let mut state = SessionState::new();
    state.track_set_guc("timezone", "UTC");
    assert!(state.has_state());
    assert!(!state.is_reconnect_safe());
    assert_eq!(
        state.custom_gucs().get("timezone"),
        Some(&"UTC".to_string())
    );
}

#[test]
fn test_session_state_in_transaction() {
    let mut state = SessionState::new();
    state.set_in_transaction(true);
    assert!(!state.is_reconnect_safe());
    // has_state doesn't count in_transaction
    assert!(!state.has_state());
}

#[test]
fn test_session_state_unlisten() {
    let mut state = SessionState::new();
    state.track_listen("events");
    assert!(state.has_state());
    state.track_unlisten("events");
    assert!(!state.has_state());
}

#[test]
fn test_session_state_close_statement() {
    let mut state = SessionState::new();
    state.track_prepare("stmt1", "SELECT 1");
    assert!(state.has_state());
    state.track_close_statement("stmt1");
    assert!(!state.has_state());
}

#[test]
fn test_session_state_clear() {
    let mut state = SessionState::new();
    state.track_prepare("stmt1", "SELECT 1");
    state.track_listen("events");
    state.track_temp_table("tmp");
    state.track_set_guc("timezone", "UTC");
    state.set_in_transaction(true);
    state.clear();
    assert!(!state.has_state());
    assert!(state.is_reconnect_safe());
}

// ========================================================================
// Integration tests with real PostgreSQL (behind tokio-transport feature)
// ========================================================================

#[cfg(feature = "tokio-transport")]
mod integration {
    use super::*;
    use crate::common::test_config;
    use wasi_pg_client::Connection;

    #[tokio::test]
    async fn test_connect_with_retry_success() {
        let policy =
            RetryPolicy::exponential_backoff(3, Duration::from_millis(100), Duration::from_secs(5));
        let conn = Connection::connect_with_retry(&test_config(), &policy).await;
        assert!(conn.is_ok());
        let mut conn = conn.unwrap();
        conn.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_connect_with_retry_failure() {
        // Use an invalid host to force failure
        let config = wasi_pg_client::Config::new()
            .host("nonexistent.invalid")
            .port(5432)
            .user("postgres")
            .password("postgres");

        let policy = RetryPolicy::fixed_delay(2, Duration::from_millis(50));
        let result = Connection::connect_with_retry(&config, &policy).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_stale_detection() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();

        // Fresh connection should not be stale
        assert!(!conn.is_stale(Duration::from_secs(30)));

        // After pinging, last_confirmed_alive is updated
        conn.ping().await.unwrap();
        assert!(!conn.is_stale(Duration::from_secs(30)));

        conn.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_ensure_alive_fresh_connection() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        conn.ensure_alive().await.unwrap(); // should succeed immediately
        conn.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_connection_health_tracking() {
        let mut conn = Connection::connect(&test_config()).await.unwrap();
        assert!(conn.is_alive());

        // Ping updates health
        conn.ping().await.unwrap();
        assert!(conn.is_alive());

        // After close, the connection is no longer alive
        conn.close().await.unwrap();
    }
}
