//! Automatic reconnection, retry policies, and connection resilience.
//!
//! This module provides:
//!
//! - **Error classification**: [`ErrorClass`] and [`classify_error`] for categorizing
//!   errors as Broken, Transient, or Permanent.
//! - **Reconnection configuration**: [`ReconnectConfig`] for controlling automatic
//!   reconnection behavior.
//! - **Stale connection detection**: [`StaleConfig`] for proactive detection of
//!   connections that may be broken.
//! - **Retry policy**: [`RetryPolicy`] for standalone retry with exponential backoff.
//! - **Session state tracking**: [`SessionState`] for tracking state that would be
//!   lost on reconnection (prepared statements, LISTEN channels, GUCs).
//! - **Connection health**: [`ConnectionHealth`] for tracking connection liveness
//!   and reconnection history.
//!
//! # Design Philosophy
//!
//! **Transparent reconnection is opt-in**. By default, broken connections return
//! errors. Users must explicitly enable reconnection via `Config::enable_reconnect()`
//! or connection string `reconnect=true`.
//!
//! **Retry policies are explicit**. The library provides retry helpers but does
//! not automatically retry queries. Users choose when and how to retry via
//! [`Connection::with_retry()`](crate::Connection::with_retry).
//!
//! **Mid-transaction reconnection is blocked by default**. If a connection breaks
//! mid-transaction, the transaction state is lost and the operation may have
//! partially completed. Set `allow_mid_transaction=true` to override this.

pub mod classify;
pub mod config;
pub mod env;
pub mod retry;
pub mod session;

pub use classify::{classify_error, ErrorClass};
pub use config::{ReconnectCallback, ReconnectConfig, StaleConfig};
pub use retry::RetryPolicy;
pub use session::{ConnectionHealth, SessionState};
