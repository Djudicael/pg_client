//! Reconnection and stale connection configuration.
//!
//! This module defines [`ReconnectConfig`] and [`StaleConfig`] which control
//! automatic reconnection behavior and proactive stale connection detection.

use std::fmt;
use std::time::Duration;

/// Callback invoked before a reconnection attempt.
///
/// Can be used for logging, metrics, or custom logic.
/// The callback receives the attempt number and the error that triggered
/// the reconnection.
pub type ReconnectCallback = Box<dyn Fn(u32, &crate::error::PgError) + Send + Sync>;

/// Reconnection policy configuration.
///
/// By default, automatic reconnection is **disabled**. Users must explicitly
/// enable it via `Config::enable_reconnect()` or connection string `reconnect=true`.
///
/// # Example
///
/// ```rust,ignore
/// let config = Config::new()
///     .host("localhost")
///     .enable_reconnect()
///     .max_reconnect_attempts(5);
/// ```
pub struct ReconnectConfig {
    /// Whether automatic reconnection is enabled.
    /// When enabled, the connection will attempt to reconnect when a broken
    /// connection is detected.
    /// Default: false (opt-in).
    pub enabled: bool,

    /// Maximum number of reconnection attempts before giving up.
    /// Each attempt may involve DNS resolution, TCP connect, TLS, and auth.
    /// Default: 3.
    pub max_attempts: u32,

    /// Delay between reconnection attempts.
    /// Uses exponential backoff: initial_delay * 2^(attempt-1) (capped at max_delay).
    /// Default: 100ms initial, 10s max.
    pub initial_delay: Duration,
    /// Maximum delay between reconnection attempts (cap for exponential backoff).
    pub max_delay: Duration,

    /// Whether to rebuild session state after reconnection.
    /// When enabled, the connection will re-prepare statements, re-LISTEN
    /// channels, and re-SET custom GUC parameters after reconnecting.
    /// Default: true.
    pub rebuild_session: bool,

    /// Whether reconnection is allowed mid-transaction.
    /// When false (default), reconnection is only attempted if the connection
    /// is not inside a transaction. Mid-transaction reconnection is dangerous
    /// because the transaction state is lost and the operation may have
    /// partially completed.
    /// Default: false.
    pub allow_mid_transaction: bool,

    /// Callback invoked before a reconnection attempt.
    /// Can be used for logging, metrics, or custom logic.
    /// The callback receives the attempt number and the error that triggered
    /// the reconnection.
    ///
    /// Note: This field is not cloned. When a `ReconnectConfig` is cloned,
    /// the callback is set to `None` in the clone.
    pub on_before_reconnect: Option<ReconnectCallback>,
}

impl Clone for ReconnectConfig {
    fn clone(&self) -> Self {
        ReconnectConfig {
            enabled: self.enabled,
            max_attempts: self.max_attempts,
            initial_delay: self.initial_delay,
            max_delay: self.max_delay,
            rebuild_session: self.rebuild_session,
            allow_mid_transaction: self.allow_mid_transaction,
            on_before_reconnect: None, // callbacks are not clonable
        }
    }
}

impl fmt::Debug for ReconnectConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReconnectConfig")
            .field("enabled", &self.enabled)
            .field("max_attempts", &self.max_attempts)
            .field("initial_delay", &self.initial_delay)
            .field("max_delay", &self.max_delay)
            .field("rebuild_session", &self.rebuild_session)
            .field("allow_mid_transaction", &self.allow_mid_transaction)
            .field(
                "on_before_reconnect",
                &self.on_before_reconnect.as_ref().map(|_| "Some(callback)"),
            )
            .finish()
    }
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        ReconnectConfig {
            enabled: false,
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            rebuild_session: true,
            allow_mid_transaction: false,
            on_before_reconnect: None,
        }
    }
}

impl ReconnectConfig {
    /// Create a new reconnect config with reconnection enabled.
    pub fn enabled() -> Self {
        ReconnectConfig {
            enabled: true,
            ..ReconnectConfig::default()
        }
    }

    /// Set the maximum number of reconnection attempts.
    pub fn max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n;
        self
    }

    /// Set the initial delay for exponential backoff.
    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set the maximum delay for exponential backoff.
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set whether to rebuild session state after reconnection.
    pub fn rebuild_session(mut self, rebuild: bool) -> Self {
        self.rebuild_session = rebuild;
        self
    }

    /// Set whether to allow mid-transaction reconnection.
    pub fn allow_mid_transaction(mut self, allow: bool) -> Self {
        self.allow_mid_transaction = allow;
        self
    }

    /// Calculate the backoff delay for the given attempt number (1-based).
    ///
    /// Uses exponential backoff: initial_delay * 2^(attempt-1), capped at max_delay.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let multiplier = 2u32.saturating_pow(attempt.saturating_sub(1));
        let delay = self.initial_delay * multiplier;
        delay.min(self.max_delay)
    }
}

/// Configuration for proactive stale connection detection.
///
/// A connection is considered "stale" if it hasn't been confirmed alive
/// recently. Stale connections are pinged before use to verify they're
/// still alive.
#[derive(Debug, Clone)]
pub struct StaleConfig {
    /// Time threshold after which a connection is considered "stale"
    /// and should be pinged before use.
    /// Default: 30 seconds.
    pub stale_threshold: Duration,

    /// Whether to automatically ping stale connections before use.
    /// If false, stale connections are used without checking (may fail).
    /// Default: true.
    pub ping_on_stale: bool,
}

impl Default for StaleConfig {
    fn default() -> Self {
        StaleConfig {
            stale_threshold: Duration::from_secs(30),
            ping_on_stale: true,
        }
    }
}

impl StaleConfig {
    /// Set the stale threshold duration.
    pub fn stale_threshold(mut self, threshold: Duration) -> Self {
        self.stale_threshold = threshold;
        self
    }

    /// Set whether to ping stale connections before use.
    pub fn ping_on_stale(mut self, ping: bool) -> Self {
        self.ping_on_stale = ping;
        self
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconnect_config_default() {
        let config = ReconnectConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(10));
        assert!(config.rebuild_session);
        assert!(!config.allow_mid_transaction);
        assert!(config.on_before_reconnect.is_none());
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
    fn test_reconnect_delay_for_attempt() {
        let config = ReconnectConfig::default();

        // attempt 1: 100ms * 2^0 = 100ms
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(100));

        // attempt 2: 100ms * 2^1 = 200ms
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(200));

        // attempt 3: 100ms * 2^2 = 400ms
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(400));

        // attempt 7: 100ms * 2^6 = 6400ms
        assert_eq!(config.delay_for_attempt(7), Duration::from_millis(6400));

        // attempt 8: 100ms * 2^7 = 12800ms, capped at 10s
        assert_eq!(config.delay_for_attempt(8), Duration::from_secs(10));
    }

    #[test]
    fn test_stale_config_default() {
        let config = StaleConfig::default();
        assert_eq!(config.stale_threshold, Duration::from_secs(30));
        assert!(config.ping_on_stale);
    }

    #[test]
    fn test_stale_config_builder() {
        let config = StaleConfig::default()
            .stale_threshold(Duration::from_secs(60))
            .ping_on_stale(false);

        assert_eq!(config.stale_threshold, Duration::from_secs(60));
        assert!(!config.ping_on_stale);
    }
}
