//! Environment variable parsing for reconnection settings.
//!
//! Supported environment variables:
//!
//! - `PGRECONNECT` — "true" / "false" (enable/disable reconnection)
//! - `PGRECONNECT_ATTEMPTS` — max reconnection attempts (e.g., "5")
//! - `PGRECONNECT_DELAY_MS` — initial delay in milliseconds (e.g., "200")
//! - `PGRECONNECT_MAX_DELAY_MS` — max delay in milliseconds (e.g., "10000")
//! - `PGSTALE_THRESHOLD_SECS` — stale threshold in seconds (e.g., "60")

use std::time::Duration;

use crate::reconnect::config::{ReconnectConfig, StaleConfig};

/// Apply reconnection-related environment variables to the config.
///
/// This is called during `Config::from_env()`.
pub fn apply_reconnect_env(reconnect: &mut ReconnectConfig, stale: &mut StaleConfig) {
    if let Ok(val) = std::env::var("PGRECONNECT") {
        reconnect.enabled = val == "true" || val == "1";
    }
    if let Ok(val) = std::env::var("PGRECONNECT_ATTEMPTS") {
        if let Ok(n) = val.parse() {
            reconnect.max_attempts = n;
        }
    }
    if let Ok(val) = std::env::var("PGRECONNECT_DELAY_MS") {
        if let Ok(ms) = val.parse() {
            reconnect.initial_delay = Duration::from_millis(ms);
        }
    }
    if let Ok(val) = std::env::var("PGRECONNECT_MAX_DELAY_MS") {
        if let Ok(ms) = val.parse() {
            reconnect.max_delay = Duration::from_millis(ms);
        }
    }
    if let Ok(val) = std::env::var("PGSTALE_THRESHOLD_SECS") {
        if let Ok(secs) = val.parse() {
            stale.stale_threshold = Duration::from_secs(secs);
        }
    }
}

/// Parse reconnection-related parameters from a connection string.
///
/// Supported parameters:
///
/// - `reconnect` — "true" / "false"
/// - `reconnect_max_attempts` — integer
/// - `reconnect_initial_delay_ms` — integer (milliseconds)
/// - `reconnect_max_delay_ms` — integer (milliseconds)
/// - `stale_threshold_secs` — integer (seconds)
pub fn parse_reconnect_params(
    reconnect: &mut ReconnectConfig,
    stale: &mut StaleConfig,
    key: &str,
    value: &str,
) -> Result<(), String> {
    match key {
        "reconnect" => {
            reconnect.enabled = value.parse().map_err(|_| {
                format!(
                    "invalid value for 'reconnect': expected 'true' or 'false', got '{}'",
                    value
                )
            })?;
        }
        "reconnect_max_attempts" => {
            reconnect.max_attempts = value.parse().map_err(|_| {
                format!(
                    "invalid value for 'reconnect_max_attempts': expected integer, got '{}'",
                    value
                )
            })?;
        }
        "reconnect_initial_delay_ms" => {
            let ms: u64 = value.parse().map_err(|_| {
                format!(
                    "invalid value for 'reconnect_initial_delay_ms': expected integer, got '{}'",
                    value
                )
            })?;
            reconnect.initial_delay = Duration::from_millis(ms);
        }
        "reconnect_max_delay_ms" => {
            let ms: u64 = value.parse().map_err(|_| {
                format!(
                    "invalid value for 'reconnect_max_delay_ms': expected integer, got '{}'",
                    value
                )
            })?;
            reconnect.max_delay = Duration::from_millis(ms);
        }
        "stale_threshold_secs" => {
            let secs: u64 = value.parse().map_err(|_| {
                format!(
                    "invalid value for 'stale_threshold_secs': expected integer, got '{}'",
                    value
                )
            })?;
            stale.stale_threshold = Duration::from_secs(secs);
        }
        _ => return Err(format!("unknown reconnection parameter: {}", key)),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_reconnect_params_enable() {
        let mut reconnect = ReconnectConfig::default();
        let mut stale = StaleConfig::default();
        parse_reconnect_params(&mut reconnect, &mut stale, "reconnect", "true").unwrap();
        assert!(reconnect.enabled);
    }

    #[test]
    fn test_parse_reconnect_params_disable() {
        let mut reconnect = ReconnectConfig::default();
        let mut stale = StaleConfig::default();
        parse_reconnect_params(&mut reconnect, &mut stale, "reconnect", "false").unwrap();
        assert!(!reconnect.enabled);
    }

    #[test]
    fn test_parse_reconnect_params_max_attempts() {
        let mut reconnect = ReconnectConfig::default();
        let mut stale = StaleConfig::default();
        parse_reconnect_params(&mut reconnect, &mut stale, "reconnect_max_attempts", "5").unwrap();
        assert_eq!(reconnect.max_attempts, 5);
    }

    #[test]
    fn test_parse_reconnect_params_initial_delay() {
        let mut reconnect = ReconnectConfig::default();
        let mut stale = StaleConfig::default();
        parse_reconnect_params(
            &mut reconnect,
            &mut stale,
            "reconnect_initial_delay_ms",
            "200",
        )
        .unwrap();
        assert_eq!(reconnect.initial_delay, Duration::from_millis(200));
    }

    #[test]
    fn test_parse_reconnect_params_max_delay() {
        let mut reconnect = ReconnectConfig::default();
        let mut stale = StaleConfig::default();
        parse_reconnect_params(
            &mut reconnect,
            &mut stale,
            "reconnect_max_delay_ms",
            "30000",
        )
        .unwrap();
        assert_eq!(reconnect.max_delay, Duration::from_secs(30));
    }

    #[test]
    fn test_parse_reconnect_params_stale_threshold() {
        let mut reconnect = ReconnectConfig::default();
        let mut stale = StaleConfig::default();
        parse_reconnect_params(&mut reconnect, &mut stale, "stale_threshold_secs", "60").unwrap();
        assert_eq!(stale.stale_threshold, Duration::from_secs(60));
    }

    #[test]
    fn test_parse_reconnect_params_unknown() {
        let mut reconnect = ReconnectConfig::default();
        let mut stale = StaleConfig::default();
        let result = parse_reconnect_params(&mut reconnect, &mut stale, "unknown_param", "value");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_reconnect_params_invalid_value() {
        let mut reconnect = ReconnectConfig::default();
        let mut stale = StaleConfig::default();
        let result = parse_reconnect_params(&mut reconnect, &mut stale, "reconnect", "maybe");
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_reconnect_env() {
        // Set env vars
        std::env::set_var("PGRECONNECT", "true");
        std::env::set_var("PGRECONNECT_ATTEMPTS", "7");
        std::env::set_var("PGRECONNECT_DELAY_MS", "500");
        std::env::set_var("PGRECONNECT_MAX_DELAY_MS", "20000");
        std::env::set_var("PGSTALE_THRESHOLD_SECS", "120");

        let mut reconnect = ReconnectConfig::default();
        let mut stale = StaleConfig::default();
        apply_reconnect_env(&mut reconnect, &mut stale);

        assert!(reconnect.enabled);
        assert_eq!(reconnect.max_attempts, 7);
        assert_eq!(reconnect.initial_delay, Duration::from_millis(500));
        assert_eq!(reconnect.max_delay, Duration::from_secs(20));
        assert_eq!(stale.stale_threshold, Duration::from_secs(120));

        // Clean up
        std::env::remove_var("PGRECONNECT");
        std::env::remove_var("PGRECONNECT_ATTEMPTS");
        std::env::remove_var("PGRECONNECT_DELAY_MS");
        std::env::remove_var("PGRECONNECT_MAX_DELAY_MS");
        std::env::remove_var("PGSTALE_THRESHOLD_SECS");
    }

    #[test]
    fn test_apply_reconnect_env_invalid_values_ignored() {
        std::env::set_var("PGRECONNECT_ATTEMPTS", "not_a_number");
        std::env::set_var("PGRECONNECT_DELAY_MS", "also_not_a_number");

        let mut reconnect = ReconnectConfig::default();
        let mut stale = StaleConfig::default();
        apply_reconnect_env(&mut reconnect, &mut stale);

        // Should keep defaults since the values are invalid
        assert_eq!(reconnect.max_attempts, 3);
        assert_eq!(reconnect.initial_delay, Duration::from_millis(100));

        // Clean up
        std::env::remove_var("PGRECONNECT_ATTEMPTS");
        std::env::remove_var("PGRECONNECT_DELAY_MS");
    }
}
