//! Connection configuration for PostgreSQL.
//!
//! This module defines the `Config` struct which holds parameters for connecting
//! to a PostgreSQL server, including connection string parsing.

use std::time::Duration;

use crate::transport::SslMode;

/// Target session attributes for connection validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TargetSessionAttrs {
    /// Any session is acceptable (default).
    #[default]
    Any,
    /// The session must be read-write (reject hot standbys).
    ReadWrite,
    /// The session must be read-only (prefer standbys).
    ReadOnly,
}

impl TargetSessionAttrs {
    /// Parse from a string.
    pub fn from_str(s: &str) -> Result<Self, ConfigError> {
        match s.to_lowercase().as_str() {
            "any" => Ok(TargetSessionAttrs::Any),
            "read-write" => Ok(TargetSessionAttrs::ReadWrite),
            "read-only" => Ok(TargetSessionAttrs::ReadOnly),
            _ => Err(ConfigError::InvalidValue(format!(
                "invalid target_session_attrs: {s}"
            ))),
        }
    }
}

/// Errors that can occur when building or parsing a configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// An invalid value was provided.
    #[error("invalid value: {0}")]
    InvalidValue(String),
    /// A required field is missing.
    #[error("missing required field: {0}")]
    MissingField(String),
    /// The connection string could not be parsed.
    #[error("parse error: {0}")]
    ParseError(String),
}

/// Configuration for a PostgreSQL connection.
///
/// Use `Config::new()` to create a default configuration and then set fields
/// using the builder methods, or parse from a connection string with
/// `Config::from_uri` or `Config::from_key_value`.
#[derive(Debug, Clone)]
pub struct Config {
    /// The hostname or IP address of the PostgreSQL server.
    pub(crate) host: String,
    /// The port number of the PostgreSQL server.
    pub(crate) port: u16,
    /// The username to authenticate with.
    pub(crate) user: String,
    /// The password to authenticate with.
    pub(crate) password: Option<String>,
    /// The database name to connect to.
    pub(crate) database: Option<String>,
    /// Application name reported to PostgreSQL.
    pub(crate) application_name: Option<String>,
    /// SSL/TLS mode.
    pub(crate) ssl_mode: SslMode,
    /// Extra startup parameters.
    pub(crate) options: Vec<(String, String)>,
    /// Connection timeout.
    pub(crate) connect_timeout: Option<Duration>,
    /// Statement timeout (sent as `statement_timeout` startup param).
    pub(crate) statement_timeout: Option<Duration>,
    /// Target session attributes.
    pub(crate) target_session_attrs: TargetSessionAttrs,
    /// Whether to use TLS (deprecated, prefer `ssl_mode`).
    pub(crate) use_tls: bool,
    /// Accept invalid/self-signed TLS certificates.
    /// **WARNING**: Only for development/testing. Never use in production.
    pub(crate) accept_invalid_certs: bool,
    /// TCP keepalive settings.
    pub(crate) keepalive: Option<Duration>,
    /// Reconnection policy.
    pub(crate) reconnect: crate::reconnect::config::ReconnectConfig,
    /// Stale connection detection.
    pub(crate) stale: crate::reconnect::config::StaleConfig,
}

impl Config {
    /// Creates a new configuration with default values.
    ///
    /// Defaults:
    /// - host: `"localhost"`
    /// - port: `5432`
    /// - user: `"postgres"`
    /// - password: `None`
    /// - database: `None`
    /// - ssl_mode: `Prefer` (if tls feature enabled) / `Disable` (otherwise)
    /// - connect_timeout: `None`
    /// - target_session_attrs: `Any`
    pub fn new() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: None,
            database: None,
            application_name: None,
            #[cfg(feature = "tls")]
            ssl_mode: SslMode::Prefer,
            #[cfg(not(feature = "tls"))]
            ssl_mode: SslMode::Disable,
            options: Vec::new(),
            connect_timeout: None,
            statement_timeout: None,
            target_session_attrs: TargetSessionAttrs::Any,
            use_tls: cfg!(feature = "tls"),
            accept_invalid_certs: false,
            keepalive: None,
            reconnect: crate::reconnect::config::ReconnectConfig::default(),
            stale: crate::reconnect::config::StaleConfig::default(),
        }
    }

    // -----------------------------------------------------------------------
    // Builder methods
    // -----------------------------------------------------------------------

    /// Sets the hostname or IP address.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Sets the port number.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets the username.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    /// Sets the password.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Sets the database name.
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Sets the application name.
    pub fn application_name(mut self, name: impl Into<String>) -> Self {
        self.application_name = Some(name.into());
        self
    }

    /// Sets the SSL mode.
    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.ssl_mode = mode;
        self
    }

    /// Sets the connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Sets the statement timeout (sent as a startup parameter).
    pub fn statement_timeout(mut self, timeout: Duration) -> Self {
        self.statement_timeout = Some(timeout);
        self
    }

    /// Sets the target session attributes.
    pub fn target_session_attrs(mut self, attrs: TargetSessionAttrs) -> Self {
        self.target_session_attrs = attrs;
        self
    }

    /// Adds an extra startup option.
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.push((key.into(), value.into()));
        self
    }

    /// Sets whether to use TLS (legacy alias for `ssl_mode`).
    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    /// Accept invalid/self-signed TLS certificates.
    /// **WARNING**: Only for development/testing. Never use in production.
    pub fn accept_invalid_certs(mut self, accept: bool) -> Self {
        self.accept_invalid_certs = accept;
        self
    }

    /// Sets the TCP keepalive interval.
    pub fn keepalive(mut self, keepalive: Duration) -> Self {
        self.keepalive = Some(keepalive);
        self
    }

    /// Sets the reconnection policy.
    pub fn reconnect(mut self, config: crate::reconnect::config::ReconnectConfig) -> Self {
        self.reconnect = config;
        self
    }

    /// Enable automatic reconnection with default settings.
    pub fn enable_reconnect(self) -> Self {
        self.reconnect(crate::reconnect::config::ReconnectConfig::enabled())
    }

    /// Set the maximum number of reconnection attempts.
    pub fn max_reconnect_attempts(mut self, n: u32) -> Self {
        self.reconnect.max_attempts = n;
        self
    }

    /// Set the stale connection detection threshold.
    pub fn stale_threshold(mut self, threshold: std::time::Duration) -> Self {
        self.stale.stale_threshold = threshold;
        self
    }

    /// Sets the stale connection detection configuration.
    pub fn stale(mut self, config: crate::reconnect::config::StaleConfig) -> Self {
        self.stale = config;
        self
    }

    // -----------------------------------------------------------------------
    // Getters
    // -----------------------------------------------------------------------

    pub fn get_host(&self) -> &str {
        &self.host
    }
    pub fn get_port(&self) -> u16 {
        self.port
    }
    pub fn get_user(&self) -> &str {
        &self.user
    }
    pub fn get_password(&self) -> Option<&str> {
        self.password.as_deref()
    }
    pub fn get_database(&self) -> Option<&str> {
        self.database.as_deref()
    }
    pub fn get_application_name(&self) -> Option<&str> {
        self.application_name.as_deref()
    }
    pub fn get_ssl_mode(&self) -> SslMode {
        self.ssl_mode
    }
    pub fn get_connect_timeout(&self) -> Option<Duration> {
        self.connect_timeout
    }
    pub fn get_statement_timeout(&self) -> Option<Duration> {
        self.statement_timeout
    }
    pub fn get_target_session_attrs(&self) -> TargetSessionAttrs {
        self.target_session_attrs
    }
    pub fn get_use_tls(&self) -> bool {
        self.use_tls
    }
    pub fn get_accept_invalid_certs(&self) -> bool {
        self.accept_invalid_certs
    }
    pub fn get_keepalive(&self) -> Option<Duration> {
        self.keepalive
    }
    pub fn get_reconnect(&self) -> &crate::reconnect::config::ReconnectConfig {
        &self.reconnect
    }
    pub fn get_stale(&self) -> &crate::reconnect::config::StaleConfig {
        &self.stale
    }

    /// Returns the startup parameters to send in the StartupMessage.
    pub fn startup_params(&self) -> Vec<(String, String)> {
        let mut params = vec![
            ("user".to_string(), self.user.clone()),
            (
                "database".to_string(),
                self.database.clone().unwrap_or_else(|| self.user.clone()),
            ),
            ("client_encoding".to_string(), "UTF8".to_string()),
        ];
        if let Some(ref app_name) = self.application_name {
            params.push(("application_name".to_string(), app_name.clone()));
        }
        if let Some(timeout) = self.statement_timeout {
            params.push((
                "statement_timeout".to_string(),
                format!("{}ms", timeout.as_millis()),
            ));
        }
        for (k, v) in &self.options {
            params.push((k.clone(), v.clone()));
        }
        params
    }

    // -----------------------------------------------------------------------
    // Parsing
    // -----------------------------------------------------------------------

    /// Parse a PostgreSQL connection URI.
    ///
    /// Supported format:
    /// ```text
    /// postgresql://[user[:password]@][host][:port][/dbname][?param1=value1&...]
    /// ```
    pub fn from_uri(uri: &str) -> Result<Self, ConfigError> {
        let url = url::Url::parse(uri)
            .map_err(|e| ConfigError::ParseError(format!("invalid URI: {e}")))?;

        if url.scheme() != "postgresql" && url.scheme() != "postgres" {
            return Err(ConfigError::ParseError(format!(
                "expected scheme 'postgresql' or 'postgres', got '{}'",
                url.scheme()
            )));
        }

        let mut config = Config::new();

        // Host
        config.host = url.host_str().unwrap_or("localhost").to_string();

        // Port
        config.port = url.port().unwrap_or(5432);

        // User / Password
        if let Some(info) = url.password() {
            config.password = Some(info.to_string());
        }
        if !url.username().is_empty() {
            config.user = url.username().to_string();
        }

        // Database (path without leading slash)
        let path = url.path();
        if path.len() > 1 {
            config.database = Some(path[1..].to_string());
        }

        // Query parameters
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "sslmode" => {
                    config.ssl_mode = SslMode::from_str(value.as_ref())
                        .map_err(|e| ConfigError::InvalidValue(e.to_string()))?;
                }
                "connect_timeout" => {
                    if let Ok(secs) = value.parse::<u64>() {
                        config.connect_timeout = Some(Duration::from_secs(secs));
                    }
                }
                "application_name" => {
                    config.application_name = Some(value.to_string());
                }
                "target_session_attrs" => {
                    config.target_session_attrs = TargetSessionAttrs::from_str(value.as_ref())?;
                }
                "reconnect"
                | "reconnect_max_attempts"
                | "reconnect_initial_delay_ms"
                | "reconnect_max_delay_ms"
                | "stale_threshold_secs" => {
                    if let Err(_e) = crate::reconnect::env::parse_reconnect_params(
                        &mut config.reconnect,
                        &mut config.stale,
                        key.as_ref(),
                        value.as_ref(),
                    ) {
                        // Unknown reconnect params are ignored (not added to options)
                        // rather than causing an error
                    }
                }
                _ => {
                    config.options.push((key.to_string(), value.to_string()));
                }
            }
        }

        Ok(config)
    }

    /// Parse a key-value connection string.
    ///
    /// Format:
    /// ```text
    /// host=localhost port=5432 dbname=mydb user=myuser password=secret sslmode=require
    /// ```
    ///
    /// Values containing spaces may be wrapped in single quotes:
    /// ```text
    /// host='my host' user=postgres
    /// ```
    pub fn from_key_value(s: &str) -> Result<Self, ConfigError> {
        let mut config = Config::new();

        // Simple tokenizer that respects single-quoted values.
        let tokens = tokenize_key_value(s)?;

        for token in tokens {
            let mut parts = token.splitn(2, '=');
            let key = parts
                .next()
                .ok_or_else(|| ConfigError::ParseError("empty key".into()))?;
            let value = parts
                .next()
                .ok_or_else(|| ConfigError::ParseError(format!("missing value for {key}")))?;
            // Unquote if wrapped in single quotes
            let value = value
                .strip_prefix('\'')
                .and_then(|s| s.strip_suffix('\''))
                .unwrap_or(value);

            match key {
                "host" => config.host = value.to_string(),
                "port" => {
                    config.port = value
                        .parse()
                        .map_err(|e| ConfigError::InvalidValue(format!("invalid port: {e}")))?;
                }
                "user" => config.user = value.to_string(),
                "password" => config.password = Some(value.to_string()),
                "dbname" | "database" => config.database = Some(value.to_string()),
                "application_name" => config.application_name = Some(value.to_string()),
                "sslmode" => {
                    config.ssl_mode = SslMode::from_str(value)
                        .map_err(|e| ConfigError::InvalidValue(e.to_string()))?;
                }
                "connect_timeout" => {
                    if let Ok(secs) = value.parse::<u64>() {
                        config.connect_timeout = Some(Duration::from_secs(secs));
                    }
                }
                "target_session_attrs" => {
                    config.target_session_attrs = TargetSessionAttrs::from_str(value)?;
                }
                "reconnect"
                | "reconnect_max_attempts"
                | "reconnect_initial_delay_ms"
                | "reconnect_max_delay_ms"
                | "stale_threshold_secs" => {
                    let _ = crate::reconnect::env::parse_reconnect_params(
                        &mut config.reconnect,
                        &mut config.stale,
                        key,
                        value,
                    );
                }
                _ => config.options.push((key.to_string(), value.to_string())),
            }
        }

        Ok(config)
    }

    /// Build a configuration from standard PostgreSQL environment variables.
    ///
    /// Variables: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`,
    /// `PGSSLMODE`, `PGCONNECT_TIMEOUT`, `PGAPPNAME`.
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut config = Config::new();

        if let Ok(v) = std::env::var("PGHOST") {
            config.host = v;
        }
        if let Ok(v) = std::env::var("PGPORT") {
            config.port = v
                .parse()
                .map_err(|e| ConfigError::InvalidValue(format!("PGPORT: {e}")))?;
        }
        if let Ok(v) = std::env::var("PGDATABASE") {
            config.database = Some(v);
        }
        if let Ok(v) = std::env::var("PGUSER") {
            config.user = v;
        }
        if let Ok(v) = std::env::var("PGPASSWORD") {
            config.password = Some(v);
        }
        if let Ok(v) = std::env::var("PGSSLMODE") {
            config.ssl_mode = SslMode::from_str(&v)
                .map_err(|e| ConfigError::InvalidValue(format!("PGSSLMODE: {e}")))?;
        }
        if let Ok(v) = std::env::var("PGCONNECT_TIMEOUT") {
            if let Ok(secs) = v.parse::<u64>() {
                config.connect_timeout = Some(Duration::from_secs(secs));
            }
        }
        if let Ok(v) = std::env::var("PGAPPNAME") {
            config.application_name = Some(v);
        }

        crate::reconnect::env::apply_reconnect_env(&mut config.reconnect, &mut config.stale);

        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Tokenize a key-value string, respecting single-quoted values.
fn tokenize_key_value(s: &str) -> Result<Vec<String>, ConfigError> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let chars = s.chars();

    for ch in chars {
        if ch == '\'' {
            in_quote = !in_quote;
            current.push(ch);
        } else if ch.is_whitespace() && !in_quote {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
        } else {
            current.push(ch);
        }
    }

    if in_quote {
        return Err(ConfigError::ParseError(
            "unclosed single quote in connection string".into(),
        ));
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = Config::new()
            .host("my-host")
            .port(15432)
            .user("my-user")
            .password("secret")
            .database("my-db")
            .ssl_mode(SslMode::Require)
            .connect_timeout(Duration::from_secs(10))
            .application_name("test-app");

        assert_eq!(config.get_host(), "my-host");
        assert_eq!(config.get_port(), 15432);
        assert_eq!(config.get_user(), "my-user");
        assert_eq!(config.get_password(), Some("secret"));
        assert_eq!(config.get_database(), Some("my-db"));
        assert_eq!(config.get_ssl_mode(), SslMode::Require);
        assert_eq!(config.get_connect_timeout(), Some(Duration::from_secs(10)));
        assert_eq!(config.get_application_name(), Some("test-app"));
    }

    #[test]
    fn test_config_startup_params() {
        let config = Config::new()
            .user("postgres")
            .database("test")
            .application_name("my-app");

        let params = config.startup_params();
        assert!(params.iter().any(|(k, v)| k == "user" && v == "postgres"));
        assert!(params.iter().any(|(k, v)| k == "database" && v == "test"));
        assert!(params
            .iter()
            .any(|(k, v)| k == "client_encoding" && v == "UTF8"));
        assert!(params
            .iter()
            .any(|(k, v)| k == "application_name" && v == "my-app"));
    }

    #[test]
    fn test_parse_uri_basic() {
        let config =
            Config::from_uri("postgresql://user:pass@host:1234/db?sslmode=require").unwrap();
        assert_eq!(config.get_host(), "host");
        assert_eq!(config.get_port(), 1234);
        assert_eq!(config.get_user(), "user");
        assert_eq!(config.get_password(), Some("pass"));
        assert_eq!(config.get_database(), Some("db"));
        assert_eq!(config.get_ssl_mode(), SslMode::Require);
    }

    #[test]
    fn test_parse_uri_defaults() {
        let config = Config::from_uri("postgresql://localhost").unwrap();
        assert_eq!(config.get_host(), "localhost");
        assert_eq!(config.get_port(), 5432);
        assert_eq!(config.get_user(), "postgres"); // default
    }

    #[test]
    fn test_parse_key_value() {
        let config = Config::from_key_value(
            "host=myhost port=5433 user=u password=p dbname=d sslmode=disable",
        )
        .unwrap();
        assert_eq!(config.get_host(), "myhost");
        assert_eq!(config.get_port(), 5433);
        assert_eq!(config.get_user(), "u");
        assert_eq!(config.get_password(), Some("p"));
        assert_eq!(config.get_database(), Some("d"));
        assert_eq!(config.get_ssl_mode(), SslMode::Disable);
    }

    #[test]
    fn test_parse_key_value_quoted() {
        let config = Config::from_key_value("host='my host' user=u").unwrap();
        assert_eq!(config.get_host(), "my host");
    }

    #[test]
    fn test_target_session_attrs_roundtrip() {
        assert_eq!(
            TargetSessionAttrs::from_str("read-write").unwrap(),
            TargetSessionAttrs::ReadWrite
        );
        assert_eq!(
            TargetSessionAttrs::from_str("read-only").unwrap(),
            TargetSessionAttrs::ReadOnly
        );
        assert!(TargetSessionAttrs::from_str("bogus").is_err());
    }

    #[test]
    fn test_reconnect_config() {
        let config = Config::new()
            .enable_reconnect()
            .max_reconnect_attempts(5)
            .stale_threshold(std::time::Duration::from_secs(60));

        assert!(config.get_reconnect().enabled);
        assert_eq!(config.get_reconnect().max_attempts, 5);
        assert_eq!(
            config.get_stale().stale_threshold,
            std::time::Duration::from_secs(60)
        );
    }

    #[test]
    fn test_reconnect_from_uri() {
        let config = Config::from_uri(
            "postgresql://user@host/db?reconnect=true&reconnect_max_attempts=5&stale_threshold_secs=60",
        )
        .unwrap();
        assert!(config.get_reconnect().enabled);
        assert_eq!(config.get_reconnect().max_attempts, 5);
        assert_eq!(
            config.get_stale().stale_threshold,
            std::time::Duration::from_secs(60)
        );
    }
}
