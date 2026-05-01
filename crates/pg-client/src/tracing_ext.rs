//! Internal helpers for consistent tracing across the library.
//!
//! This module is NOT public — it provides internal macros and helpers
//! for structured logging via the `tracing` crate.
//!
//! # Target Namespacing
//!
//! All tracing targets are namespaced under `wasi_pg_client::*`, allowing
//! users to filter to just our library's events. Sub-targets (transport,
//! connection, query, etc.) allow fine-grained filtering.
//!
//! # Sensitive Data Redaction
//!
//! Passwords, auth tokens, SCRAM proofs, query parameter values, and row
//! data are NEVER logged at INFO/DEBUG/WARN/ERROR levels. Only at TRACE
//! level is some potentially sensitive data exposed, and this is clearly
//! documented.
//!
//! # Tracing Level Guide
//!
//! | Level | What gets logged |
//! |-------|-----------------|
//! | ERROR | Fatal errors: auth failed, TLS handshake failed, reconnection failed after all attempts |
//! | WARN  | Recoverable problems: connection broken, transaction rolled back, pool guard dropped without release |
//! | INFO  | Normal operations: connection established/closed, query completed, transaction BEGIN/COMMIT/ROLLBACK |
//! | DEBUG | Detailed info: TCP connect attempt, auth method, pool acquire/release, retry attempt |
//! | TRACE | Wire-level detail: every protocol message, full SQL, buffer flush operations |
//!
//! ⚠️ TRACE may expose sensitive data. Use only in development/debugging, never in production.
//!
//! # WASI P2 Subscriber Setup
//!
//! The `tracing` crate is just a facade. Users must install a subscriber
//! (e.g., `tracing-subscriber`) in their application. The library only emits
//! spans and events — it doesn't configure how they're handled.
//!
//! Example: Setting up tracing in a WASI P2 component:
//!
//! ```ignore
//! // Add to your Cargo.toml:
//! //   tracing-subscriber = "0.3"
//!
//! #[wstd::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     use tracing_subscriber::{fmt, EnvFilter};
//!
//!     // Install a simple subscriber that writes to stderr.
//!     // stderr is available on WASI P2 via wasi:cli/stderr.
//!     fmt()
//!         .with_env_filter(
//!             EnvFilter::try_from_default_env()
//!                 .unwrap_or_else(|_| EnvFilter::new("wasi_pg_client=info"))
//!         )
//!         .with_writer(std::io::stderr)  // Use stderr (available on WASI)
//!         .init();
//!
//!     // Now use the library — all operations will be traced
//!     let mut conn = Connection::connect(&config).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Environment Variable Filtering
//!
//! Users can control tracing verbosity via the `RUST_LOG` environment variable
//! (available on WASI P2 via `wasi:cli/environment`):
//!
//! ```bash
//! # Production: only info and above
//! wasmtime run --wasi inherit-env --env RUST_LOG=wasi_pg_client=info component.wasm
//!
//! # Debugging: detailed operation info
//! wasmtime run --wasi inherit-env --env RUST_LOG=wasi_pg_client=debug component.wasm
//!
//! # Protocol debugging: very verbose
//! wasmtime run --wasi inherit-env --env RUST_LOG=wasi_pg_client=trace component.wasm
//!
//! # Only connection events
//! wasmtime run --wasi inherit-env --env RUST_LOG=wasi_pg_client::connection=debug component.wasm
//!
//! # Only query events
//! wasmtime run --wasi inherit-env --env RUST_LOG=wasi_pg_client::query=debug component.wasm
//! ```

/// Target prefix for all wasi-pg-client tracing events.
///
/// Users can filter to only our events with:
/// ```ignore
/// tracing_subscriber::filter::Targets::new()
///     .with_target("wasi_pg_client", tracing::Level::DEBUG)
/// ```
pub const TARGET_PREFIX: &str = "wasi_pg_client";

/// Target for transport-layer events.
pub const TARGET_TRANSPORT: &str = "wasi_pg_client::transport";

/// Target for connection lifecycle events.
pub const TARGET_CONNECTION: &str = "wasi_pg_client::connection";

/// Target for authentication events.
pub const TARGET_AUTH: &str = "wasi_pg_client::auth";

/// Target for query execution events.
pub const TARGET_QUERY: &str = "wasi_pg_client::query";

/// Target for transaction events.
pub const TARGET_TRANSACTION: &str = "wasi_pg_client::transaction";

/// Target for COPY protocol events.
pub const TARGET_COPY: &str = "wasi_pg_client::copy";

/// Target for notification events.
pub const TARGET_NOTIFICATION: &str = "wasi_pg_client::notification";

/// Target for pool events.
pub const TARGET_POOL: &str = "wasi_pg_client::pool";

/// Target for reconnection events.
pub const TARGET_RECONNECT: &str = "wasi_pg_client::reconnect";

/// Target for wire protocol events.
pub const TARGET_PROTOCOL: &str = "wasi_pg_client::protocol";

/// Target for cancel request events.
pub const TARGET_CANCEL: &str = "wasi_pg_client::cancel";

// ---------------------------------------------------------------------------
// Sensitive data redaction helpers
// ---------------------------------------------------------------------------

/// Truncate a string to `max_len` characters, appending "..." if truncated.
///
/// Used for SQL text in DEBUG-level tracing to avoid flooding logs with
/// huge queries. Full SQL can be logged at TRACE level.
///
/// # Examples
/// ```ignore
/// assert_eq!(truncate_str("hello world", 5), "hello...");
/// assert_eq!(truncate_str("hi", 10), "hi");
/// ```
pub fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        // Find a valid char boundary to avoid panicking on multi-byte chars
        let mut end = max_len;
        while !s.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

/// Redact a connection string (URI), replacing the password with "***".
///
/// # Examples
/// ```ignore
/// let input = "postgresql://user:secret@host:5432/db";
/// let output = redact_connection_string(input);
/// assert_eq!(output, "postgresql://user:***@host:5432/db");
/// ```
pub fn redact_connection_string(s: &str) -> String {
    // Simple heuristic: find the userinfo part (between :// and the last @
    // before the first / after the authority). We need to find the @ that
    // separates userinfo from host, not an @ inside the password.
    // Strategy: find the last @ in the authority component.
    if let Some(start) = s.find("://") {
        let after_scheme = &s[start + 3..];
        // Find the last @ in the authority part (before the first / or end)
        let authority_end = after_scheme.find('/').unwrap_or(after_scheme.len());
        let authority = &after_scheme[..authority_end];
        if let Some(at_pos) = authority.rfind('@') {
            let user_part = &authority[..at_pos];
            if let Some(colon_pos) = user_part.find(':') {
                let before = &s[..start + 3 + colon_pos + 1];
                let after = &s[start + 3 + at_pos..];
                return format!("{}***{}", before, after);
            }
        }
    }
    s.to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_str_short() {
        assert_eq!(truncate_str("hi", 10), "hi");
    }

    #[test]
    fn test_truncate_str_exact() {
        assert_eq!(truncate_str("hello", 5), "hello");
    }

    #[test]
    fn test_truncate_str_long() {
        assert_eq!(truncate_str("hello world", 5), "hello...");
    }

    #[test]
    fn test_truncate_str_empty() {
        assert_eq!(truncate_str("", 5), "");
    }

    #[test]
    fn test_truncate_str_zero_max() {
        assert_eq!(truncate_str("hello", 0), "...");
    }

    #[test]
    fn test_truncate_str_multibyte() {
        // "café" is 5 bytes but 4 chars; the é is 2 bytes
        let s = "café résumé";
        let truncated = truncate_str(s, 5);
        // Should not panic and should be valid UTF-8
        assert!(truncated.ends_with("...") || truncated.len() <= 8);
        assert!(std::str::from_utf8(truncated.as_bytes()).is_ok());
    }

    #[test]
    fn test_redact_connection_string_with_password() {
        let input = "postgresql://user:secret@host:5432/db";
        let output = redact_connection_string(input);
        assert_eq!(output, "postgresql://user:***@host:5432/db");
        assert!(!output.contains("secret"));
    }

    #[test]
    fn test_redact_connection_string_no_password() {
        let input = "postgresql://user@host:5432/db";
        let output = redact_connection_string(input);
        assert_eq!(output, "postgresql://user@host:5432/db");
    }

    #[test]
    fn test_redact_connection_string_no_scheme() {
        let input = "host=localhost port=5432";
        let output = redact_connection_string(input);
        assert_eq!(output, "host=localhost port=5432");
    }

    #[test]
    fn test_redact_connection_string_complex_password() {
        let input = "postgresql://admin:p@ss:w0rd@db.example.com:5432/mydb";
        let output = redact_connection_string(input);
        assert_eq!(output, "postgresql://admin:***@db.example.com:5432/mydb");
        assert!(!output.contains("p@ss:w0rd"));
    }

    #[test]
    fn test_target_constants_are_prefixed() {
        assert!(TARGET_TRANSPORT.starts_with(TARGET_PREFIX));
        assert!(TARGET_CONNECTION.starts_with(TARGET_PREFIX));
        assert!(TARGET_AUTH.starts_with(TARGET_PREFIX));
        assert!(TARGET_QUERY.starts_with(TARGET_PREFIX));
        assert!(TARGET_TRANSACTION.starts_with(TARGET_PREFIX));
        assert!(TARGET_COPY.starts_with(TARGET_PREFIX));
        assert!(TARGET_NOTIFICATION.starts_with(TARGET_PREFIX));
        assert!(TARGET_POOL.starts_with(TARGET_PREFIX));
        assert!(TARGET_RECONNECT.starts_with(TARGET_PREFIX));
        assert!(TARGET_PROTOCOL.starts_with(TARGET_PREFIX));
        assert!(TARGET_CANCEL.starts_with(TARGET_PREFIX));
    }
}
