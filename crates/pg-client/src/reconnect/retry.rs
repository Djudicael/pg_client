//! Standalone retry policy for operations that don't need a Connection.
//!
//! This module defines [`RetryPolicy`] which can be used to retry any async
//! operation with exponential backoff. It's useful for retrying connection
//! establishment itself, or for custom retry logic in application code.

use std::future::Future;
use std::time::Duration;

/// A standalone retry policy that can be used without a Connection.
///
/// Useful for retrying connection establishment itself, or for custom
/// retry logic in application code.
///
/// # Example
///
/// ```rust,ignore
/// let policy = RetryPolicy::exponential_backoff(5, Duration::from_millis(100), Duration::from_secs(5));
/// let result = policy.retry(|| async {
///     some_fallible_operation().await
/// }).await?;
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Maximum number of retry attempts.
    pub max_attempts: u32,
    /// Initial delay between attempts.
    pub initial_delay: Duration,
    /// Maximum delay between attempts (cap for exponential backoff).
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        }
    }
}

impl RetryPolicy {
    /// Create a retry policy that does not retry (single attempt).
    pub fn no_retry() -> Self {
        RetryPolicy {
            max_attempts: 1,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    /// Create a retry policy with the specified number of attempts and fixed delay.
    pub fn fixed_delay(max_attempts: u32, delay: Duration) -> Self {
        RetryPolicy {
            max_attempts,
            initial_delay: delay,
            max_delay: delay,
        }
    }

    /// Create a retry policy with exponential backoff.
    pub fn exponential_backoff(
        max_attempts: u32,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Self {
        RetryPolicy {
            max_attempts,
            initial_delay,
            max_delay,
        }
    }

    /// Execute an async operation with this retry policy.
    ///
    /// The operation is retried if it returns an error. All errors are retried
    /// (no classification — use [`crate::Connection::with_retry`] for smart
    /// retry based on error class).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let policy = RetryPolicy::exponential_backoff(3, Duration::from_millis(100), Duration::from_secs(5));
    /// let result = policy.retry(|| async {
    ///     some_fallible_operation().await
    /// }).await?;
    /// ```
    #[must_use = "retry errors should be checked"]
    pub async fn retry<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut attempt = 0;

        loop {
            attempt += 1;
            match f().await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if attempt >= self.max_attempts {
                        return Err(err);
                    }

                    let delay = self.delay_for_attempt(attempt);
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        attempt = attempt,
                        delay_ms = delay.as_millis(),
                        error = %err,
                        "Retrying after error"
                    );
                    sleep(delay).await;
                }
            }
        }
    }

    /// Calculate the delay for a given attempt number (1-based).
    ///
    /// Uses exponential backoff: initial_delay * 2^(attempt-1), capped at max_delay.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let multiplier = 2u32.saturating_pow(attempt.saturating_sub(1));
        let delay = self.initial_delay * multiplier;
        delay.min(self.max_delay)
    }
}

/// Platform-aware async sleep.
///
/// Uses `wstd::task::sleep` on WASI P2 and `tokio::time::sleep` on native.
#[cfg(target_arch = "wasm32")]
async fn sleep(duration: Duration) {
    wstd::task::sleep(duration.into()).await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn sleep(duration: Duration) {
    // For native builds, use a simple tokio sleep if available,
    // otherwise a busy-wait. In practice, this is called from
    // tokio-transport builds.
    #[cfg(feature = "tokio-transport")]
    tokio::time::sleep(duration).await;

    #[cfg(not(feature = "tokio-transport"))]
    {
        // Fallback: busy-wait (not ideal, but works for testing)
        let start = std::time::Instant::now();
        while start.elapsed() < duration {
            std::thread::yield_now();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
            RetryPolicy::exponential_backoff(5, Duration::from_millis(100), Duration::from_secs(5));
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.initial_delay, Duration::from_millis(100));
        assert_eq!(policy.max_delay, Duration::from_secs(5));
    }

    #[test]
    fn test_delay_for_attempt() {
        let policy = RetryPolicy::exponential_backoff(
            10,
            Duration::from_millis(100),
            Duration::from_secs(5),
        );

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

        // attempt 6: 100ms * 2^5 = 3200ms
        assert_eq!(policy.delay_for_attempt(6), Duration::from_millis(3200));

        // attempt 7: 100ms * 2^6 = 5000ms (capped at max_delay)
        assert_eq!(policy.delay_for_attempt(7), Duration::from_secs(5));
    }

    #[test]
    fn test_delay_for_attempt_zero_delay() {
        let policy = RetryPolicy::no_retry();
        assert_eq!(policy.delay_for_attempt(1), Duration::ZERO);
    }

    #[tokio::test]
    async fn test_retry_succeeds_first_try() {
        let policy = RetryPolicy::fixed_delay(3, Duration::from_millis(1));
        let mut call_count = 0;
        let result = policy
            .retry(|| {
                call_count += 1;
                async { Ok::<i32, &str>(42) }
            })
            .await;
        assert_eq!(result, Ok(42));
        assert_eq!(call_count, 1);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_failures() {
        let policy = RetryPolicy::fixed_delay(3, Duration::from_millis(1));
        let mut call_count = 0;
        let result = policy
            .retry(|| {
                call_count += 1;
                async move {
                    if call_count < 3 {
                        Err("temporary")
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;
        assert_eq!(result, Ok(42));
        assert_eq!(call_count, 3);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let policy = RetryPolicy::fixed_delay(2, Duration::from_millis(1));
        let mut call_count = 0;
        let result: Result<i32, &str> = policy
            .retry(|| {
                call_count += 1;
                async { Err("always fails") }
            })
            .await;
        assert_eq!(result, Err("always fails"));
        assert_eq!(call_count, 2);
    }
}
