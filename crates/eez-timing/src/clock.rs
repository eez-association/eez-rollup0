//! The wall clock that slot timing reads, injected so tests control it.

use core::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Source of the current unix time in milliseconds.
pub trait Clock: Send + Sync + fmt::Debug {
    /// Current unix time in milliseconds.
    fn now_unix_ms(&self) -> u64;
}

/// Shared handle to a [`Clock`].
pub type SharedClock = Arc<dyn Clock>;

/// The system wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
    }
}

/// Unix ms derived from tokio's `Instant`, so reads agree with tokio sleeps,
/// including under paused time in tests.
#[derive(Debug, Clone, Copy)]
pub struct TokioClock {
    unix_ms_at_start: u64,
    start: tokio::time::Instant,
}

impl TokioClock {
    /// A clock that reads the system time now and then advances with tokio time.
    #[must_use]
    pub fn new() -> Self {
        Self::starting_at(SystemClock.now_unix_ms())
    }

    /// A clock that reads `unix_ms` now and then advances with tokio time.
    #[must_use]
    pub fn starting_at(unix_ms: u64) -> Self {
        Self {
            unix_ms_at_start: unix_ms,
            start: tokio::time::Instant::now(),
        }
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TokioClock {
    fn now_unix_ms(&self) -> u64 {
        let elapsed = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.unix_ms_at_start.saturating_add(elapsed)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn tokio_clock_follows_paused_sleeps() {
        let clock = TokioClock::starting_at(1_700_000_000_250);
        assert_eq!(clock.now_unix_ms(), 1_700_000_000_250);
        tokio::time::sleep(Duration::from_millis(1_750)).await;
        assert_eq!(clock.now_unix_ms(), 1_700_000_002_000);
    }
}
