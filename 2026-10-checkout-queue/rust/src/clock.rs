//! Where "now" comes from. Deadlines are computed from this clock and passed into the scripts,
//! and both background loops wait on it, so tests move time forward instead of sleeping.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

/// A wait on a [`Clock`], boxed so the trait stays usable as `Arc<dyn Clock>`.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub trait Clock: Send + Sync + 'static {
    /// Wall-clock time in epoch milliseconds. Every instance must agree on it (NTP).
    fn now_ms(&self) -> i64;

    /// Completes once `duration` has passed on this clock.
    fn sleep(&self, duration: Duration) -> Sleep;
}

/// The real clock: wall time for deadlines, Tokio's monotonic timer for waiting.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, millis)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(duration))
    }
}

/// A clock that only moves when told to, for tests (the Rust stand-in for .NET's FakeTimeProvider).
#[derive(Debug)]
pub struct ManualClock {
    now_ms: watch::Sender<i64>,
}

impl ManualClock {
    pub fn new(start_ms: i64) -> Self {
        Self {
            now_ms: watch::Sender::new(start_ms),
        }
    }

    /// Moves time forward and wakes every [`Clock::sleep`] whose time has come.
    pub fn advance(&self, by: Duration) {
        self.now_ms.send_modify(|now| *now += millis(by));
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        *self.now_ms.borrow()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        let mut now = self.now_ms.subscribe();
        let deadline = *now.borrow() + millis(duration);
        Box::pin(async move {
            if now.wait_for(|now| *now >= deadline).await.is_err() {
                // The clock is gone, so time never moves again.
                std::future::pending::<()>().await;
            }
        })
    }
}

/// Milliseconds in a duration, saturating instead of wrapping.
pub fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn system_clock_reads_epoch_milliseconds() {
        let now = SystemClock.now_ms();

        // Some time after 2026-01-01 and before 2100-01-01.
        assert!(now > 1_767_225_600_000 && now < 4_102_444_800_000, "{now}");
    }

    #[tokio::test]
    async fn system_clock_sleeps_for_real() {
        let started = std::time::Instant::now();

        SystemClock.sleep(Duration::from_millis(5)).await;

        assert!(started.elapsed() >= Duration::from_millis(5));
    }

    #[tokio::test]
    async fn manual_clock_wakes_sleepers_only_when_advanced_far_enough() {
        let clock = Arc::new(ManualClock::new(1_000));
        let sleeping = tokio::spawn(clock.sleep(Duration::from_secs(1)));

        clock.advance(Duration::from_millis(999));
        tokio::task::yield_now().await;
        assert!(!sleeping.is_finished());
        assert_eq!(clock.now_ms(), 1_999);

        clock.advance(Duration::from_millis(1));
        tokio::time::timeout(Duration::from_secs(5), sleeping)
            .await
            .expect("woke up")
            .expect("did not panic");
        assert_eq!(clock.now_ms(), 2_000);
    }

    #[tokio::test]
    async fn manual_clock_sleep_of_zero_completes_at_once() {
        let clock = ManualClock::new(0);

        tokio::time::timeout(Duration::from_secs(5), clock.sleep(Duration::ZERO))
            .await
            .expect("no wait");
    }

    #[test]
    fn millis_saturates() {
        assert_eq!(millis(Duration::from_millis(1_500)), 1_500);
        assert_eq!(millis(Duration::MAX), i64::MAX);
    }
}
