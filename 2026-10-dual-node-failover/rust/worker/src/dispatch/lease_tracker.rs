use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use shared::clock::MonotonicClock;

/// This worker's own view of its task lease. It starts at the send time of the request that won or
/// renewed it and ends a margin early, so the worker gives up before the upstream hands the task to
/// someone else.
pub struct LeaseTracker {
    clock: MonotonicClock,
    inner: Mutex<Inner>,
}

struct Inner {
    until: Duration,
    revoked: bool,
}

impl LeaseTracker {
    pub fn new(clock: MonotonicClock, until: Duration) -> Self {
        Self {
            clock,
            inner: Mutex::new(Inner {
                until,
                revoked: false,
            }),
        }
    }

    pub fn is_valid(&self) -> bool {
        let inner = self.lock();
        !inner.revoked && self.clock.now() < inner.until
    }

    /// Time left on the lease; zero once it is gone.
    pub fn remaining(&self) -> Duration {
        let inner = self.lock();
        if inner.revoked {
            Duration::ZERO
        } else {
            inner.until.saturating_sub(self.clock.now())
        }
    }

    /// Moves the end later; an older renewal that arrives late never shortens it.
    pub fn extend_to(&self, until: Duration) {
        let mut inner = self.lock();
        inner.until = inner.until.max(until);
    }

    /// The upstream said another attempt owns the task now.
    pub fn revoke(&self) {
        self.lock().revoked = true;
    }

    /// End of a lease that started at `sent_at`, minus a tenth for clock drift.
    pub fn end(sent_at: Duration, ttl_ms: i32) -> Duration {
        sent_at + ttl(ttl_ms) * 9 / 10
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Nothing panics while holding this lock.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A lease length from the wire; a negative one is no lease at all.
pub fn ttl(ttl_ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(ttl_ms).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use tokio::time::advance;

    use super::*;

    const TTL_MS: i32 = 1_000;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn the_end_is_ninety_percent_of_the_ttl_after_the_send() {
        assert_eq!(LeaseTracker::end(Duration::from_secs(1), TTL_MS), ms(1_900));
        assert_eq!(LeaseTracker::end(ms(0), 2_000), ms(1_800));
        assert_eq!(LeaseTracker::end(ms(5), -1), ms(5));
    }

    #[tokio::test(start_paused = true)]
    async fn the_lease_runs_out_unless_extended() {
        let clock = MonotonicClock::new();
        let tracker = LeaseTracker::new(clock, LeaseTracker::end(clock.now(), TTL_MS));

        advance(ms(800)).await;
        assert!(tracker.is_valid());
        tracker.extend_to(LeaseTracker::end(clock.now(), TTL_MS));
        tracker.extend_to(Duration::ZERO); // an older renewal never shortens it
        advance(ms(800)).await;
        assert!(tracker.is_valid());
        advance(ms(100)).await;

        assert!(!tracker.is_valid());
        assert_eq!(tracker.remaining(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn remaining_counts_down_to_the_end() {
        let clock = MonotonicClock::new();
        let tracker = LeaseTracker::new(clock, LeaseTracker::end(clock.now(), TTL_MS));

        advance(ms(600)).await;

        assert_eq!(tracker.remaining(), ms(300));
    }

    #[tokio::test(start_paused = true)]
    async fn revoke_ends_the_lease_at_once() {
        let clock = MonotonicClock::new();
        let tracker = LeaseTracker::new(clock, LeaseTracker::end(clock.now(), TTL_MS));

        tracker.revoke();
        tracker.extend_to(Duration::MAX);

        assert!(!tracker.is_valid());
        assert_eq!(tracker.remaining(), Duration::ZERO);
    }
}
