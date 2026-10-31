use std::time::Duration;

use tokio::time::Instant;

/// Time elapsed since this clock was created, read from the monotonic clock tokio's timers use.
///
/// Leases are measured with it so a wall-clock jump cannot stretch or shrink them. Tests pause the
/// runtime clock and move it with `tokio::time::advance`, which moves this clock and every timer together.
#[derive(Debug, Clone, Copy)]
pub struct MonotonicClock {
    origin: Instant,
}

impl MonotonicClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }

    pub fn now(&self) -> Duration {
        Instant::now().saturating_duration_since(self.origin)
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn follows_the_runtime_clock() {
        let clock = MonotonicClock::default();
        assert_eq!(clock.now(), Duration::ZERO);

        tokio::time::advance(Duration::from_millis(1500)).await;

        assert_eq!(clock.now(), Duration::from_millis(1500));
    }
}
