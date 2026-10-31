use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use shared::error::ApiError;
use shared::work::{EventPage, LoggedEvent, MAX_WORK_ID_LENGTH, NewEvent, is_valid_work_id};
use tokio::sync::watch;

/// Most events returned by one read, whatever the reader asks for.
pub const MAX_PAGE: usize = 500;
pub const MAX_AMOUNT: i64 = 1_000_000_000;

/// The feed for hot-hot redundancy: an append-only, in-memory log of events. Readers keep their own
/// position and can re-read from any point, which is what lets a restarted worker catch up without
/// asking the other worker anything.
pub struct EventLog {
    events: Mutex<Vec<LoggedEvent>>,
    /// Bumped on every append, under the lock, so a reader that subscribed under the lock cannot miss one.
    appended: watch::Sender<u64>,
}

impl Default for EventLog {
    fn default() -> Self {
        Self::new()
    }
}

impl EventLog {
    pub fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            appended: watch::Sender::new(0),
        }
    }

    /// The newest sequence number; 0 while the log is empty.
    pub fn last(&self) -> i64 {
        seq_of(self.lock().len())
    }

    pub fn append(&self, request: &NewEvent) -> Result<LoggedEvent, ApiError> {
        let Some(account) = request.account.as_deref().filter(|a| is_valid_work_id(a)) else {
            return Err(ApiError::InvalidInput(format!(
                "account must be 1-{MAX_WORK_ID_LENGTH} ASCII letters, digits, '-', '_' or '.'"
            )));
        };
        if !(-MAX_AMOUNT..=MAX_AMOUNT).contains(&request.amount) {
            return Err(ApiError::InvalidInput(format!(
                "amount must be between -{MAX_AMOUNT} and {MAX_AMOUNT}"
            )));
        }
        let mut events = self.lock();
        let logged = LoggedEvent {
            seq: seq_of(events.len()) + 1,
            account: account.to_owned(),
            amount: request.amount,
        };
        events.push(logged.clone());
        self.appended
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(logged)
    }

    /// Events after `after`; waits up to `wait` for one if there are none yet.
    pub async fn read(
        &self,
        after: i64,
        limit: i32,
        wait: Duration,
    ) -> Result<EventPage, ApiError> {
        if after < 0 || limit < 1 {
            return Err(ApiError::InvalidInput(
                "after must be >= 0 and limit >= 1".into(),
            ));
        }
        let mut appended = {
            let events = self.lock();
            if after < seq_of(events.len()) || wait.is_zero() {
                return Ok(page(&events, after, limit));
            }
            // Taken under the lock: an append after this point is seen by `changed` below.
            self.appended.subscribe()
        };
        // A timeout just means nothing new: answer with an empty page.
        let _ = tokio::time::timeout(wait, appended.changed()).await;
        Ok(page(&self.lock(), after, limit))
    }

    fn lock(&self) -> MutexGuard<'_, Vec<LoggedEvent>> {
        // Nothing panics while holding this lock, and the log is only ever appended to.
        self.events.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn page(events: &[LoggedEvent], after: i64, limit: i32) -> EventPage {
    let start = usize::try_from(after)
        .unwrap_or(usize::MAX)
        .min(events.len());
    let count = usize::try_from(limit)
        .unwrap_or(0)
        .min(MAX_PAGE)
        .min(events.len() - start);
    EventPage {
        events: events[start..start + count].to_vec(),
        last: seq_of(events.len()),
    }
}

fn seq_of(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    const WAIT: Duration = Duration::from_secs(1);

    fn event(account: &str, amount: i64) -> NewEvent {
        NewEvent {
            account: Some(account.into()),
            amount,
        }
    }

    fn seqs(page: &EventPage) -> Vec<i64> {
        page.events.iter().map(|e| e.seq).collect()
    }

    #[test]
    fn append_numbers_events_from_one() {
        let log = EventLog::new();
        assert_eq!(log.last(), 0);

        let first = log.append(&event("acct-1", 10)).unwrap();
        let second = log.append(&event("acct-2", -5)).unwrap();

        assert_eq!(
            first,
            LoggedEvent {
                seq: 1,
                account: "acct-1".into(),
                amount: 10
            }
        );
        assert_eq!((second.seq, second.amount), (2, -5));
        assert_eq!(log.last(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn read_returns_events_after_the_position() {
        let log = EventLog::new();
        for i in 1..=5 {
            log.append(&event("acct", i)).unwrap();
        }

        let page = log.read(2, 2, Duration::ZERO).await.unwrap();
        let past_the_end = log.read(9, 10, Duration::ZERO).await.unwrap();

        assert_eq!((seqs(&page), page.last), (vec![3, 4], 5));
        assert_eq!((seqs(&past_the_end), past_the_end.last), (vec![], 5));
    }

    #[tokio::test(start_paused = true)]
    async fn read_caps_the_page() {
        let log = EventLog::new();
        for _ in 0..MAX_PAGE + 10 {
            log.append(&event("acct", 1)).unwrap();
        }

        let page = log.read(0, i32::MAX, Duration::ZERO).await.unwrap();

        assert_eq!(page.events.len(), MAX_PAGE);
    }

    #[tokio::test(start_paused = true)]
    async fn read_waits_for_the_next_event() {
        let log = Arc::new(EventLog::new());
        let reader = Arc::clone(&log);
        let reading = tokio::spawn(async move { reader.read(0, 10, WAIT).await });
        tokio::task::yield_now().await;
        assert!(!reading.is_finished());

        log.append(&event("acct", 1)).unwrap();

        let page = reading.await.unwrap().unwrap();
        assert_eq!(seqs(&page), vec![1]);
    }

    #[tokio::test(start_paused = true)]
    async fn read_returns_an_empty_page_when_nothing_arrives() {
        let log = EventLog::new();
        let started = tokio::time::Instant::now();

        let page = log.read(0, 10, WAIT).await.unwrap();

        assert_eq!(started.elapsed(), WAIT);
        assert_eq!((page.events.len(), page.last), (0, 0));
    }

    #[test]
    fn append_rejects_bad_events() {
        let log = EventLog::new();
        for bad in [
            NewEvent::default(),
            event("", 1),
            event("bad account", 1),
            event("acct", MAX_AMOUNT + 1),
            event("acct", -MAX_AMOUNT - 1),
            event("acct", i64::MIN),
        ] {
            let error = log.append(&bad).unwrap_err();
            assert!(matches!(error, ApiError::InvalidInput(_)), "{bad:?}");
        }
        assert!(log.append(&event("acct", -MAX_AMOUNT)).is_ok());
        assert_eq!(log.last(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn read_rejects_a_negative_position_or_an_empty_page() {
        let log = EventLog::new();

        for (after, limit) in [(-1, 10), (0, 0), (0, -1)] {
            let error = log.read(after, limit, Duration::ZERO).await.unwrap_err();
            assert_eq!(error.message(), "after must be >= 0 and limit >= 1");
        }
    }
}
