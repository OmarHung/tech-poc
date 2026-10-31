use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use shared::error::ApiError;
use shared::wire::is_valid_node_id;
use shared::work::{ResultRecord, is_valid_work_id};

/// Longest result value, in UTF-16 code units like the .NET version counts string length.
pub const MAX_VALUE_LENGTH: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultStatus {
    /// First result for this key: the side effect happens now.
    Applied,
    /// The same value again (the other hot-hot worker, or a retried task). Ignored.
    Duplicate,
    /// A different value for a key that already has one: the two copies disagree. The first one stays.
    Conflict,
    /// From an older attempt than one already seen: a worker that lost its task lease and kept going.
    Stale,
}

impl ResultStatus {
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Duplicate => "duplicate",
            Self::Conflict => "conflict",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkSummary {
    /// Every result that arrived: the number of side effects a sink without deduplication would cause.
    pub received: i64,
    /// Distinct keys: the side effects that actually happened.
    pub applied: i64,
    pub duplicates: i64,
    pub conflicts: i64,
    pub stale: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultView {
    pub value: String,
    pub node: String,
    pub attempt: i32,
    /// Wall-clock time (Unix ms) the first copy arrived.
    pub first_at_ms: i64,
    pub copies: i64,
    pub conflicts: i64,
    pub stale: i64,
}

/// What `GET /results` returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkSnapshot {
    pub summary: SinkSummary,
    pub results: BTreeMap<String, ResultView>,
}

type WallClock = Box<dyn Fn() -> i64 + Send + Sync>;

/// The downstream end. Applying a result is the side effect (send the mail, book the payment), so it
/// must happen once per key no matter how many copies arrive: the first copy is applied, later ones are
/// only counted.
pub struct ResultStore {
    now_ms: WallClock,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    entries: BTreeMap<String, Entry>,
    received: i64,
    duplicates: i64,
    conflicts: i64,
    stale: i64,
}

struct Entry {
    view: ResultView,
    max_attempt: i32,
}

impl Default for ResultStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ResultStore {
    pub fn new() -> Self {
        Self::with_clock(Box::new(unix_ms))
    }

    /// `now_ms` stamps the first copy of each result; tests pass a fixed clock.
    pub fn with_clock(now_ms: WallClock) -> Self {
        Self {
            now_ms,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn record(&self, record: &ResultRecord) -> Result<ResultStatus, ApiError> {
        let (Some(key), Some(node), Some(value)) = (
            record.key.as_deref().filter(|k| is_valid_work_id(k)),
            record.node.as_deref().filter(|n| is_valid_node_id(n)),
            record
                .value
                .as_deref()
                .filter(|v| v.encode_utf16().count() <= MAX_VALUE_LENGTH),
        ) else {
            return Err(invalid());
        };
        if record.attempt < 0 {
            return Err(invalid());
        }
        let mut inner = self.lock();
        inner.received += 1;
        let Some(entry) = inner.entries.get_mut(key) else {
            let view = ResultView {
                value: value.to_owned(),
                node: node.to_owned(),
                attempt: record.attempt,
                first_at_ms: (self.now_ms)(),
                copies: 1,
                conflicts: 0,
                stale: 0,
            };
            let entry = Entry {
                view,
                max_attempt: record.attempt,
            };
            inner.entries.insert(key.to_owned(), entry);
            return Ok(ResultStatus::Applied);
        };
        entry.view.copies += 1;
        if record.attempt < entry.max_attempt {
            entry.view.stale += 1;
            inner.stale += 1;
            return Ok(ResultStatus::Stale);
        }
        entry.max_attempt = record.attempt;
        if entry.view.value == value {
            inner.duplicates += 1;
            return Ok(ResultStatus::Duplicate);
        }
        entry.view.conflicts += 1;
        inner.conflicts += 1;
        Ok(ResultStatus::Conflict)
    }

    pub fn summary(&self) -> SinkSummary {
        summary_of(&self.lock())
    }

    pub fn snapshot(&self) -> SinkSnapshot {
        let inner = self.lock();
        SinkSnapshot {
            summary: summary_of(&inner),
            results: inner
                .entries
                .iter()
                .map(|(key, entry)| (key.clone(), entry.view.clone()))
                .collect(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Nothing panics while holding this lock.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn summary_of(inner: &Inner) -> SinkSummary {
    SinkSummary {
        received: inner.received,
        applied: i64::try_from(inner.entries.len()).unwrap_or(i64::MAX),
        duplicates: inner.duplicates,
        conflicts: inner.conflicts,
        stale: inner.stale,
    }
}

fn invalid() -> ApiError {
    ApiError::InvalidInput(format!(
        "key must be a valid id, node a node id, attempt >= 0 and value a string of at most {MAX_VALUE_LENGTH} characters"
    ))
}

/// Wall-clock Unix milliseconds: the verify script compares them with its own clock.
pub fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> ResultStore {
        ResultStore::with_clock(Box::new(|| 1_000))
    }

    fn result(key: &str, node: &str, attempt: i32, value: &str) -> ResultRecord {
        ResultRecord {
            key: Some(key.into()),
            node: Some(node.into()),
            attempt,
            value: Some(value.into()),
        }
    }

    #[test]
    fn the_first_copy_is_applied_and_the_same_value_again_is_a_duplicate() {
        let store = store();

        assert_eq!(
            store.record(&result("event-1", "a", 0, "acct:5")),
            Ok(ResultStatus::Applied)
        );
        assert_eq!(
            store.record(&result("event-1", "b", 0, "acct:5")),
            Ok(ResultStatus::Duplicate)
        );

        let snapshot = store.snapshot();
        assert_eq!(
            snapshot.summary,
            SinkSummary {
                received: 2,
                applied: 1,
                duplicates: 1,
                conflicts: 0,
                stale: 0
            }
        );
        assert_eq!(
            snapshot.results["event-1"],
            ResultView {
                value: "acct:5".into(),
                node: "a".into(),
                attempt: 0,
                first_at_ms: 1_000,
                copies: 2,
                conflicts: 0,
                stale: 0
            }
        );
    }

    #[test]
    fn a_different_value_is_a_conflict_and_the_first_one_stays() {
        let store = store();
        store.record(&result("event-1", "a", 0, "acct:5")).unwrap();

        assert_eq!(
            store.record(&result("event-1", "b", 0, "acct:7")),
            Ok(ResultStatus::Conflict)
        );

        let snapshot = store.snapshot();
        assert_eq!(snapshot.results["event-1"].value, "acct:5");
        assert_eq!(snapshot.results["event-1"].conflicts, 1);
        assert_eq!(snapshot.summary.conflicts, 1);
    }

    #[test]
    fn an_older_attempt_after_a_newer_one_is_stale() {
        let store = store();
        store.record(&result("task-1", "b", 2, "1^2=1")).unwrap();

        assert_eq!(
            store.record(&result("task-1", "a", 1, "1^2=1")),
            Ok(ResultStatus::Stale)
        );
        assert_eq!(
            store.record(&result("task-1", "a", 3, "1^2=1")),
            Ok(ResultStatus::Duplicate)
        );
        // The highest attempt seen is now 3, so attempt 2 is stale too.
        assert_eq!(
            store.record(&result("task-1", "b", 2, "1^2=1")),
            Ok(ResultStatus::Stale)
        );
        let snapshot = store.snapshot();
        assert_eq!(snapshot.summary.stale, 2);
        assert_eq!(
            (
                snapshot.results["task-1"].copies,
                snapshot.results["task-1"].stale
            ),
            (4, 2)
        );
        assert_eq!(snapshot.results["task-1"].attempt, 2); // the applied copy's attempt
    }

    #[test]
    fn bad_records_are_rejected_and_not_counted() {
        let store = store();
        let long = "x".repeat(MAX_VALUE_LENGTH + 1);
        let bad = [
            ResultRecord::default(),
            ResultRecord {
                key: None,
                ..result("k", "a", 0, "v")
            },
            result("bad key", "a", 0, "v"),
            result("k", "A", 0, "v"),
            result("k", "a", -1, "v"),
            ResultRecord {
                value: None,
                ..result("k", "a", 0, "v")
            },
            result("k", "a", 0, &long),
            // 1024 characters but 2048 UTF-16 units
            result("k", "a", 0, &"😀".repeat(MAX_VALUE_LENGTH / 2 + 1)),
        ];
        for record in bad {
            let error = store.record(&record).unwrap_err();
            assert!(matches!(error, ApiError::InvalidInput(_)), "{record:?}");
        }
        assert!(
            store
                .record(&result("k", "a", 0, &"x".repeat(MAX_VALUE_LENGTH)))
                .is_ok()
        );
        assert_eq!(store.summary().received, 1);
    }

    #[test]
    fn the_wall_clock_is_unix_milliseconds() {
        // 2020-09-13 and later; the verify script compares these with its own time.time().
        assert!(unix_ms() > 1_600_000_000_000);
    }
}
