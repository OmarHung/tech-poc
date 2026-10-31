use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use super::log_file::{LogFile, LogFileError};
use super::{LogEntry, LogSlice};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendStatus {
    Ok,
    Mismatch,
}

/// `match_index`: on `Ok`, the last index of the batch, now known to match the leader. On `Mismatch`,
/// where the leader should continue from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendOutcome {
    pub status: AppendStatus,
    pub match_index: i64,
}

impl AppendOutcome {
    fn ok(match_index: i64) -> Self {
        Self {
            status: AppendStatus::Ok,
            match_index,
        }
    }

    fn mismatch(match_index: i64) -> Self {
        Self {
            status: AppendStatus::Mismatch,
            match_index,
        }
    }
}

#[derive(Debug)]
pub enum AppendError {
    /// The batch does not fit together (a 400 for the sender).
    Invalid(String),
    Io(io::Error),
}

impl fmt::Display for AppendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::Io(error) => write!(f, "could not write the log: {error}"),
        }
    }
}

impl std::error::Error for AppendError {}

impl From<io::Error> for AppendError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// The node's data: an ordered log of writes plus the key-value view built from it. Thread-safe.
///
/// Replication uses Raft's log-matching rule: an entry is identified by (index, epoch), a standby only
/// accepts a batch that continues an entry it already has, and drops whatever it has past that point
/// that disagrees.
pub struct ReplicatedLog {
    inner: Mutex<Inner>,
}

struct Inner {
    file: LogFile,
    entries: Vec<LogEntry>,
    /// Key -> position in `entries` of its latest write.
    latest: HashMap<String, usize>,
    /// Entries dropped because a newer primary never had them (writes nobody was told succeeded).
    truncated: i64,
}

impl ReplicatedLog {
    pub fn open(path: &Path) -> Result<Self, LogFileError> {
        let (file, entries) = LogFile::open(path)?;
        let mut inner = Inner {
            file,
            entries,
            latest: HashMap::new(),
            truncated: 0,
        };
        inner.rebuild_latest();
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    pub fn last_index(&self) -> i64 {
        self.lock().len()
    }

    pub fn last_epoch(&self) -> i64 {
        self.lock().entries.last().map_or(0, |entry| entry.epoch)
    }

    pub fn truncated_entries(&self) -> i64 {
        self.lock().truncated
    }

    /// Appends one write as the primary.
    pub fn append(&self, epoch: i64, key: &str, value: &str) -> io::Result<LogEntry> {
        let mut inner = self.lock();
        let entry = LogEntry {
            index: inner.len() + 1,
            epoch,
            key: key.to_owned(),
            value: value.to_owned(),
        };
        inner.file.append(std::slice::from_ref(&entry))?;
        inner.add(entry.clone());
        Ok(entry)
    }

    /// Applies a batch from the primary (the standby side of replication).
    ///
    /// `leader_epoch` is the primary's epoch; `leader_last_index` its last index when it built the batch.
    pub fn append_from(
        &self,
        leader_epoch: i64,
        prev_index: i64,
        prev_epoch: i64,
        entries: &[LogEntry],
        leader_last_index: i64,
    ) -> Result<AppendOutcome, AppendError> {
        validate_batch(leader_epoch, prev_index, entries, leader_last_index)?;
        let mut inner = self.lock();
        if inner.epoch_at(prev_index) != Some(prev_epoch) {
            return Ok(AppendOutcome::mismatch(inner.mismatch_hint(prev_index)));
        }
        let mut fresh: Vec<LogEntry> = Vec::new();
        for entry in entries {
            if fresh.is_empty() && entry.index <= inner.len() {
                if inner.entries[position(entry.index)].epoch == entry.epoch {
                    continue; // already have it, e.g. a resend after a timeout
                }
                inner.truncate(entry.index)?;
            }
            fresh.push(entry.clone());
        }
        if !fresh.is_empty() {
            inner.file.append(&fresh)?;
            fresh.into_iter().for_each(|entry| inner.add(entry));
        }
        let matched = prev_index + entries.len() as i64;
        // The batch reached the primary's last entry when it was sent, so entries past it from an older
        // epoch came from an older primary. Entries from this epoch stay: a request that arrived late
        // must not undo a newer one.
        if matched == leader_last_index
            && inner.len() > matched
            && inner.entries[position(matched + 1)].epoch < leader_epoch
        {
            inner.truncate(matched + 1)?;
        }
        Ok(AppendOutcome::ok(matched))
    }

    /// Up to `max` entries starting at `from_index`; `None` if that is past the end.
    pub fn read(&self, from_index: i64, max: usize) -> Option<LogSlice> {
        let inner = self.lock();
        let prev_epoch = inner.epoch_at(from_index.checked_sub(1)?)?;
        let start = position(from_index);
        let count = (inner.entries.len() - start).min(max);
        Some(LogSlice {
            prev_epoch,
            entries: inner.entries[start..start + count].to_vec(),
            last_index: inner.len(),
        })
    }

    pub fn get(&self, key: &str) -> Option<LogEntry> {
        let inner = self.lock();
        inner.latest.get(key).map(|&at| inner.entries[at].clone())
    }

    /// Every key with its latest value.
    pub fn snapshot(&self) -> BTreeMap<String, String> {
        let inner = self.lock();
        inner
            .latest
            .iter()
            .map(|(key, &at)| (key.clone(), inner.entries[at].value.clone()))
            .collect()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Nothing panics while holding this lock; if something ever did, the data is still a valid
        // prefix of the file, so keep serving rather than fail every later request.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Inner {
    fn len(&self) -> i64 {
        self.entries.len() as i64
    }

    /// Epoch at `index`: 0 for index 0 (the empty prefix), `None` past either end.
    fn epoch_at(&self, index: i64) -> Option<i64> {
        match index {
            0 => Some(0),
            _ if index < 0 || index > self.len() => None,
            _ => Some(self.entries[position(index)].epoch),
        }
    }

    /// Where the leader should retry from: the end of this log if it is too short, otherwise just
    /// before the first entry of the disagreeing epoch, so a whole divergent stretch is skipped in one
    /// round trip (Raft's fast backup).
    fn mismatch_hint(&self, prev_index: i64) -> i64 {
        if prev_index > self.len() {
            return self.len();
        }
        if prev_index <= 0 {
            return 0; // (0, x) with x != 0 never matches; start from the beginning
        }
        let conflict_epoch = self.entries[position(prev_index)].epoch;
        let mut index = prev_index;
        while index > 1 && self.entries[position(index - 1)].epoch == conflict_epoch {
            index -= 1;
        }
        index - 1
    }

    /// Drops `from_index` and everything after it, on disk first.
    fn truncate(&mut self, from_index: i64) -> io::Result<()> {
        let start = position(from_index);
        self.file.rewrite(&self.entries[..start])?;
        self.truncated += (self.entries.len() - start) as i64;
        self.entries.truncate(start);
        self.rebuild_latest();
        Ok(())
    }

    fn add(&mut self, entry: LogEntry) {
        self.latest.insert(entry.key.clone(), self.entries.len());
        self.entries.push(entry);
    }

    fn rebuild_latest(&mut self) {
        self.latest = self
            .entries
            .iter()
            .enumerate()
            .map(|(at, entry)| (entry.key.clone(), at))
            .collect();
    }
}

/// Vector position of a 1-based log index the caller has checked is in range.
fn position(index: i64) -> usize {
    usize::try_from(index - 1).unwrap_or(usize::MAX)
}

fn validate_batch(
    leader_epoch: i64,
    prev_index: i64,
    entries: &[LogEntry],
    leader_last_index: i64,
) -> Result<(), AppendError> {
    let fits = prev_index >= 0
        && prev_index
            .checked_add(entries.len() as i64)
            .is_some_and(|end| end <= leader_last_index);
    if !fits {
        return Err(AppendError::Invalid(
            "prevIndex and leaderLastIndex do not fit the batch".into(),
        ));
    }
    for (i, entry) in entries.iter().enumerate() {
        let expected = prev_index + 1 + i as i64;
        if entry.index != expected || entry.epoch < 1 || entry.epoch > leader_epoch {
            return Err(AppendError::Invalid(format!(
                "entry {i} does not continue the batch at index {expected}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    const LEADER_EPOCH: i64 = 5;

    fn open(dir: &TempDir) -> ReplicatedLog {
        ReplicatedLog::open(&dir.path().join("log.jsonl")).unwrap()
    }

    fn entry(index: i64, epoch: i64, key: &str) -> LogEntry {
        LogEntry {
            index,
            epoch,
            key: key.into(),
            value: key.into(),
        }
    }

    fn le(index: i64, epoch: i64, key: &str, value: &str) -> LogEntry {
        LogEntry {
            index,
            epoch,
            key: key.into(),
            value: value.into(),
        }
    }

    #[test]
    fn append_numbers_entries_and_they_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = open(&dir);
            assert_eq!(log.append(1, "k1", "v1").unwrap(), le(1, 1, "k1", "v1"));
            assert_eq!(log.append(2, "k1", "v2").unwrap(), le(2, 2, "k1", "v2"));
        }

        let reopened = open(&dir);

        assert_eq!(reopened.last_index(), 2);
        assert_eq!(reopened.last_epoch(), 2);
        assert_eq!(reopened.get("k1").map(|e| e.value), Some("v2".into()));
    }

    #[test]
    fn read_returns_the_batch_with_its_surroundings() {
        let dir = tempfile::tempdir().unwrap();
        let log = open(&dir);
        log.append(1, "a", "1").unwrap();
        log.append(2, "b", "2").unwrap();
        log.append(2, "c", "3").unwrap();

        let slice = log.read(2, 1).unwrap();

        assert_eq!(
            slice,
            LogSlice {
                prev_epoch: 1,
                entries: vec![le(2, 2, "b", "2")],
                last_index: 3
            }
        );
        assert!(log.read(4, 10).unwrap().entries.is_empty());
        assert_eq!(log.read(5, 10), None);
        assert_eq!(log.read(0, 10), None);
        assert_eq!(log.read(i64::MIN, 10), None);
    }

    #[test]
    fn append_from_adds_entries_that_continue_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let log = open(&dir);

        let outcome = log
            .append_from(LEADER_EPOCH, 0, 0, &[entry(1, 1, "a"), entry(2, 1, "b")], 2)
            .unwrap();

        assert_eq!(outcome, AppendOutcome::ok(2));
        assert_eq!(log.last_index(), 2);
    }

    #[test]
    fn append_from_reports_a_mismatch_when_the_previous_entry_is_missing_or_different() {
        let dir = tempfile::tempdir().unwrap();
        let log = open(&dir);
        log.append(1, "a", "1").unwrap();
        log.append(1, "b", "2").unwrap();

        assert_eq!(
            log.append_from(LEADER_EPOCH, 5, 3, &[], 5).unwrap(),
            AppendOutcome::mismatch(2)
        );
        assert_eq!(
            log.append_from(LEADER_EPOCH, 2, 2, &[], 2).unwrap(),
            AppendOutcome::mismatch(0)
        );
        assert_eq!(
            log.append_from(LEADER_EPOCH, 0, 4, &[], 2).unwrap(),
            AppendOutcome::mismatch(0)
        );
        assert_eq!(log.last_index(), 2);
    }

    #[test]
    fn append_from_mismatch_skips_the_whole_disagreeing_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let log = open(&dir);
        log.append(1, "a", "1").unwrap();
        log.append(1, "b", "2").unwrap();
        log.append(2, "x", "3").unwrap(); // written alone by a deposed primary
        log.append(2, "y", "4").unwrap();
        log.append(2, "z", "5").unwrap();

        let outcome = log.append_from(LEADER_EPOCH, 5, 3, &[], 6).unwrap();

        assert_eq!(outcome, AppendOutcome::mismatch(2));
    }

    #[test]
    fn append_from_ignores_entries_it_already_has() {
        let dir = tempfile::tempdir().unwrap();
        let log = open(&dir);
        log.append_from(LEADER_EPOCH, 0, 0, &[entry(1, 1, "a"), entry(2, 1, "b")], 2)
            .unwrap();

        let outcome = log
            .append_from(
                LEADER_EPOCH,
                0,
                0,
                &[entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")],
                3,
            )
            .unwrap();

        assert_eq!(outcome, AppendOutcome::ok(3));
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.truncated_entries(), 0);
    }

    #[test]
    fn append_from_replaces_entries_a_newer_primary_never_had() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = open(&dir);
            log.append(1, "k", "old").unwrap();
            log.append(1, "k", "unacknowledged").unwrap(); // the old primary wrote this, then crashed

            let outcome = log
                .append_from(LEADER_EPOCH, 1, 1, &[entry(2, 2, "other")], 2)
                .unwrap();

            assert_eq!(outcome, AppendOutcome::ok(2));
            assert_eq!(log.truncated_entries(), 1);
            assert_eq!(log.get("k").map(|e| e.value), Some("old".into()));
            assert_eq!(log.get("other").map(|e| e.value), Some("other".into()));
        }

        let reopened = open(&dir);
        let expected: BTreeMap<String, String> =
            [("k".into(), "old".into()), ("other".into(), "other".into())].into();
        assert_eq!(reopened.snapshot(), expected);
    }

    #[test]
    fn append_from_drops_everything_past_the_leaders_last_entry() {
        let dir = tempfile::tempdir().unwrap();
        let log = open(&dir);
        log.append(1, "a", "1").unwrap();
        log.append(1, "stale", "2").unwrap();
        log.append(1, "stale2", "3").unwrap();

        let outcome = log.append_from(LEADER_EPOCH, 1, 1, &[], 1).unwrap();

        assert_eq!(outcome, AppendOutcome::ok(1));
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.truncated_entries(), 2);
        assert_eq!(log.get("stale"), None);
    }

    #[test]
    fn append_from_late_request_from_the_same_epoch_drops_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let log = open(&dir);
        log.append_from(
            3,
            0,
            0,
            &[entry(1, 3, "a"), entry(2, 3, "b"), entry(3, 3, "c")],
            3,
        )
        .unwrap();

        // A heartbeat the primary sent when its log ended at 1, arriving after the batch above.
        let outcome = log.append_from(3, 1, 3, &[], 1).unwrap();

        assert_eq!(outcome, AppendOutcome::ok(1));
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.truncated_entries(), 0);
    }

    #[test]
    fn append_from_keeps_entries_past_a_partial_batch() {
        let dir = tempfile::tempdir().unwrap();
        let log = open(&dir);
        log.append_from(
            LEADER_EPOCH,
            0,
            0,
            &[entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")],
            3,
        )
        .unwrap();

        let outcome = log
            .append_from(LEADER_EPOCH, 0, 0, &[entry(1, 1, "a")], 3)
            .unwrap();

        assert_eq!(outcome, AppendOutcome::ok(1));
        assert_eq!(log.last_index(), 3);
    }

    #[test]
    fn append_from_rejects_batches_that_do_not_fit_together() {
        let dir = tempfile::tempdir().unwrap();
        let log = open(&dir);
        let cases: [(i64, i64, Vec<LogEntry>, i64); 6] = [
            (LEADER_EPOCH, 0, vec![entry(2, 1, "a")], 2),
            (LEADER_EPOCH, 0, vec![entry(1, 0, "a")], 1),
            (LEADER_EPOCH, 0, vec![entry(1, 1, "a")], 0),
            (LEADER_EPOCH, -1, vec![], 0),
            (1, 0, vec![entry(1, 2, "a")], 1),
            (
                LEADER_EPOCH,
                i64::MAX - 1,
                vec![entry(i64::MAX, 1, "a"), entry(i64::MAX, 1, "b")],
                i64::MAX,
            ),
        ];

        for (leader_epoch, prev_index, entries, leader_last_index) in cases {
            let result = log.append_from(leader_epoch, prev_index, 0, &entries, leader_last_index);
            assert!(
                matches!(result, Err(AppendError::Invalid(_))),
                "{prev_index} {entries:?}"
            );
        }
        assert_eq!(log.last_index(), 0);
    }

    #[test]
    fn open_drops_a_partial_last_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.jsonl");
        fs::write(
            &path,
            "{\"index\":1,\"epoch\":1,\"key\":\"a\",\"value\":\"1\"}\n{\"index\":2,\"ep",
        )
        .unwrap();
        {
            let log = ReplicatedLog::open(&path).unwrap();
            assert_eq!(log.last_index(), 1);
            log.append(1, "b", "2").unwrap();
        }

        assert_eq!(ReplicatedLog::open(&path).unwrap().last_index(), 2);
    }

    #[test]
    fn open_refuses_corruption_before_the_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.jsonl");
        fs::write(
            &path,
            "garbage\n{\"index\":1,\"epoch\":1,\"key\":\"a\",\"value\":\"1\"}\n",
        )
        .unwrap();

        assert!(matches!(
            ReplicatedLog::open(&path),
            Err(LogFileError::Corrupt { line: 1, .. })
        ));
    }

    #[test]
    fn errors_describe_themselves() {
        assert_eq!(AppendError::Invalid("bad".into()).to_string(), "bad");
        assert!(
            AppendError::from(io::Error::other("disk"))
                .to_string()
                .contains("disk")
        );
    }
}
