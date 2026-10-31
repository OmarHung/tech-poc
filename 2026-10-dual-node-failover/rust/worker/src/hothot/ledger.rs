use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use shared::work::LoggedEvent;

/// What a hot-hot worker has computed so far: running balances per account, and the last event it
/// applied. It depends only on the events and their order, so two workers that saw the same events
/// hold the same ledger. It is also the checkpoint file and the body of `GET /state`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ledger {
    pub offset: i64,
    pub balances: BTreeMap<String, i64>,
}

/// An event that does not come right after the ledger's position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapError {
    pub seq: i64,
    pub offset: i64,
}

impl fmt::Display for GapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "event {} does not follow {}", self.seq, self.offset)
    }
}

impl std::error::Error for GapError {}

impl Ledger {
    /// Applies the next event and returns the new ledger with the account's balance after it.
    pub fn apply(&self, logged: &LoggedEvent) -> Result<(Self, i64), GapError> {
        if Some(logged.seq) != self.offset.checked_add(1) {
            return Err(GapError {
                seq: logged.seq,
                offset: self.offset,
            });
        }
        // Wraps instead of panicking, like .NET's unchecked arithmetic; 9e9 events of the largest amount away.
        let balance = self
            .balances
            .get(&logged.account)
            .copied()
            .unwrap_or(0)
            .wrapping_add(logged.amount);
        let mut balances = self.balances.clone();
        balances.insert(logged.account.clone(), balance);
        let next = Self {
            offset: logged.seq,
            balances,
        };
        Ok((next, balance))
    }

    /// Moves the position without applying the events in between: whatever they changed is now missing.
    pub fn skip_to(&self, offset: i64) -> Self {
        Self {
            offset,
            balances: self.balances.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(seq: i64, account: &str, amount: i64) -> LoggedEvent {
        LoggedEvent {
            seq,
            account: account.into(),
            amount,
        }
    }

    #[test]
    fn apply_keeps_running_balances_in_order() {
        let empty = Ledger::default();

        let (first, b1) = empty.apply(&event(1, "x", 10)).unwrap();
        let (second, b2) = first.apply(&event(2, "y", 3)).unwrap();
        let (third, b3) = second.apply(&event(3, "x", -4)).unwrap();

        assert_eq!((b1, b2, b3), (10, 3, 6));
        assert_eq!(third.offset, 3);
        assert_eq!(
            third.balances,
            BTreeMap::from([("x".into(), 6), ("y".into(), 3)])
        );
        assert_eq!(empty, Ledger::default()); // applying returns a new ledger
    }

    #[test]
    fn apply_refuses_a_gap_or_a_repeat() {
        let (ledger, _) = Ledger::default().apply(&event(1, "x", 1)).unwrap();

        for seq in [3, 1, 0] {
            let error = ledger.apply(&event(seq, "x", 1)).unwrap_err();
            assert_eq!(error.to_string(), format!("event {seq} does not follow 1"));
        }
    }

    #[test]
    fn skip_to_moves_the_position_but_keeps_the_old_balances() {
        let (ledger, _) = Ledger::default().apply(&event(1, "x", 10)).unwrap();

        let skipped = ledger.skip_to(50);

        assert_eq!(skipped.offset, 50);
        assert_eq!(skipped.balances["x"], 10);
        assert_eq!(skipped.apply(&event(51, "x", 50)).unwrap().1, 60);
    }

    #[test]
    fn the_checkpoint_round_trips_as_json() {
        let (ledger, _) = Ledger::default().apply(&event(1, "x", 10)).unwrap();

        let json = serde_json::to_string(&ledger).unwrap();

        assert_eq!(json, r#"{"offset":1,"balances":{"x":10}}"#);
        assert_eq!(serde_json::from_str::<Ledger>(&json).unwrap(), ledger);
        assert!(serde_json::from_str::<Ledger>(r#"{"offset":1}"#).is_err());
    }
}
