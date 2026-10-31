use serde::Serialize;

use crate::sale_redis::{ScriptReply, StoreError};

/// Below this many admissions per second the estimate would be noise (or hours), so none is given.
const MIN_ADMIT_RATE_PER_SECOND: f64 = 0.01;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TicketStatus {
    /// In line. Keep polling; position and estimate tell how far.
    Waiting,
    /// Allowed to check out until `admission_expires_at_ms`.
    Admitted,
    /// Placed an order; see `order_id`.
    Ordered,
    /// Was admitted but did not order in time. May join again, at the back.
    Expired,
    /// Stopped polling and was skipped. May join again, at the back.
    Abandoned,
    /// Nothing left to sell and nothing held by unpaid orders. Final: stop polling.
    SoldOut,
}

impl TicketStatus {
    pub fn parse(wire: &str) -> Result<Self, StoreError> {
        match wire {
            "waiting" => Ok(Self::Waiting),
            "admitted" => Ok(Self::Admitted),
            "ordered" => Ok(Self::Ordered),
            "expired" => Ok(Self::Expired),
            "abandoned" => Ok(Self::Abandoned),
            "sold_out" => Ok(Self::SoldOut),
            other => Err(StoreError::Unexpected(format!(
                "unknown ticket status '{other}'"
            ))),
        }
    }

    /// The ticket can no longer change, so an event stream for it can end.
    pub fn is_final(self) -> bool {
        match self {
            Self::Ordered | Self::Expired | Self::Abandoned | Self::SoldOut => true,
            Self::Waiting | Self::Admitted => false,
        }
    }
}

/// What a client sees when it polls its ticket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TicketView {
    pub ticket_id: String,
    pub user_id: String,
    pub seq: i64,
    pub status: TicketStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_wait_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admitted_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admission_expires_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    pub poll_after_ms: u64,
}

impl TicketView {
    pub fn from_reply(reply: &ScriptReply, poll_after_ms: u64) -> Result<Self, StoreError> {
        let position = reply.optional_int("position")?;
        Ok(Self {
            ticket_id: reply.get("ticketId")?.to_owned(),
            user_id: reply.get("userId")?.to_owned(),
            seq: reply.int("seq")?,
            status: TicketStatus::parse(reply.get("status")?)?,
            position,
            estimated_wait_seconds: estimate(position, reply.find("admitRate"))?,
            admitted_at_ms: reply.optional_int("admittedAt")?,
            admission_expires_at_ms: reply.optional_int("admissionExpiresAt")?,
            order_id: reply.find("orderId").map(str::to_owned),
            poll_after_ms,
        })
    }
}

/// Seconds until this position reaches the front at the recent admission rate.
fn estimate(position: Option<i64>, admit_rate: Option<&str>) -> Result<Option<i64>, StoreError> {
    let (Some(ahead), Some(rate)) = (position, admit_rate) else {
        return Ok(None);
    };
    let rate: f64 = rate
        .parse()
        .map_err(|_| StoreError::Unexpected(format!("admitRate is not a number: '{rate}'")))?;
    if !rate.is_finite() || rate < MIN_ADMIT_RATE_PER_SECOND {
        return Ok(None);
    }
    // Positions are small integers, so the f64 round trip is exact.
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    let seconds = (ahead as f64 / rate).ceil() as i64;
    Ok(Some(seconds))
}

#[cfg(test)]
mod tests {
    use redis::Value;
    use serde_json::json;

    use super::*;

    fn reply(pairs: &[(&str, &str)]) -> ScriptReply {
        let mut items = vec![Value::BulkString(b"ok".to_vec())];
        for (name, value) in pairs {
            items.push(Value::BulkString(name.as_bytes().to_vec()));
            items.push(Value::BulkString(value.as_bytes().to_vec()));
        }
        ScriptReply::parse("ticket", &Value::Array(items)).unwrap()
    }

    #[test]
    fn waiting_ticket_serializes_camel_case_without_empty_fields() {
        let view = TicketView::from_reply(
            &reply(&[
                ("ticketId", "t1"),
                ("userId", "alice"),
                ("seq", "3"),
                ("status", "waiting"),
                ("position", "10"),
                ("admitRate", "2"),
            ]),
            1000,
        )
        .unwrap();

        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            json!({
                "ticketId": "t1", "userId": "alice", "seq": 3, "status": "waiting",
                "position": 10, "estimatedWaitSeconds": 5, "pollAfterMs": 1000
            })
        );
    }

    #[test]
    fn admitted_ticket_carries_its_deadline() {
        let view = TicketView::from_reply(
            &reply(&[
                ("ticketId", "t1"),
                ("userId", "alice"),
                ("seq", "1"),
                ("status", "sold_out"),
                ("admittedAt", "100"),
                ("admissionExpiresAt", "200"),
                ("orderId", "o1"),
            ]),
            500,
        )
        .unwrap();

        assert_eq!(view.status, TicketStatus::SoldOut);
        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            json!({
                "ticketId": "t1", "userId": "alice", "seq": 1, "status": "sold_out",
                "admittedAtMs": 100, "admissionExpiresAtMs": 200, "orderId": "o1", "pollAfterMs": 500
            })
        );
    }

    #[test]
    fn estimate_rounds_up_and_needs_a_meaningful_rate() {
        assert_eq!(estimate(Some(3), Some("2")).unwrap(), Some(2));
        assert_eq!(
            estimate(Some(1), Some("0.33333333333333")).unwrap(),
            Some(4)
        );
        assert_eq!(estimate(Some(5), Some("0")).unwrap(), None);
        assert_eq!(estimate(Some(5), Some("0.009")).unwrap(), None);
        assert_eq!(estimate(Some(5), Some("inf")).unwrap(), None);
        assert_eq!(estimate(Some(5), Some("nan")).unwrap(), None);
        assert_eq!(estimate(Some(5), None).unwrap(), None);
        assert_eq!(estimate(None, Some("2")).unwrap(), None);
        assert!(estimate(Some(5), Some("fast")).is_err());
    }

    #[test]
    fn statuses_round_trip_and_unknown_ones_are_bugs() {
        for wire in [
            "waiting",
            "admitted",
            "ordered",
            "expired",
            "abandoned",
            "sold_out",
        ] {
            let status = TicketStatus::parse(wire).unwrap();
            assert_eq!(serde_json::to_value(status).unwrap(), json!(wire));
        }
        assert!(TicketStatus::parse("Waiting").is_err());
        assert!(TicketStatus::Ordered.is_final() && TicketStatus::SoldOut.is_final());
        assert!(!TicketStatus::Waiting.is_final() && !TicketStatus::Admitted.is_final());
    }
}
