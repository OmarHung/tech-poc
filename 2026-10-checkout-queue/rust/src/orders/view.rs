use serde::Serialize;

use crate::sale_redis::{ScriptReply, StoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Stock is held until `payment_expires_at_ms`.
    PendingPayment,
    Paid,
    /// Not paid in time; the stock went back on the shelf.
    Expired,
}

impl OrderStatus {
    pub fn parse(wire: &str) -> Result<Self, StoreError> {
        match wire {
            "pending_payment" => Ok(Self::PendingPayment),
            "paid" => Ok(Self::Paid),
            "expired" => Ok(Self::Expired),
            other => Err(StoreError::Unexpected(format!(
                "unknown order status '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderView {
    pub order_id: String,
    pub ticket_id: String,
    pub user_id: String,
    pub quantity: i64,
    pub status: OrderStatus,
    pub created_at_ms: i64,
    pub payment_expires_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paid_at_ms: Option<i64>,
}

impl OrderView {
    pub fn from_reply(reply: &ScriptReply) -> Result<Self, StoreError> {
        Ok(Self {
            order_id: reply.get("orderId")?.to_owned(),
            ticket_id: reply.get("ticketId")?.to_owned(),
            user_id: reply.get("userId")?.to_owned(),
            quantity: reply.int("quantity")?,
            status: OrderStatus::parse(reply.get("status")?)?,
            created_at_ms: reply.int("createdAt")?,
            payment_expires_at_ms: reply.int("paymentExpiresAt")?,
            paid_at_ms: reply.optional_int("paidAt")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use redis::Value;
    use serde_json::json;

    use super::*;

    #[test]
    fn serializes_camel_case_and_leaves_out_an_unpaid_paid_at() {
        let fields = [
            ("orderId", "o1"),
            ("ticketId", "t1"),
            ("userId", "alice"),
            ("quantity", "2"),
            ("status", "pending_payment"),
            ("createdAt", "100"),
            ("paymentExpiresAt", "200"),
        ];
        let mut items = vec![Value::BulkString(b"ok".to_vec())];
        for (name, value) in fields {
            items.push(Value::BulkString(name.as_bytes().to_vec()));
            items.push(Value::BulkString(value.as_bytes().to_vec()));
        }
        let reply = ScriptReply::parse("order", &Value::Array(items)).unwrap();

        let view = OrderView::from_reply(&reply).unwrap();

        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            json!({
                "orderId": "o1", "ticketId": "t1", "userId": "alice", "quantity": 2,
                "status": "pending_payment", "createdAtMs": 100, "paymentExpiresAtMs": 200
            })
        );
    }

    #[test]
    fn statuses_round_trip_and_unknown_ones_are_bugs() {
        for wire in ["pending_payment", "paid", "expired"] {
            let status = OrderStatus::parse(wire).unwrap();
            assert_eq!(serde_json::to_value(status).unwrap(), json!(wire));
        }
        assert!(OrderStatus::parse("refunded").is_err());
    }
}
