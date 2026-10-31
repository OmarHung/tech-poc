"""Reserving stock for admitted users, taking (simulated) payment, and releasing what goes unpaid."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import Config
from .ids import new_id
from .sale_redis import SaleRedis, ScriptReply
from .wire import parse_status, without_nulls

# pending_payment: stock is held until paymentExpiresAtMs. expired: not paid in time, stock went back.
ORDER_STATUSES = frozenset({"pending_payment", "paid", "expired"})

# created: new order. existing: this ticket already ordered the same quantity, replayed.
# ticket_not_found. not_admitted: detail holds the ticket status. admission_expired.
# ticket_used: this ticket already ordered a different quantity, detail holds that order id.
# sold_out: nothing on the shelf and nothing held by unpaid orders, final.
# insufficient_stock: less than asked is on the shelf right now (detail), possibly 0 while unpaid
#   orders hold the rest; retrying while admitted may succeed.
CHECKOUT_OUTCOMES = frozenset({"created", "existing"})
CHECKOUT_REJECTIONS = frozenset({"ticket_not_found", "admission_expired", "sold_out"})
CHECKOUT_REJECTIONS_WITH_DETAIL = frozenset({"not_admitted", "ticket_used", "insufficient_stock"})

# paid: charged now. already_paid: charged by an earlier request, not again. order_expired. order_not_found.
PAY_OUTCOMES_WITH_ORDER = frozenset({"paid", "already_paid", "order_expired"})

# Upper bound on orders one sweep releases, so a mass expiry cannot stall Redis.
MAX_RELEASE_PER_TICK = 1_000


@dataclass(frozen=True, slots=True)
class OrderView:
    order_id: str
    ticket_id: str
    user_id: str
    quantity: int
    status: str
    created_at_ms: int
    payment_expires_at_ms: int
    paid_at_ms: int | None

    @classmethod
    def from_reply(cls, reply: ScriptReply) -> OrderView:
        return cls(
            order_id=reply.get("orderId"),
            ticket_id=reply.get("ticketId"),
            user_id=reply.get("userId"),
            quantity=reply.get_int("quantity"),
            status=parse_status(reply.get("status"), ORDER_STATUSES),
            created_at_ms=reply.get_int("createdAt"),
            payment_expires_at_ms=reply.get_int("paymentExpiresAt"),
            paid_at_ms=reply.optional_int("paidAt"),
        )

    def to_json(self) -> dict[str, Any]:
        return without_nulls(
            orderId=self.order_id,
            ticketId=self.ticket_id,
            userId=self.user_id,
            quantity=self.quantity,
            status=self.status,
            createdAtMs=self.created_at_ms,
            paymentExpiresAtMs=self.payment_expires_at_ms,
            paidAtMs=self.paid_at_ms,
        )


@dataclass(frozen=True, slots=True)
class CheckoutResult:
    outcome: str
    order: OrderView | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class PayResult:
    outcome: str
    order: OrderView | None = None


class OrderStore:
    def __init__(self, redis: SaleRedis, config: Config) -> None:
        self._redis = redis
        self._config = config

    async def checkout(self, ticket_id: str, quantity: int) -> CheckoutResult:
        reply = await self._redis.run(
            "checkout", ticket_id, quantity, new_id(), self._redis.now_ms, self._config.payment_window_ms
        )
        if reply.outcome in CHECKOUT_OUTCOMES:
            return CheckoutResult(reply.outcome, order=OrderView.from_reply(reply))
        if reply.outcome in CHECKOUT_REJECTIONS:
            return CheckoutResult(reply.outcome)
        if reply.outcome in CHECKOUT_REJECTIONS_WITH_DETAIL:
            return CheckoutResult(reply.outcome, detail=reply.get("detail"))
        raise reply.unexpected()

    async def get_order(self, order_id: str) -> OrderView | None:
        reply = await self._redis.run("order", order_id, self._redis.now_ms)
        if reply.outcome == "ok":
            return OrderView.from_reply(reply)
        if reply.outcome == "not_found":
            return None
        raise reply.unexpected()

    async def pay(self, order_id: str) -> PayResult:
        reply = await self._redis.run("pay", order_id, self._redis.now_ms)
        if reply.outcome in PAY_OUTCOMES_WITH_ORDER:
            return PayResult(reply.outcome, OrderView.from_reply(reply))
        if reply.outcome == "order_not_found":
            return PayResult(reply.outcome)
        raise reply.unexpected()

    async def expire_orders(self) -> int:
        """Puts the stock of unpaid, overdue orders back. Returns how many orders were released."""
        reply = await self._redis.run("expire_orders", self._redis.now_ms, MAX_RELEASE_PER_TICK)
        return reply.get_int("released")
