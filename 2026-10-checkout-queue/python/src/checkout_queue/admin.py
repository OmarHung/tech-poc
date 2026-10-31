"""Admin views: the audit the PoC is judged on, the sale rules, and the board every demo tab draws."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any

from .config import Config
from .orders import OrderView
from .sale_redis import SaleRedis
from .tickets import TicketView

# Enough for a hand-driven demo; a real line would page through it instead.
MAX_BOARD_ENTRIES = 300


@dataclass(frozen=True, slots=True)
class AuditReport:
    """Recounted from the orders themselves rather than from running counters, so a bug in the
    counting cannot hide a bug in the selling."""

    initial_stock: int
    stock: int
    pending_orders: int
    pending_quantity: int
    paid_orders: int
    paid_quantity: int
    expired_orders: int
    expired_quantity: int
    charges: int
    duplicate_charges: int
    users_with_multiple_live_orders: int
    tickets: int
    waiting: int
    active: int

    @property
    def oversold(self) -> int:
        """Units promised to buyers beyond what existed. Must be 0."""
        return max(0, self.pending_quantity + self.paid_quantity - self.initial_stock)

    @property
    def stock_balanced(self) -> bool:
        """Every unit is on the shelf, held by an unpaid order, or sold. Nothing lost, nothing made up."""
        return self.stock >= 0 and self.stock + self.pending_quantity + self.paid_quantity == self.initial_stock

    def to_json(self) -> dict[str, Any]:
        counts = {_camel(f.name): getattr(self, f.name) for f in fields(self)}
        return {**counts, "oversold": self.oversold, "stockBalanced": self.stock_balanced}


class AuditStore:
    def __init__(self, redis: SaleRedis) -> None:
        self._redis = redis

    async def get(self) -> AuditReport:
        reply = await self._redis.run("audit")
        return AuditReport(**{f.name: reply.get_int(_camel(f.name)) for f in fields(AuditReport)})


@dataclass(frozen=True, slots=True)
class BoardEntry:
    ticket: TicketView
    order: OrderView | None

    def to_json(self) -> dict[str, Any]:
        entry: dict[str, Any] = {"ticket": self.ticket.to_json()}
        if self.order is not None:
            entry["order"] = self.order.to_json()
        return entry


class TicketBoardStore:
    """Every user's current ticket and order, newest first. The demo page draws its lanes from
    this, so every tab on every instance shows the same line. Not a heartbeat."""

    def __init__(self, redis: SaleRedis, config: Config) -> None:
        self._redis = redis
        self._config = config

    async def list(self) -> list[BoardEntry]:
        rows = await self._redis.run_rows("tickets", self._redis.now_ms, MAX_BOARD_ENTRIES)
        entries: list[BoardEntry] = []
        for row in rows:
            if row.outcome == "ticket":
                entries.append(BoardEntry(TicketView.from_reply(row, self._config), None))
            elif row.outcome == "order" and entries:
                entries[-1] = BoardEntry(entries[-1].ticket, OrderView.from_reply(row))
            else:
                raise row.unexpected()
        return entries


def sale_settings(config: Config) -> dict[str, Any]:
    """The sale rules a client needs to draw the line and its countdowns. Deliberately leaves out
    the Redis address, which may carry a password."""
    return {
        "instance": config.instance_name,
        "saleId": config.sale_id,
        "maxActive": config.max_active,
        "maxPerOrder": config.max_per_order,
        "admissionWindowMs": config.admission_window_ms,
        "paymentWindowMs": config.payment_window_ms,
        "heartbeatTimeoutMs": config.heartbeat_timeout_ms,
        "pollIntervalMs": config.poll_interval_ms,
        "tickMs": config.tick_ms,
    }


def _camel(snake: str) -> str:
    head, *rest = snake.split("_")
    return head + "".join(part.title() for part in rest)
