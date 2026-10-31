"""The virtual waiting room: take a number, poll, get let in when a slot frees up."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .config import Config
from .ids import new_id
from .sale_redis import SaleRedis, ScriptReply
from .wire import parse_status, without_nulls

# waiting: in line, keep polling. admitted: may check out until admissionExpiresAtMs.
# ordered: placed an order (orderId). expired: admitted but did not order in time; may join again.
# abandoned: stopped polling and was skipped; may join again. sold_out: nothing left, final.
TICKET_STATUSES = frozenset({"waiting", "admitted", "ordered", "expired", "abandoned", "sold_out"})
FINAL_STATUSES = frozenset({"ordered", "expired", "abandoned", "sold_out"})

# Upper bound on tickets one admission pass looks at, so a long run of no-shows cannot stall Redis.
MAX_SCAN_PER_TICK = 1_000
# Wait estimates use the admission rate over this many most recent seconds.
ADMIT_RATE_WINDOW_SECONDS = 10
# Tickets per watch.lua call, so one call never blocks Redis for long.
MAX_WATCH_BATCH = 500
# Below this many admissions per second the estimate would be noise (or hours), so none is given.
MIN_ADMIT_RATE_PER_SECOND = 0.01


@dataclass(frozen=True, slots=True)
class TicketView:
    """What a client sees when it polls its ticket."""

    ticket_id: str
    user_id: str
    seq: int
    status: str
    position: int | None
    estimated_wait_seconds: int | None
    admitted_at_ms: int | None
    admission_expires_at_ms: int | None
    order_id: str | None
    poll_after_ms: int

    @classmethod
    def from_reply(cls, reply: ScriptReply, config: Config) -> TicketView:
        position = reply.optional_int("position")
        return cls(
            ticket_id=reply.get("ticketId"),
            user_id=reply.get("userId"),
            seq=reply.get_int("seq"),
            status=parse_status(reply.get("status"), TICKET_STATUSES),
            position=position,
            estimated_wait_seconds=_estimate(position, reply.find("admitRate")),
            admitted_at_ms=reply.optional_int("admittedAt"),
            admission_expires_at_ms=reply.optional_int("admissionExpiresAt"),
            order_id=reply.find("orderId"),
            poll_after_ms=config.poll_interval_ms,
        )

    @property
    def is_final(self) -> bool:
        return self.status in FINAL_STATUSES

    def to_json(self) -> dict[str, Any]:
        return without_nulls(
            ticketId=self.ticket_id,
            userId=self.user_id,
            seq=self.seq,
            status=self.status,
            position=self.position,
            estimatedWaitSeconds=self.estimated_wait_seconds,
            admittedAtMs=self.admitted_at_ms,
            admissionExpiresAtMs=self.admission_expires_at_ms,
            orderId=self.order_id,
            pollAfterMs=self.poll_after_ms,
        )


@dataclass(frozen=True, slots=True)
class JoinResult:
    outcome: str  # created | existing | sold_out
    ticket: TicketView | None


@dataclass(frozen=True, slots=True)
class AdmitResult:
    admitted: int
    abandoned: int
    expired: int


class QueueStore:
    def __init__(self, redis: SaleRedis, config: Config) -> None:
        self._redis = redis
        self._config = config

    async def join(self, user_id: str) -> JoinResult:
        reply = await self._redis.run("join", user_id, new_id(), self._redis.now_ms)
        if reply.outcome in ("created", "existing"):
            return JoinResult(reply.outcome, TicketView.from_reply(reply, self._config))
        if reply.outcome == "sold_out":
            return JoinResult("sold_out", None)
        raise reply.unexpected()

    async def get_ticket(self, ticket_id: str) -> TicketView | None:
        """Current view of a ticket. Also counts as the ticket's heartbeat."""
        reply = await self._redis.run("ticket", ticket_id, self._redis.now_ms)
        if reply.outcome == "ok":
            return TicketView.from_reply(reply, self._config)
        if reply.outcome == "not_found":
            return None
        raise reply.unexpected()

    async def watch(self, ticket_ids: Sequence[str]) -> list[TicketView]:
        """Views of many tickets in a few round trips, for event streams. Counts as their heartbeat."""
        views: list[TicketView] = []
        for start in range(0, len(ticket_ids), MAX_WATCH_BATCH):
            batch = ticket_ids[start : start + MAX_WATCH_BATCH]
            rows = await self._redis.run_rows("watch", self._redis.now_ms, *batch)
            views.extend(TicketView.from_reply(row, self._config) for row in rows)
        return views

    async def admit(self) -> AdmitResult:
        """One admission pass: drop lapsed admissions, then let in as many as there are free slots."""
        config = self._config
        reply = await self._redis.run(
            "admit",
            self._redis.now_ms,
            config.max_active,
            config.admission_window_ms,
            config.heartbeat_timeout_ms,
            MAX_SCAN_PER_TICK,
            ADMIT_RATE_WINDOW_SECONDS,
        )
        return AdmitResult(reply.get_int("admitted"), reply.get_int("abandoned"), reply.get_int("expired"))


def _estimate(position: int | None, admit_rate: str | None) -> int | None:
    if position is None or admit_rate is None:
        return None
    rate = float(admit_rate)
    if not math.isfinite(rate) or rate < MIN_ADMIT_RATE_PER_SECOND:
        return None
    return math.ceil(position / rate)
