"""Fires many requests at once. They run on separate pooled connections, so they interleave on
Redis exactly like requests coming from several app instances."""

import asyncio
from collections import Counter
from dataclasses import replace

import pytest
from redis.asyncio import Redis

from checkout_queue.tickets import AdmitResult
from support import SaleUnderTest

pytestmark = pytest.mark.anyio

CLICKS_PER_USER = 3


def users(count: int) -> list[str]:
    return [f"user-{i}" for i in range(count)]


async def test_parallel_checkouts_never_oversell_nor_order_twice_per_ticket(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, initial_stock=100, max_active=100))
    tickets = await sale.join_and_admit(*users(100))
    assert all(t.status == "admitted" for t in tickets)

    # 100 admitted users each want 2 of the 100 left and click three times: 300 requests at once.
    attempts = [t.ticket_id for t in tickets for _ in range(CLICKS_PER_USER)]
    results = await asyncio.gather(*(sale.orders.checkout(ticket_id, 2) for ticket_id in attempts))

    orders = [r.order for r in results if r.order is not None]
    assert len({o.order_id for o in orders}) == 50
    assert sum(r.outcome == "created" for r in results) == 50
    per_ticket: dict[str, set[str]] = {}
    for order in orders:
        per_ticket.setdefault(order.ticket_id, set()).add(order.order_id)
    assert all(len(ids) == 1 for ids in per_ticket.values())
    audit = await sale.audit.get()
    assert (audit.stock, audit.oversold, audit.pending_quantity) == (0, 0, 100)
    assert audit.stock_balanced
    assert audit.users_with_multiple_live_orders == 0


async def test_parallel_payments_charge_each_order_once(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, initial_stock=20, max_active=10))
    tickets = await sale.join_and_admit(*users(10))
    orders = await asyncio.gather(*(sale.orders.checkout(t.ticket_id, 1) for t in tickets))

    payments = [o.order.order_id for o in orders for _ in range(10)]
    results = await asyncio.gather(*(sale.orders.pay(order_id) for order_id in payments))

    assert Counter(r.outcome for r in results) == {"paid": 10, "already_paid": 90}
    audit = await sale.audit.get()
    assert (audit.charges, audit.duplicate_charges, audit.paid_orders) == (10, 0, 10)


async def test_parallel_joins_from_the_same_user_yield_one_ticket(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)

    results = await asyncio.gather(*(sale.queue.join("refresher") for _ in range(20)))

    assert sum(r.outcome == "created" for r in results) == 1
    assert len({r.ticket.ticket_id for r in results}) == 1
    assert (await sale.audit.get()).tickets == 1


async def test_parallel_admitters_never_exceed_max_active(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, initial_stock=100, max_active=5))
    for user in users(50):
        await sale.queue.join(user)

    # Every app instance runs its own admitter; ten of them tick at the same moment.
    results = await asyncio.gather(*(sale.queue.admit() for _ in range(10)))

    assert sum(r.admitted for r in results) == 5
    audit = await sale.audit.get()
    assert (audit.active, audit.waiting) == (5, 45)


async def test_payments_racing_the_expiry_sweep_return_each_orders_stock_once(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, max_active=10))
    tickets = await sale.join_and_admit(*users(10))
    orders = await asyncio.gather(*(sale.orders.checkout(t.ticket_id, 1) for t in tickets))
    sale.clock.advance(120_000)

    late = [sale.orders.pay(o.order.order_id) for o in orders for _ in range(CLICKS_PER_USER)]
    sweeps = [sale.maintenance.run_once() for _ in range(CLICKS_PER_USER)]
    await asyncio.gather(*late, *sweeps)

    audit = await sale.audit.get()
    assert (audit.stock, audit.expired_orders, audit.charges) == (10, 10, 0)
    assert audit.stock_balanced


async def test_admitter_with_a_clock_slightly_behind_still_completes_its_pass(redis: Redis) -> None:
    ahead = await SaleUnderTest.start(redis)
    behind = await SaleUnderTest.start(redis, lambda _: ahead.config)
    await ahead.queue.join("first")
    await ahead.queue.join("second")

    # "ahead" crosses into the next second first and marks it as when the line formed.
    ahead.clock.advance(1_000)
    behind.clock.advance(995)
    await ahead.queue.admit()
    result = await behind.queue.admit()

    assert result == AdmitResult(admitted=0, abandoned=0, expired=0)
