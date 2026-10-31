from dataclasses import replace

import pytest
from redis.asyncio import Redis

from checkout_queue.ids import new_id
from support import SaleUnderTest

pytestmark = pytest.mark.anyio


async def test_checkout_before_admission_is_rejected(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    ticket = (await sale.queue.join("eager")).ticket

    result = await sale.orders.checkout(ticket.ticket_id, 1)

    assert (result.outcome, result.detail) == ("not_admitted", "waiting")


async def test_checkout_reserves_stock_and_starts_the_payment_clock(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    ticket = (await sale.join_and_admit("alice"))[0]

    result = await sale.orders.checkout(ticket.ticket_id, 2)

    assert result.outcome == "created"
    order = result.order
    assert (order.ticket_id, order.user_id, order.quantity, order.status) == (
        ticket.ticket_id,
        "alice",
        2,
        "pending_payment",
    )
    assert order.payment_expires_at_ms == sale.now_ms + 120_000
    assert order.paid_at_ms is None
    assert (await sale.audit.get()).stock == 8
    after = await sale.queue.get_ticket(ticket.ticket_id)
    assert (after.status, after.order_id) == ("ordered", order.order_id)


async def test_checkout_repeated_returns_the_same_order_and_reserves_once(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    ticket = (await sale.join_and_admit("double-clicker"))[0]

    first = await sale.orders.checkout(ticket.ticket_id, 2)
    second = await sale.orders.checkout(ticket.ticket_id, 2)

    assert second.outcome == "existing"
    assert second.order.order_id == first.order.order_id
    assert (await sale.audit.get()).stock == 8


async def test_checkout_repeated_with_a_different_quantity_is_rejected(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    ticket = (await sale.join_and_admit("indecisive"))[0]
    first = await sale.orders.checkout(ticket.ticket_id, 1)

    second = await sale.orders.checkout(ticket.ticket_id, 2)

    assert (second.outcome, second.detail) == ("ticket_used", first.order.order_id)
    assert (await sale.audit.get()).stock == 9


async def test_checkout_more_than_is_left_is_rejected_and_keeps_the_admission(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, initial_stock=1))
    ticket = (await sale.join_and_admit("alice"))[0]

    too_many = await sale.orders.checkout(ticket.ticket_id, 2)
    retry = await sale.orders.checkout(ticket.ticket_id, 1)

    assert (too_many.outcome, too_many.detail) == ("insufficient_stock", "1")
    assert retry.outcome == "created"


async def test_checkout_while_unpaid_orders_hold_the_rest_is_not_sold_out_yet(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, initial_stock=2))
    first, second = await sale.join_and_admit("first", "second")
    held = (await sale.orders.checkout(first.ticket_id, 2)).order

    while_held = await sale.orders.checkout(second.ticket_id, 1)
    await sale.orders.pay(held.order_id)
    after_paid = await sale.orders.checkout(second.ticket_id, 1)

    # "sold_out" is final everywhere; while stock may still come back it is only "insufficient".
    assert (while_held.outcome, while_held.detail) == ("insufficient_stock", "0")
    assert after_paid.outcome == "sold_out"
    assert (await sale.queue.get_ticket(second.ticket_id)).status == "sold_out"


async def test_checkout_after_the_admission_window_is_rejected(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    ticket = (await sale.join_and_admit("slow"))[0]
    sale.clock.advance(60_000)

    result = await sale.orders.checkout(ticket.ticket_id, 1)

    assert result.outcome == "admission_expired"
    audit = await sale.audit.get()
    assert (audit.active, audit.stock) == (0, 10)


async def test_checkout_unknown_ticket_is_not_found(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)

    result = await sale.orders.checkout(new_id(), 1)

    assert result.outcome == "ticket_not_found"


async def test_pay_charges_once_even_when_repeated(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    order = await sale.place_order("payer", quantity=2)

    first = await sale.orders.pay(order.order_id)
    second = await sale.orders.pay(order.order_id)

    assert first.outcome == "paid"
    assert (first.order.status, first.order.paid_at_ms) == ("paid", sale.now_ms)
    assert second.outcome == "already_paid"
    audit = await sale.audit.get()
    assert (audit.charges, audit.duplicate_charges, audit.paid_orders, audit.paid_quantity) == (1, 0, 1, 2)


async def test_pay_after_the_deadline_expires_the_order_and_returns_the_stock(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    order = await sale.place_order("late", quantity=2)
    sale.clock.advance(120_000)

    shown_before_sweep = await sale.orders.get_order(order.order_id)
    result = await sale.orders.pay(order.order_id)
    again = await sale.orders.pay(order.order_id)

    assert shown_before_sweep.status == "expired"
    assert result.outcome == "order_expired"
    assert again.outcome == "order_expired"
    audit = await sale.audit.get()
    assert (audit.stock, audit.expired_orders, audit.expired_quantity, audit.charges) == (10, 1, 2, 0)


async def test_pay_unknown_order_is_not_found(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)

    assert (await sale.orders.pay(new_id())).outcome == "order_not_found"
    assert await sale.orders.get_order(new_id()) is None


async def test_maintenance_releases_unpaid_orders_and_the_stock_goes_to_the_next_in_line(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, initial_stock=1, max_active=1))
    never_pays, nxt = await sale.join_and_admit("never-pays", "next")
    await sale.orders.checkout(never_pays.ticket_id, 1)
    assert (await sale.queue.get_ticket(nxt.ticket_id)).status == "waiting"

    sale.clock.advance(120_000)
    await sale.queue.get_ticket(nxt.ticket_id)
    sweep = await sale.maintenance.run_once()
    checkout = await sale.orders.checkout(nxt.ticket_id, 1)

    assert sweep.released_orders == 1
    assert sweep.admission.admitted == 1
    assert checkout.outcome == "created"


async def test_rejoin_after_the_order_expired_gets_a_new_ticket_but_after_paying_does_not(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    expired = await sale.place_order("forgetful")
    paid = await sale.place_order("buyer")
    await sale.orders.pay(paid.order_id)
    sale.clock.advance(120_000)

    forgetful = await sale.queue.join("forgetful")
    buyer = await sale.queue.join("buyer")

    assert forgetful.outcome == "created"
    assert forgetful.ticket.ticket_id != expired.ticket_id
    assert buyer.outcome == "existing"
    assert (buyer.ticket.status, buyer.ticket.order_id) == ("ordered", paid.order_id)


async def test_sold_out_turns_away_newcomers_and_tells_the_line(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, initial_stock=1))
    order = await sale.place_order("lucky")
    waiting = (await sale.queue.join("hopeful")).ticket

    while_pending = await sale.queue.get_ticket(waiting.ticket_id)
    await sale.orders.pay(order.order_id)
    after_paid = await sale.queue.get_ticket(waiting.ticket_id)
    newcomer = await sale.queue.join("late")

    assert while_pending.status == "waiting"
    assert after_paid.status == "sold_out"
    assert (newcomer.outcome, newcomer.ticket) == ("sold_out", None)
