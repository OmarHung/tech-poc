from dataclasses import replace

import pytest
from redis.asyncio import Redis

from checkout_queue.ids import new_id
from checkout_queue.tickets import AdmitResult
from support import SaleUnderTest

pytestmark = pytest.mark.anyio


async def test_join_puts_users_in_line_in_arrival_order(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)

    first = await sale.queue.join("alice")
    second = await sale.queue.join("bob")

    assert first.outcome == "created"
    assert second.ticket.status == "waiting"
    assert (first.ticket.position, second.ticket.position) == (1, 2)
    assert second.ticket.seq > first.ticket.seq
    assert second.ticket.poll_after_ms == 1000


async def test_join_again_returns_the_same_ticket_so_nobody_holds_two_places(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)

    first = await sale.queue.join("alice")
    again = await sale.queue.join("alice")

    assert again.outcome == "existing"
    assert again.ticket.ticket_id == first.ticket.ticket_id


async def test_admit_lets_in_at_most_max_active_in_arrival_order(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, max_active=2))

    tickets = await sale.join_and_admit("a", "b", "c")

    assert [t.status for t in tickets] == ["admitted", "admitted", "waiting"]
    assert tickets[2].position == 1
    assert tickets[0].admitted_at_ms == sale.now_ms
    assert tickets[0].admission_expires_at_ms == sale.now_ms + 60_000
    assert tickets[0].position is None


async def test_admit_never_lets_in_more_users_than_stock_left(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, initial_stock=1, max_active=5))

    tickets = await sale.join_and_admit("a", "b")

    assert [t.status for t in tickets] == ["admitted", "waiting"]


async def test_admit_skips_users_who_stopped_polling(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, max_active=1))
    gone = (await sale.queue.join("gone")).ticket
    stays = (await sale.queue.join("stays")).ticket

    sale.clock.advance(16_000)
    await sale.queue.get_ticket(stays.ticket_id)
    result = await sale.queue.admit()

    assert result == AdmitResult(admitted=1, abandoned=1, expired=0)
    assert (await sale.queue.get_ticket(gone.ticket_id)).status == "abandoned"
    assert (await sale.queue.get_ticket(stays.ticket_id)).status == "admitted"


async def test_admission_window_expires_and_frees_the_slot_for_the_next_user(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, max_active=1))
    slow, nxt = await sale.join_and_admit("slow", "next")

    sale.clock.advance(60_000)
    await sale.queue.get_ticket(nxt.ticket_id)
    expired_before_tick = await sale.queue.get_ticket(slow.ticket_id)
    result = await sale.queue.admit()

    assert expired_before_tick.status == "expired"
    assert result == AdmitResult(admitted=1, abandoned=0, expired=1)
    assert (await sale.queue.get_ticket(nxt.ticket_id)).status == "admitted"


async def test_rejoin_after_admission_expired_goes_to_the_back_of_the_line(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, max_active=1))
    tickets = await sale.join_and_admit("slow", "next", "last")
    sale.clock.advance(60_000)

    rejoined = await sale.queue.join("slow")

    assert rejoined.outcome == "created"
    assert rejoined.ticket.ticket_id != tickets[0].ticket_id
    assert rejoined.ticket.position == 3


async def test_rejoin_after_being_skipped_for_not_polling_gets_a_new_ticket(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    first = (await sale.queue.join("napper")).ticket
    sale.clock.advance(16_000)
    await sale.queue.admit()

    rejoined = await sale.queue.join("napper")

    assert rejoined.outcome == "created"
    assert rejoined.ticket.ticket_id != first.ticket_id


async def test_waiting_ticket_estimates_the_wait_once_admissions_are_flowing(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, max_active=1))
    for user in ("u0", "u1", "u2"):
        await sale.queue.join(user)
    before_any_admission = (await sale.queue.join("u2")).ticket

    sale.clock.advance(1_000)
    await sale.queue.admit()
    after_admission = (await sale.queue.join("u2")).ticket

    assert before_any_admission.estimated_wait_seconds is None
    assert after_admission.position == 2
    assert after_admission.estimated_wait_seconds > 0


async def test_wait_estimate_matches_a_steady_admission_rate(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, initial_stock=100, max_active=2))
    users = [f"u{i}" for i in range(30)]
    for user in users:
        await sale.queue.join(user)

    # Two people check out every second, so two are let in every second.
    for _ in range(5):
        await sale.queue.admit()
        for user in users:
            ticket = (await sale.queue.join(user)).ticket
            if ticket.status == "admitted":
                await sale.orders.checkout(ticket.ticket_id, 1)
        sale.clock.advance(1_000)
    await sale.queue.admit()
    tenth_in_line = (await sale.queue.join(users[21])).ticket

    assert tenth_in_line.position == 10
    assert tenth_in_line.estimated_wait_seconds == 5


async def test_get_ticket_returns_none_for_unknown_ticket(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)

    assert await sale.queue.get_ticket(new_id()) is None


async def test_watch_reads_tickets_in_batches(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, initial_stock=1_000))
    joined = [(await sale.queue.join(f"w{i}")).ticket.ticket_id for i in range(501)]

    views = await sale.queue.watch(joined)

    assert [v.ticket_id for v in views] == joined
    assert views[-1].position == 501
