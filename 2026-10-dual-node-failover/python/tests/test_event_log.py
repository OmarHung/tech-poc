from __future__ import annotations

import asyncio

import pytest

from shared.errors import InvalidInput
from shared.work_contracts import LoggedEvent, NewEvent
from upstream.event_log import MAX_AMOUNT, MAX_PAGE, EventLog

pytestmark = pytest.mark.anyio

WAIT_S = 1.0


def test_append_numbers_events_from_one() -> None:
    log = EventLog()

    assert log.append(NewEvent("acct-1", 10)) == LoggedEvent(1, "acct-1", 10)
    assert log.append(NewEvent("acct-2", -5)) == LoggedEvent(2, "acct-2", -5)
    assert log.last == 2


async def test_read_returns_events_after_the_position() -> None:
    log = EventLog()
    for i in range(1, 6):
        log.append(NewEvent("acct", i))

    page = await log.read(after=2, limit=2, wait=0)

    assert [event.seq for event in page.events] == [3, 4]
    assert page.last == 5


async def test_read_caps_the_page_and_tolerates_a_position_past_the_end() -> None:
    log = EventLog()
    for i in range(MAX_PAGE + 10):
        log.append(NewEvent("acct", i))

    assert len((await log.read(after=0, limit=MAX_PAGE * 2, wait=0)).events) == MAX_PAGE
    assert (await log.read(after=10_000, limit=10, wait=0)).events == ()


async def test_read_waits_for_the_next_event() -> None:
    log = EventLog()
    reading = asyncio.ensure_future(log.read(after=0, limit=10, wait=WAIT_S))
    await asyncio.sleep(0)
    assert not reading.done()

    log.append(NewEvent("acct", 1))

    page = await asyncio.wait_for(reading, WAIT_S / 2)
    assert len(page.events) == 1


async def test_every_waiting_reader_is_woken_and_a_later_one_waits_for_the_next_event() -> None:
    log = EventLog()
    first = [asyncio.ensure_future(log.read(after=0, limit=10, wait=WAIT_S)) for _ in range(3)]
    await asyncio.sleep(0)

    log.append(NewEvent("acct", 1))
    later = asyncio.ensure_future(log.read(after=1, limit=10, wait=WAIT_S))
    await asyncio.sleep(0)

    assert [len(page.events) for page in await asyncio.gather(*first)] == [1, 1, 1]
    assert not later.done()
    log.append(NewEvent("acct", 2))
    assert (await asyncio.wait_for(later, WAIT_S / 2)).events[0].seq == 2


async def test_read_returns_an_empty_page_when_nothing_arrives() -> None:
    page = await EventLog().read(after=0, limit=10, wait=0.05)

    assert page.events == ()
    assert page.last == 0


@pytest.mark.parametrize(
    ("account", "amount"),
    [(None, 1), ("", 1), ("bad account", 1), ("ä", 1), ("x" * 65, 1), ("acct", MAX_AMOUNT + 1), ("acct", -(2**63))],
)
def test_append_rejects_bad_events(account: str | None, amount: int) -> None:
    with pytest.raises(InvalidInput):
        EventLog().append(NewEvent(account, amount))


@pytest.mark.parametrize(("after", "limit"), [(-1, 10), (0, 0)])
async def test_read_rejects_a_negative_position_or_an_empty_page(after: int, limit: int) -> None:
    with pytest.raises(InvalidInput, match="after must be >= 0 and limit >= 1"):
        await EventLog().read(after, limit, 0)
