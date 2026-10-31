"""The feed for hot-hot redundancy: an append-only, in-memory log of events. Readers keep their own position and can
re-read from any point, which is what lets a restarted worker catch up without asking the other worker anything."""

from __future__ import annotations

import asyncio

from shared.errors import InvalidInput
from shared.work_contracts import WORK_ID_MAX_LENGTH, EventPage, LoggedEvent, NewEvent, is_work_id

MAX_PAGE = 500
MAX_AMOUNT = 1_000_000_000


class EventLog:
    """append is synchronous, and read never awaits between looking at the log and picking up the signal it waits on,
    so an append can never slip in between and go unnoticed."""

    def __init__(self) -> None:
        self._events: list[LoggedEvent] = []
        self._appended = asyncio.Event()

    @property
    def last(self) -> int:
        return len(self._events)

    def append(self, request: NewEvent) -> LoggedEvent:
        account = request.account
        if not is_work_id(account):
            raise InvalidInput(f"account must be 1-{WORK_ID_MAX_LENGTH} ASCII letters, digits, '-', '_' or '.'")
        if not -MAX_AMOUNT <= request.amount <= MAX_AMOUNT:
            raise InvalidInput(f"amount must be between -{MAX_AMOUNT} and {MAX_AMOUNT}")
        logged = LoggedEvent(len(self._events) + 1, account, request.amount)
        self._events.append(logged)
        # A fresh signal for the next append; everyone waiting on this one is woken.
        fired, self._appended = self._appended, asyncio.Event()
        fired.set()
        return logged

    async def read(self, after: int, limit: int, wait: float) -> EventPage:
        """Events after `after`; waits up to `wait` seconds for one if there are none yet."""
        if after < 0 or limit < 1:
            raise InvalidInput("after must be >= 0 and limit >= 1")
        if after < len(self._events) or wait <= 0:
            return self._page(after, limit)
        appended = self._appended
        try:
            async with asyncio.timeout(wait):
                await appended.wait()
        except TimeoutError:
            pass  # nothing new; answer with an empty page
        return self._page(after, limit)

    def _page(self, after: int, limit: int) -> EventPage:
        start = min(after, len(self._events))
        count = min(limit, MAX_PAGE, len(self._events) - start)
        return EventPage(tuple(self._events[start : start + count]), len(self._events))
