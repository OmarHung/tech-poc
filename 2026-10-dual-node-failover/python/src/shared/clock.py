"""Time for leases and timeouts. Only differences between two readings mean anything."""

from __future__ import annotations

import asyncio
import math
import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Seconds since some fixed point."""
        ...


class MonotonicClock:
    """Reads time.monotonic(), so a wall-clock jump cannot stretch or shrink a lease."""

    def __init__(self) -> None:
        self._origin = time.monotonic()

    def now(self) -> float:
        return time.monotonic() - self._origin


def unix_ms() -> int:
    """Wall-clock time in Unix milliseconds. For timestamps other processes compare, never for leases."""
    return int(time.time() * 1000)


class PeriodicTimer:
    """Ticks on a fixed grid like .NET's PeriodicTimer: the first tick one interval after creation, and a tick missed
    while the caller was busy fires at once, without a burst of catch-up ticks after it."""

    def __init__(self, interval: float) -> None:
        self._interval = interval
        self._next = time.monotonic() + interval

    async def tick(self) -> None:
        now = time.monotonic()
        if self._next < now:
            self._next += math.floor((now - self._next) / self._interval) * self._interval
        await asyncio.sleep(max(0.0, self._next - now))
        self._next += self._interval
