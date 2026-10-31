"""Time for leases and timeouts. Only differences between two readings mean anything."""

from __future__ import annotations

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
