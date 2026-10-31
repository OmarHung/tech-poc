"""Test doubles shared by several test modules."""

from __future__ import annotations


class FakeClock:
    """A clock the test moves by hand, like .NET's FakeTimeProvider (for code that only reads the time)."""

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds
