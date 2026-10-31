"""Time as epoch milliseconds, injectable so tests can move it forward instead of sleeping.

Deadlines are computed from this clock and passed into the Lua scripts, so every instance's
clock must be in sync (NTP). The background loops also sleep on it, so a fake clock drives them.
"""

import asyncio
import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...

    async def sleep_ms(self, ms: int) -> None: ...


class SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000

    async def sleep_ms(self, ms: int) -> None:
        await asyncio.sleep(ms / 1000)
