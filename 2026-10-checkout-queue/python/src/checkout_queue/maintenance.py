"""One maintenance pass, run by every instance on its own timer.

Each step is one atomic script, so there is no leader to elect and no instance whose loss stops
the line: ten instances ticking at once admit exactly as many users as one would.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .clock import Clock
from .orders import OrderStore
from .sale_redis import REDIS_ERRORS, describe
from .tickets import AdmitResult, QueueStore

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MaintenanceResult:
    released_orders: int
    admission: AdmitResult


class SaleMaintenance:
    def __init__(self, queue: QueueStore, orders: OrderStore) -> None:
        self._queue = queue
        self._orders = orders

    async def run_once(self) -> MaintenanceResult:
        # Release first: stock coming back from unpaid orders can be admitted in the same pass.
        released = await self._orders.expire_orders()
        admission = await self._queue.admit()
        return MaintenanceResult(released, admission)


async def run_maintenance_loop(maintenance: SaleMaintenance, clock: Clock, tick_ms: int, sale_id: str) -> None:
    """Runs `maintenance` every TICK_MS until cancelled.

    Redis blips are expected and retried next tick; anything else is a bug and ends the loop
    (the app then stops the process).
    """
    while True:
        await clock.sleep_ms(tick_ms)
        try:
            await maintenance.run_once()
        except REDIS_ERRORS as exc:
            log.warning("Maintenance tick failed for sale %s; retrying next tick: %s", sale_id, describe(exc))
