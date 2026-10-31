"""Hot-hot redundancy: read every event from the upstream log, apply it to the ledger and send the result to the sink.
The other worker does exactly the same; neither knows the other exists. After each batch the position and balances
are saved, so a restart replays only what was not yet saved (and re-sends those results, which the sink ignores)."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from shared import state_file
from shared.clock import unix_ms
from shared.work_contracts import LoggedEvent, ResultRecord
from worker.client import WorkClient
from worker.config import StartFrom, WorkerConfig
from worker.ledger import EMPTY_LEDGER, Ledger
from worker.status import WorkerStatus

BATCH = 100
CHECKPOINT_FILE = "checkpoint.json"

logger = logging.getLogger(__name__)


class HotHotWorker:
    def __init__(self, client: WorkClient, status: WorkerStatus, config: WorkerConfig) -> None:
        self._client = client
        self._status = status
        self._config = config
        self._checkpoint = Path(config.data_dir) / CHECKPOINT_FILE

    async def run(self) -> None:
        config, client, status = self._config, self._client, self._status
        ledger = self._load()
        if config.start_from is StartFrom.LATEST:
            ledger = await self._skip_to_latest(ledger)
        status.ledger = ledger
        logger.info("Hot-hot worker %s starting after event %d", config.worker_id, ledger.offset)
        while True:
            page = await client.read_events(ledger.offset, BATCH)
            if page is None:
                await asyncio.sleep(config.retry_delay)
                continue
            if page.last < ledger.offset:
                # The upstream has fewer events than this worker already applied: it lost history (an in-memory
                # upstream restarted). New events would reuse old numbers, so stop rather than send wrong results.
                logger.error(
                    "Upstream ends at event %d but this worker is at %d; it lost events. Not processing.",
                    page.last,
                    ledger.offset,
                )
                await asyncio.sleep(config.poll_wait)
                continue
            for logged in page.events:
                applied, balance = ledger.apply(logged)
                key = f"event-{logged.seq}"
                await self._deliver(ResultRecord(key, config.worker_id, 0, self._format(logged, balance)))
                ledger = applied
            if page.events:
                # After delivering: a crash before this line means re-sending the batch, never skipping it.
                self._save(ledger)
                status.ledger = ledger

    def _format(self, logged: LoggedEvent, balance: int) -> str:
        if self._config.deterministic:
            return f"{logged.account}:{balance}"
        return f"{logged.account}:{balance}@{unix_ms()}"

    async def _deliver(self, record: ResultRecord) -> None:
        """Retries until the sink answers. Any answer counts: a conflict is recorded there, retrying will not change
        it."""
        while (reply := await self._client.deliver(record)) is None:
            await asyncio.sleep(self._config.retry_delay)
        self._status.sink_replied(reply)
        if reply == "conflict":
            logger.warning("Sink holds a different result for %s than %s", record.key, record.value)

    async def _skip_to_latest(self, ledger: Ledger) -> Ledger:
        while (last := await self._client.last_event()) is None:
            await asyncio.sleep(self._config.retry_delay)
        if last <= ledger.offset:
            return ledger
        logger.warning("Skipping events %d-%d (START_FROM=latest); balances will miss them", ledger.offset + 1, last)
        skipped = ledger.skip_to(last)
        self._save(skipped)
        return skipped

    def _load(self) -> Ledger:
        saved = state_file.load(self._checkpoint)
        return EMPTY_LEDGER if saved is None else Ledger.from_file(saved)

    def _save(self, ledger: Ledger) -> None:
        state_file.save(self._checkpoint, ledger.to_file())
