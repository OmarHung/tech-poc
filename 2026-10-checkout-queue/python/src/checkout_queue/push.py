"""Feeds this instance's event streams.

Admissions arrive over Redis pub/sub and are pushed at once; everything else (position, wait
estimate) is refreshed once per poll interval in one batched call, so a stream never sends more
often than polling would. One consumer loop does all the work, fed by two sources.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from typing import Any

from redis.asyncio.client import PubSub

from .clock import Clock
from .config import Config
from .sale_redis import REDIS_ERRORS, SaleRedis, describe
from .streams import TicketStreams
from .tickets import QueueStore, TicketView

log = logging.getLogger(__name__)

SUBSCRIBE_TIMEOUT_S = 5.0

# Runs a coroutine as a background task that stops the process if it dies of a bug.
Spawn = Callable[[str, Coroutine[Any, Any, None]], "asyncio.Task[None]"]


class TicketPushWorker:
    def __init__(
        self, redis: SaleRedis, queue: QueueStore, streams: TicketStreams, config: Config, clock: Clock
    ) -> None:
        self._redis = redis
        self._queue = queue
        self._streams = streams
        self._config = config
        self._clock = clock
        # None asks for a refresh of every open stream; a list names the tickets just admitted.
        self._work: asyncio.Queue[list[str] | None] = asyncio.Queue()
        # Last view pushed per ticket. Only the consumer loop touches it.
        self._sent: dict[str, TicketView] = {}
        self._pubsub: PubSub | None = None
        self._spawn: Spawn | None = None
        self._tasks: list[asyncio.Task[None]] = []

    @property
    def subscribed(self) -> bool:
        return self._pubsub is not None

    async def start(self, spawn: Spawn) -> None:
        """Subscribes before the app takes requests, so no admission can be missed in between."""
        self._spawn = spawn
        await self._try_subscribe()
        self._tasks += [spawn("ticket push", self._consume()), spawn("ticket refresh timer", self._refresh_timer())]

    async def stop(self) -> None:
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._drop_subscription()

    async def _consume(self) -> None:
        while True:
            admitted = await self._work.get()
            try:
                if not self.subscribed:
                    await self._try_subscribe()
                await self._push(admitted)
            except REDIS_ERRORS as exc:
                log.warning(
                    "Pushing ticket updates failed for sale %s; retrying next refresh: %s",
                    self._config.sale_id,
                    describe(exc),
                )

    async def _refresh_timer(self) -> None:
        while True:
            await self._clock.sleep_ms(self._config.poll_interval_ms)
            self._work.put_nowait(None)

    async def _try_subscribe(self) -> None:
        pubsub = self._redis.client.pubsub()
        try:
            await pubsub.subscribe(self._redis.admitted_channel)
            # Wait for Redis to confirm: only then is every later admission sure to reach us.
            confirmation = await pubsub.get_message(timeout=SUBSCRIBE_TIMEOUT_S)
            if confirmation is None or confirmation["type"] != "subscribe":
                raise TimeoutError(f"no subscribe confirmation, got {confirmation!r}")
        except REDIS_ERRORS as exc:
            log.warning(
                "Subscribing to admissions failed for sale %s; streams fall back to the periodic refresh: %s",
                self._config.sale_id,
                describe(exc),
            )
            await _close_quietly(pubsub)
            return
        self._pubsub = pubsub
        if self._spawn is not None:
            self._tasks.append(self._spawn("admissions listener", self._listen(pubsub)))

    async def _listen(self, pubsub: PubSub) -> None:
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
                if message is not None and message["type"] == "message":
                    self._work.put_nowait(message["data"].split(" "))
        except REDIS_ERRORS as exc:
            # The periodic refresh still catches up; the consumer resubscribes on its next run.
            log.warning("Admissions subscription lost for sale %s: %s", self._config.sale_id, describe(exc))
            if self._pubsub is pubsub:
                self._pubsub = None
            await _close_quietly(pubsub)

    async def _drop_subscription(self) -> None:
        pubsub, self._pubsub = self._pubsub, None
        if pubsub is not None:
            await _close_quietly(pubsub)

    async def _push(self, admitted: list[str] | None) -> None:
        open_ids = self._streams.ticket_ids
        open_set = set(open_ids)
        for closed in self._sent.keys() - open_set:
            del self._sent[closed]
        wanted = open_ids if admitted is None else [i for i in dict.fromkeys(admitted) if i in open_set]
        if not wanted:
            return
        for view in await self._queue.watch(wanted):
            if self._sent.get(view.ticket_id) == view:
                continue
            self._sent[view.ticket_id] = view
            self._streams.publish(view)


async def _close_quietly(pubsub: PubSub) -> None:
    with contextlib.suppress(*REDIS_ERRORS):
        await pubsub.aclose()
