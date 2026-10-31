"""One stint as primary: created on promotion, ended on step-down.

Concurrency note: everything here runs on one asyncio event loop, so a stretch of code without an `await` is atomic.
That is what the .NET version's locks buy; the rule here is to keep every check-then-act sequence free of awaits.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from enum import Enum
from typing import Any, Protocol

from node.storage.replicated_log import ReplicatedLog
from shared import tasks
from shared.clock import Clock
from shared.errors import Unavailable
from shared.witness_contracts import SyncReply

logger = logging.getLogger(__name__)


class PeerState(Enum):
    """Where the standby stands, as far as the primary knows. The values are what /health reports as `peer`."""

    IN_SYNC = "in-sync"
    """The witness may promote the standby, so every acknowledged write must already be on it."""
    SOLO = "solo"
    """The witness agreed not to promote the standby; a write is acknowledged once it is on this node."""
    CATCHING_UP = "catching-up"
    """Writes wait for the standby, and once it has everything the witness is told it may take over again."""


class Arbiter(Protocol):
    """Reports to the arbiter whether the standby may take over. Without a witness every report is accepted."""

    async def set_standby_in_sync(self, epoch: int, in_sync: bool, expected_version: int) -> SyncReply | None:
        """expected_version: the latest witness version this node has seen; only checked for in_sync = true.
        Returns the witness's answer, or None if the report was refused or the witness could not be reached."""
        ...


class Signal:
    """An event that is replaced every time it fires. Take `current` before checking the state it guards; any change
    after that sets the event taken, so the waiter cannot miss it."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    @property
    def current(self) -> asyncio.Event:
        return self._event

    def pulse(self) -> None:
        fired, self._event = self._event, asyncio.Event()
        fired.set()


class PrimaryTerm:
    """Tracks how far the standby has replicated and decides when a write is safe to acknowledge."""

    def __init__(
        self,
        epoch: int,
        standby_in_sync: bool,
        witness_version: int,
        log: ReplicatedLog,
        arbiter: Arbiter,
        catch_up_slack: int,
    ) -> None:
        """standby_in_sync: what the witness said when it granted the lease.
        catch_up_slack: how far behind the standby may be when writes start waiting for it again."""
        self.epoch = epoch
        self._log = log
        self._arbiter = arbiter
        self._catch_up_slack = catch_up_slack
        self._witness_version = witness_version
        self._peer_state = PeerState.IN_SYNC if standby_in_sync else PeerState.CATCHING_UP
        self._match_index = 0
        self._catch_up_target = log.last_index
        self._progress = Signal()
        self._appended = Signal()
        self._ended = False
        # One witness report at a time, so this node never has two in flight that could arrive in the wrong order.
        # Reports from an older process or a call this node gave up on are handled by the witness's version check.
        self._witness_calls = asyncio.Lock()
        self._solo_request: asyncio.Task[bool] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()

    @property
    def peer_state(self) -> PeerState:
        return self._peer_state

    @property
    def match_index(self) -> int:
        return self._match_index

    @property
    def is_ended(self) -> bool:
        return self._ended

    @property
    def append_signal(self) -> asyncio.Event:
        """Set by the next append. Take it before reading the log so no append slips between."""
        return self._appended.current

    def notify_appended(self) -> None:
        self._appended.pulse()

    def spawn[T](self, coro: Coroutine[Any, Any, T], name: str) -> asyncio.Task[T]:
        """Runs work that belongs to this term; end() cancels it (the .NET version's `Ended` token)."""
        task = tasks.spawn(coro, f"{name} (epoch {self.epoch})")
        if self._ended:
            task.cancel()
            return task
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def end(self) -> None:
        """Called by NodeState on step-down. Wakes every waiter and cancels the term's own work."""
        if self._ended:
            return
        self._ended = True
        self._progress.pulse()
        self._appended.pulse()
        for task in list(self._tasks):
            task.cancel()

    async def wait_durable(self, index: int, timeout: float, clock: Clock) -> None:
        """Returns once `index` may be acknowledged: it is on the standby, or the witness has agreed the standby is
        out. If the standby has not confirmed within `timeout` seconds, asks the witness to go on alone."""
        deadline = clock.now() + timeout
        while True:
            if index <= self._match_index or self._peer_state is PeerState.SOLO:
                return
            if self._ended:
                raise Unavailable("lost the primary role before the write reached the standby")
            progress = self._progress.current
            remaining = deadline - clock.now()
            if remaining <= 0:
                if not await self.try_go_solo():
                    raise Unavailable("standby unreachable and the witness did not allow going on alone")
                continue
            try:
                async with asyncio.timeout(remaining):
                    await progress.wait()
            except TimeoutError:
                pass  # deadline reached; the loop asks the witness

    async def try_go_solo(self) -> bool:
        """Asks the witness to let this node acknowledge writes on its own. Writes waiting for the standby share one
        request. The replicator also starts one when a term begins with the witness saying the standby is behind:
        that both avoids stalling the first writes after a failover and outdates any late "in sync" from an older
        process."""
        if self._peer_state is PeerState.SOLO:
            return True
        if self._ended:
            return False
        request = self.request_solo()
        try:
            # shield: one waiter giving up (its client hung up) must not cancel the request the others share.
            return await asyncio.shield(request)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if request.cancelled() and (current is None or current.cancelling() == 0):
                return False  # the term ended while asking
            raise

    def request_solo(self) -> asyncio.Task[bool]:
        """Starts the shared "go on alone" request unless one is already in flight."""
        if self._solo_request is None:
            self._solo_request = self.spawn(self._request_solo(), "go-solo")
        return self._solo_request

    def record_replicated(self, match_index: int) -> bool:
        """Records that the standby holds everything up to match_index. Returns true when the standby has caught up
        and the witness should be told it may be promoted again."""
        self._match_index = max(self._match_index, match_index)
        if self._peer_state is PeerState.SOLO:
            # Read in the same atomic step: a write acknowledged while Solo was appended before it saw Solo, so it is
            # at or below this index, and every write from here on waits for the standby.
            last_index = self._log.last_index
            if last_index - self._match_index <= self._catch_up_slack:
                self._start_catching_up(last_index, "standby is close behind")
        self._progress.pulse()
        return self._peer_state is PeerState.CATCHING_UP and self._match_index >= self._catch_up_target

    def observe_witness(self, version: int, standby_in_sync: bool) -> None:
        """Takes in what the witness reported on lease renewal. If it says the standby is behind while this node
        thinks it is in sync (a "go on alone" whose reply was lost), the standby goes through catching up and is
        reported again."""
        if version < self._witness_version:
            return  # an older reply overtaken by one already seen
        self._witness_version = version
        if not standby_in_sync and self._peer_state is PeerState.IN_SYNC:
            self._start_catching_up(self._log.last_index, "witness says the standby is behind")

    async def try_mark_in_sync(self) -> None:
        """Tells the witness the standby may be promoted again, if it still has every acknowledged write."""
        async with self._witness_calls:
            if self._peer_state is not PeerState.CATCHING_UP or self._match_index < self._catch_up_target:
                return
            reply = await self._arbiter.set_standby_in_sync(self.epoch, True, self._witness_version)
            if reply is None:
                return
            self._witness_version = max(self._witness_version, reply.sync_version)
            if self._peer_state is PeerState.CATCHING_UP:
                self._peer_state = PeerState.IN_SYNC
                logger.info("Epoch %d: standby in sync at index %d", self.epoch, self._match_index)

    async def _request_solo(self) -> bool:
        try:
            async with self._witness_calls:
                reply = await self._arbiter.set_standby_in_sync(self.epoch, False, self._witness_version)
                if reply is None:
                    return False
                self._witness_version = max(self._witness_version, reply.sync_version)
                if self._peer_state is not PeerState.SOLO:
                    logger.warning(
                        "Epoch %d: going on without the standby (it has %d of %d)",
                        self.epoch,
                        self._match_index,
                        self._log.last_index,
                    )
                self._peer_state = PeerState.SOLO
                self._progress.pulse()
                return True
        finally:
            self._solo_request = None

    def _start_catching_up(self, target: int, reason: str) -> None:
        self._peer_state = PeerState.CATCHING_UP
        self._catch_up_target = target
        logger.info("Epoch %d: standby catching up to index %d (%s)", self.epoch, target, reason)
