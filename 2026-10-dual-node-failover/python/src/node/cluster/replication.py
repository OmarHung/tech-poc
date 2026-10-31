"""While this node is primary, ships its log to the standby one batch at a time and heartbeats it when idle.
Every answer from the standby renews the peer lease and moves PrimaryTerm.match_index."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import urljoin

from node.cluster.contracts import MAX_BATCH, ReplicateReply, ReplicateRequest, ReplicateStatus
from node.cluster.node_state import NodeState
from node.cluster.primary_term import PeerState, PrimaryTerm
from node.cluster.rpc import RpcClient
from node.config import NodeConfig
from shared.wire import WireError

logger = logging.getLogger(__name__)


class ReplicationWorker:
    def __init__(self, state: NodeState, rpc: RpcClient, config: NodeConfig) -> None:
        self._state = state
        self._rpc = rpc
        self._config = config
        self._replicate_url = urljoin(config.peer_url, "internal/replicate")

    async def run(self) -> None:
        while True:
            term = await self._state.wait_for_term()
            # A task of the term, so stepping down cancels it wherever it is waiting.
            replicating = term.spawn(self.replicate_term(term), "replicate")
            try:
                await replicating
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    raise  # shutting down
                # The term ended; wait for the next one.

    async def replicate_term(self, term: PrimaryTerm) -> None:
        log, clock, config = self._state.log, self._state.clock, self._config
        retry_delay = config.renew_interval / 5
        next_index = log.last_index + 1  # optimistic; a mismatch walks it back
        last_sent = clock.now() - config.renew_interval  # heartbeat right away
        in_sync_call: asyncio.Task[Any] | None = None
        if term.peer_state is not PeerState.IN_SYNC:
            term.request_solo()  # the witness says the standby is behind; see PrimaryTerm.try_go_solo
        while not term.is_ended:
            appended = term.append_signal
            batch = log.read(next_index, MAX_BATCH)
            if batch is None:
                # The log shrank, which only happens after a newer primary deposed this one and ended the term.
                return
            idle = clock.now() - last_sent
            if not batch.entries and idle < config.renew_interval:
                await _wait_quietly(appended, config.renew_interval - idle)
                continue

            sent_at = clock.now()
            last_sent = sent_at
            request = ReplicateRequest(
                config.node_id, term.epoch, next_index - 1, batch.prev_epoch, batch.entries, batch.last_index
            )
            reply = await self._send(request)
            if reply is None or reply.status is ReplicateStatus.REJECTED:
                if reply is not None and reply.epoch > term.epoch:
                    # Steps down, which cancels this very task: return before the next await so it ends quietly.
                    self._state.observe_epoch(reply.epoch, "standby reply")
                    return
                await asyncio.sleep(retry_delay)
                continue

            # The standby accepted this node as primary and promised not to seek the lease for a while.
            self._state.extend_peer_lease(term, sent_at + reply.promise_ms / 1000 - config.lease_safety_margin)
            if reply.status is ReplicateStatus.MISMATCH:
                next_index = max(1, min(next_index - 1, reply.match_index + 1))
                continue
            next_index = reply.match_index + 1
            if term.record_replicated(reply.match_index) and (in_sync_call is None or in_sync_call.done()):
                # In the background: a slow witness must not hold up replication.
                in_sync_call = term.spawn(term.try_mark_in_sync(), "mark-in-sync")

    async def _send(self, request: ReplicateRequest) -> ReplicateReply | None:
        response = await self._rpc.post(self._replicate_url, request.to_wire())
        if response is None:
            return None
        if not response.ok:
            logger.warning("Standby answered replication with %d", response.status)
            return None
        body = response.json()
        try:
            return None if body is None else ReplicateReply.from_wire(body)
        except WireError:
            return None


async def _wait_quietly(signal: asyncio.Event, timeout: float) -> None:
    try:
        async with asyncio.timeout(timeout):
            await signal.wait()
    except TimeoutError:
        pass  # time for a heartbeat
