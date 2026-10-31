"""Decides when this node becomes or stops being primary. Runs every RENEW_INTERVAL_MS."""

from __future__ import annotations

import asyncio
import logging
import math

from node.cluster.node_state import NodeState
from node.cluster.primary_term import PrimaryTerm
from node.cluster.witness_client import WitnessClient
from node.config import ArbiterMode, NodeConfig
from shared.witness_contracts import LeaseReply

logger = logging.getLogger(__name__)


class ElectionWorker:
    def __init__(self, state: NodeState, witness: WitnessClient, config: NodeConfig) -> None:
        self._state = state
        self._witness = witness
        self._config = config

    async def run(self) -> None:
        """Ticks on a fixed grid like .NET's PeriodicTimer: a slow tick is followed by one immediate tick, not a
        burst of catch-up ticks."""
        clock, interval = self._state.clock, self._config.renew_interval
        next_tick = clock.now()
        while True:
            if self._config.arbiter is ArbiterMode.WITNESS:
                await self.tick_with_witness()
            else:
                self.tick_without_witness()
            next_tick += interval
            now = clock.now()
            if next_tick < now:
                next_tick += math.floor((now - next_tick) / interval) * interval
            await asyncio.sleep(max(0.0, next_tick - now))

    async def tick_with_witness(self) -> None:
        state = self._state
        state.step_down_if_lease_expired()
        term = state.current_term
        if term is not None:
            await self._renew(term)
            return
        if not state.try_begin_candidacy():
            return
        try:
            sent_at = state.clock.now()
            reply = await self._witness.acquire()
            if reply is None:
                return
            if not reply.granted:
                state.observe_epoch(reply.epoch, "witness")
                logger.debug("Witness refused the lease: %s", reply.reason)
                return
            state.try_promote(reply.epoch, self._lease_end(sent_at, reply), reply.standby_in_sync, reply.sync_version)
        finally:
            state.end_candidacy()

    def tick_without_witness(self) -> None:
        """The classic setup: the standby promotes itself once the primary has been quiet for a full lease."""
        state = self._state
        if state.current_term is not None or not state.try_begin_candidacy():
            return
        try:
            state.try_promote(state.epoch + 1, math.inf, standby_in_sync=False)
        finally:
            state.end_candidacy()

    async def _renew(self, term: PrimaryTerm) -> None:
        state = self._state
        sent_at = state.clock.now()
        reply = await self._witness.acquire()
        if reply is None:
            return  # the peer lease may still carry this node
        if reply.granted and reply.epoch == term.epoch:
            state.extend_witness_lease(term, self._lease_end(sent_at, reply))
            term.observe_witness(reply.sync_version, reply.standby_in_sync)
            return
        if reply.granted:
            state.try_promote(reply.epoch, self._lease_end(sent_at, reply), reply.standby_in_sync, reply.sync_version)
            return
        state.observe_epoch(reply.epoch, "witness")
        state.step_down(term, f"witness refused renewal: {reply.reason}")

    def _lease_end(self, sent_at: float, reply: LeaseReply) -> float:
        """The lease started no later than when the request was sent; end it a margin early for clock drift."""
        return sent_at + reply.ttl_ms / 1000 - self._config.lease_safety_margin
