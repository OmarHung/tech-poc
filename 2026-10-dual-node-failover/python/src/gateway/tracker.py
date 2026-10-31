"""Polls every node's /health and remembers which one is primary, the way HAProxy is pointed at Patroni's /primary
check. If two nodes claim it (only possible without a witness), the newer epoch wins."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import urljoin

import httpx

from gateway.config import GatewayConfig, NodeTarget
from shared import wire
from shared.http import is_json_content_type

MIN_HEALTH_TIMEOUT_S = 0.3

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NodeStatus:
    """The part of a node's /health the gateway needs."""

    role: str | None
    epoch: int

    @staticmethod
    def parse(content_type: str | None, body: bytes) -> NodeStatus | None:
        """None for anything that is not a JSON object with the right types: a node answering nonsense is a node to
        skip, not a reason to stop polling."""
        if not is_json_content_type(content_type):
            return None
        try:
            value = wire.loads(body)
            if value is None:
                return None
            fields = wire.JsonObject(value)
            return NodeStatus(fields.string("role"), fields.integer("epoch"))
        except wire.WireError:
            return None


@dataclass(frozen=True)
class PrimaryChoice:
    target: NodeTarget
    epoch: int


def choose(polled: Iterable[tuple[NodeTarget, NodeStatus | None]]) -> PrimaryChoice | None:
    best: PrimaryChoice | None = None
    for target, status in polled:
        if status is not None and status.role == "primary" and (best is None or status.epoch > best.epoch):
            best = PrimaryChoice(target, status.epoch)
    return best


class PrimaryTracker:
    def __init__(self, config: GatewayConfig, http: httpx.AsyncClient) -> None:
        self._config = config
        self._http = http
        self._nudged = asyncio.Event()
        self._current: PrimaryChoice | None = None
        self._health_timeout = max(config.poll_interval * 3, MIN_HEALTH_TIMEOUT_S)

    @property
    def current(self) -> PrimaryChoice | None:
        return self._current

    def nudge(self) -> None:
        """Polls again right away, e.g. after the primary failed a request. One pending poll is enough."""
        self._nudged.set()

    async def run(self) -> None:
        while True:
            self._nudged.clear()
            await self.poll_once()
            try:
                async with asyncio.timeout(self._config.poll_interval):
                    await self._nudged.wait()
            except TimeoutError:
                pass

    async def poll_once(self) -> PrimaryChoice | None:
        polled = await asyncio.gather(*(self._poll(node) for node in self._config.nodes))
        choice, previous = choose(polled), self._current
        self._current = choice
        if choice != previous:
            logger.warning(
                "Primary is now %s (epoch %s)",
                "none" if choice is None else choice.target.id,
                None if choice is None else choice.epoch,
            )
        return choice

    async def _poll(self, node: NodeTarget) -> tuple[NodeTarget, NodeStatus | None]:
        try:
            async with asyncio.timeout(self._health_timeout):
                response = await self._http.get(urljoin(node.url, "health"))
        except (httpx.HTTPError, TimeoutError, OSError):
            return node, None
        if not response.is_success:
            return node, None
        return node, NodeStatus.parse(response.headers.get("content-type"), response.content)
