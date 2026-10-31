"""Calls to the upstream and the sink. Every call is bounded; unreachable means None, never an exception."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Literal
from urllib.parse import urljoin

import httpx

from shared import wire
from shared.work_contracts import EventPage, ResultRecord, ResultReply, TaskAttempt, TaskLease, TaskLeaseRequest
from worker.config import WorkerConfig

# No cap on connections, and drop pooled ones quickly: after a network blip, a fresh connection beats a half-dead one.
_LIMITS = httpx.Limits(max_connections=None, max_keepalive_connections=20, keepalive_expiry=5.0)

TaskAction = Literal["heartbeat", "complete"]

logger = logging.getLogger(__name__)


def _http(transport: httpx.AsyncBaseTransport | None) -> httpx.AsyncClient:
    # trust_env=False: an HTTP(S)_PROXY in the environment must not reroute calls between our own processes.
    # The timeout is set per call; asyncio.timeout bounds each one from connect to last byte.
    return httpx.AsyncClient(timeout=None, follow_redirects=False, trust_env=False, limits=_LIMITS, transport=transport)


def _parse[T](parse: Callable[[object], T], response: httpx.Response) -> T | None:
    try:
        return parse(wire.loads(response.content))
    except wire.WireError:
        return None


class WorkClient:
    def __init__(self, config: WorkerConfig, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._config = config
        self._events_url = urljoin(config.upstream_url, "events")
        self._lease_url = urljoin(config.upstream_url, "tasks/lease")
        self._results_url = urljoin(config.sink_url, "results")
        self._sink_health_url = urljoin(config.sink_url, "health")
        # Three pools, so a long-poll never waits behind a heartbeat or a delivery, and they never wait behind it.
        self._polls = _http(transport)
        self._upstream = _http(transport)
        self._sink = _http(transport)

    async def read_events(self, after: int, limit: int) -> EventPage | None:
        config = self._config
        url = f"{self._events_url}?after={after}&limit={limit}&waitMs={config.poll_wait_ms}"
        response = await self._send(self._polls, "GET", url, None, config.poll_wait + config.rpc_timeout)
        return None if response is None or not response.is_success else _parse(EventPage.from_wire, response)

    async def last_event(self) -> int | None:
        url = f"{self._events_url}?after=0&limit=1&waitMs=0"
        response = await self._send(self._upstream, "GET", url, None, self._config.rpc_timeout)
        if response is None or not response.is_success:
            return None
        page = _parse(EventPage.from_wire, response)
        return None if page is None else page.last

    async def lease_task(self) -> tuple[bool, TaskLease | None]:
        """(reached, lease). reached: whether the upstream answered at all (false: refused, reset or timed out, so wait
        before asking again). lease: the task, or None if none came up within the poll wait."""
        config = self._config
        body = TaskLeaseRequest(config.worker_id, config.poll_wait_ms).to_wire()
        response = await self._send(self._polls, "POST", self._lease_url, body, config.poll_wait + config.rpc_timeout)
        if response is None:
            return False, None
        if response.status_code == 200:
            return True, _parse(TaskLease.from_wire, response)
        return response.status_code == 204, None

    async def heartbeat(self, lease: TaskLease, timeout: float) -> bool | None:
        """True if the lease was extended, False if it is lost, None if the upstream could not be reached in time."""
        return await self._task_call(lease, "heartbeat", timeout)

    async def complete(self, lease: TaskLease) -> bool | None:
        """True if the task is done, False if another attempt owns it now (or the task is gone), None if the upstream
        could not be reached."""
        return await self._task_call(lease, "complete", self._config.rpc_timeout)

    async def sink_reachable(self) -> bool:
        """Whether the sink answers its health check."""
        response = await self._send(self._sink, "GET", self._sink_health_url, None, self._config.rpc_timeout)
        return response is not None and response.is_success

    async def deliver(self, record: ResultRecord, timeout: float | None = None) -> str | None:
        """The sink's verdict ("applied", "duplicate", "conflict", "stale"), or None if it could not be reached.
        timeout defaults to RPC_TIMEOUT_MS; a dispatched worker passes what is left of its lease."""
        bound = self._config.rpc_timeout if timeout is None else timeout
        response = await self._send(self._sink, "POST", self._results_url, record.to_wire(), bound)
        if response is None or response.status_code >= 500:
            return None
        reply = _parse(ResultReply.from_wire, response)
        return None if reply is None else reply.status

    async def aclose(self) -> None:
        for http in (self._polls, self._upstream, self._sink):
            await http.aclose()

    async def _task_call(self, lease: TaskLease, action: TaskAction, timeout: float) -> bool | None:
        url = urljoin(self._config.upstream_url, f"tasks/{lease.id}/{action}")
        body = TaskAttempt(self._config.worker_id, lease.attempt).to_wire()
        response = await self._send(self._upstream, "POST", url, body, timeout)
        if response is None:
            return None
        if response.status_code == 200:
            return True
        # 404: the upstream no longer knows the task (it restarted); retrying will not bring it back.
        return False if response.status_code in (404, 409) else None

    async def _send(
        self, http: httpx.AsyncClient, method: str, url: str, body: dict[str, object] | None, timeout: float
    ) -> httpx.Response | None:
        """The response (any status, body read), or None if the target could not be reached in time. Cancellation from
        the caller (shutdown, a task ending) is not swallowed."""
        content, headers = (None, None) if body is None else (wire.dumps(body), {"content-type": "application/json"})
        try:
            async with asyncio.timeout(timeout):
                return await http.request(method, url, content=content, headers=headers)
        except (httpx.HTTPError, TimeoutError, OSError) as ex:
            logger.debug("%s %s failed: %r", method, url, ex)
            return None
