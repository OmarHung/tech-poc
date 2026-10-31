"""Test doubles shared by several test modules."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import httpx


class FakeClock:
    """A clock the test moves by hand, like .NET's FakeTimeProvider (for code that only reads the time)."""

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


class FakeNetwork:
    """Routes requests by host name ("a", "witness", "sink", ...) to the ASGI apps. Can take a process down or cut
    links."""

    def __init__(self) -> None:
        self.apps: dict[str, Callable[..., Awaitable[None]]] = {}
        self.down: set[str] = set()
        self.cut: set[tuple[str, str]] = set()

    def isolate(self, node: str) -> None:
        for peer in self.apps:
            if peer != node:
                self.cut |= {(node, peer), (peer, node)}

    def heal(self) -> None:
        self.cut.clear()

    def transport_from(self, origin: str) -> httpx.AsyncBaseTransport:
        return _Router(self, origin)


class _Router(httpx.AsyncBaseTransport):
    def __init__(self, network: FakeNetwork, origin: str) -> None:
        self._network = network
        self._origin = origin

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        target, network = request.url.host, self._network
        if self._origin in network.down or target in network.down or (self._origin, target) in network.cut:
            raise httpx.ConnectError(f"{self._origin} cannot reach {target}", request=request)
        return await httpx.ASGITransport(app=network.apps[target]).handle_async_request(request)
