"""JSON POSTs to the peer or the witness, each bounded by RENEW_INTERVAL_MS. Unreachable means None, never an error."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import httpx

from shared import wire

logger = logging.getLogger(__name__)

# Drop pooled connections quickly: after a network blip, a fresh connection beats a half-dead one.
_LIMITS = httpx.Limits(keepalive_expiry=5.0)


@dataclass(frozen=True)
class RpcResponse:
    status: int
    body: bytes

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    def json(self) -> object | None:
        """The parsed body, or None if it is not JSON."""
        try:
            return wire.loads(self.body)
        except wire.WireError:
            return None


class RpcClient:
    def __init__(self, timeout: float, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._timeout = timeout
        # trust_env=False: an HTTP(S)_PROXY in the environment must not reroute calls between our own processes.
        self._http = httpx.AsyncClient(
            timeout=timeout, follow_redirects=False, trust_env=False, limits=_LIMITS, transport=transport
        )

    async def post(self, url: str, body: dict[str, object]) -> RpcResponse | None:
        """The response (any status), or None if the target could not be reached in time. The timeout covers the
        whole call, connect to last byte. Cancellation from the caller (a term ending) is not swallowed."""
        try:
            async with asyncio.timeout(self._timeout):
                response = await self._http.post(
                    url, content=wire.dumps(body), headers={"content-type": "application/json"}
                )
        except (httpx.HTTPError, TimeoutError, OSError) as ex:
            logger.debug("%s unreachable: %r", url, ex)
            return None
        return RpcResponse(response.status_code, response.content)

    async def aclose(self) -> None:
        await self._http.aclose()
