from __future__ import annotations

import logging
from collections.abc import Callable
from urllib.parse import urljoin

from node.cluster.rpc import RpcClient
from node.config import NodeConfig
from shared.wire import JsonObject, WireError
from shared.witness_contracts import LeaseReply, LeaseRequest, SyncReply, SyncRequest

logger = logging.getLogger(__name__)


class WitnessClient:
    """The node's side of the witness API. Every call returns None when the witness cannot be reached."""

    def __init__(self, rpc: RpcClient, config: NodeConfig) -> None:
        self._rpc = rpc
        self._node_id = config.node_id
        self._lease_url = urljoin(config.witness_url, "lease")
        self._sync_url = urljoin(config.witness_url, "sync")

    async def acquire(self) -> LeaseReply | None:
        """Acquires or renews the lease."""
        response = await self._rpc.post(self._lease_url, LeaseRequest(self._node_id).to_wire())
        if response is None or not response.ok:
            return None
        return _parse(LeaseReply.from_wire, response.json())

    async def set_standby_in_sync(self, epoch: int, in_sync: bool, expected_version: int) -> SyncReply | None:
        request = SyncRequest(self._node_id, epoch, in_sync, expected_version)
        response = await self._rpc.post(self._sync_url, request.to_wire())
        if response is None:
            return None
        if response.status == 409:
            error = _parse(lambda body: JsonObject(body).string("error"), response.json())
            logger.warning("Witness refused standby in sync = %s for epoch %d: %s", in_sync, epoch, error)
            return None
        return _parse(SyncReply.from_wire, response.json()) if response.ok else None


class NoArbiter:
    """No witness: nobody to ask, so the primary may always go on alone. This is what lets both sides keep writing."""

    async def set_standby_in_sync(self, epoch: int, in_sync: bool, expected_version: int) -> SyncReply | None:
        return SyncReply(epoch, in_sync, expected_version + 1)


def _parse[T](parse: Callable[[object], T], body: object | None) -> T | None:
    if body is None:
        return None
    try:
        return parse(body)
    except WireError:
        return None
