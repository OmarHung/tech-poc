"""The replication loop against a scripted standby."""

from __future__ import annotations

import asyncio
import json
import math
from pathlib import Path

import httpx
import pytest

from node.cluster.node_state import NodeState
from node.cluster.primary_term import PeerState
from node.cluster.replication import ReplicationWorker
from node.cluster.rpc import RpcClient
from node.cluster.witness_client import NoArbiter
from node.config import NodeConfig
from node.storage.replicated_log import ReplicatedLog
from shared.clock import MonotonicClock

pytestmark = pytest.mark.anyio

CONFIG = NodeConfig(lease_ttl_ms=300, renew_interval_ms=50, replication_timeout_ms=100, peer_url="http://b/")


class ScriptedStandby:
    """Answers each /internal/replicate with the next scripted (status code, body); then plain heartbeats."""

    def __init__(self, *answers: tuple[int, object]) -> None:
        self.answers = list(answers)
        self.requests: list[dict[str, object]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.requests.append(body)
        if self.answers:
            status, answer = self.answers.pop(0)
            return httpx.Response(status, json=answer)
        last = body["prevIndex"] + len(body["entries"])
        return httpx.Response(200, json=reply("ok", body["epoch"], last))


def reply(status: str, epoch: int, match_index: int) -> dict[str, object]:
    return {"status": status, "epoch": epoch, "matchIndex": match_index, "promiseMs": 300, "reason": None}


@pytest.fixture
def primary(tmp_path: Path):
    log = ReplicatedLog.open(tmp_path / "log.jsonl")
    state = NodeState(NodeConfig(data_dir=str(tmp_path)), log, NoArbiter(), MonotonicClock())
    yield state
    log.close()


async def run_for(worker: ReplicationWorker, state: NodeState, seconds: float) -> None:
    term = state.current_term
    assert term is not None
    try:
        async with asyncio.timeout(seconds):
            await worker.replicate_term(term)
    except TimeoutError:
        pass


async def test_a_mismatch_walks_back_and_resends_from_the_hint(primary: NodeState) -> None:
    for key in "abc":
        primary.log.append(1, key, key)
    primary.try_promote(1, math.inf, standby_in_sync=True)
    standby = ScriptedStandby((200, reply("mismatch", 1, 0)))
    rpc = RpcClient(1.0, httpx.MockTransport(standby))

    await run_for(ReplicationWorker(primary, rpc, CONFIG), primary, 0.1)

    first, second = standby.requests[0], standby.requests[1]
    assert (first["prevIndex"], first["entries"], first["leaderLastIndex"]) == (3, [], 3)
    assert (second["prevIndex"], len(second["entries"])) == (0, 3)  # type: ignore[arg-type]
    assert primary.current_term.match_index == 3  # type: ignore[union-attr]
    await rpc.aclose()


async def test_a_newer_epoch_in_a_rejection_deposes_this_primary(primary: NodeState) -> None:
    primary.try_promote(1, math.inf, standby_in_sync=True)
    term = primary.current_term
    standby = ScriptedStandby((200, {**reply("rejected", 4, 0), "promiseMs": 0}))
    rpc = RpcClient(1.0, httpx.MockTransport(standby))

    await run_for(ReplicationWorker(primary, rpc, CONFIG), primary, 1.0)

    assert term is not None and term.is_ended
    assert (primary.current_term, primary.epoch) == (None, 4)
    await rpc.aclose()


async def test_errors_and_plain_rejections_are_retried(primary: NodeState) -> None:
    primary.try_promote(1, math.inf, standby_in_sync=True)
    standby = ScriptedStandby((500, {"error": "boom"}), (200, {"status": "weird"}), (200, reply("rejected", 1, 0)))
    rpc = RpcClient(1.0, httpx.MockTransport(standby))

    await run_for(ReplicationWorker(primary, rpc, CONFIG), primary, 0.2)

    assert len(standby.requests) >= 4
    assert primary.current_term is not None
    await rpc.aclose()


async def test_a_standby_claiming_more_than_this_log_has_ends_the_loop_quietly(primary: NodeState) -> None:
    primary.try_promote(1, math.inf, standby_in_sync=True)
    standby = ScriptedStandby((200, reply("ok", 1, 10)))
    rpc = RpcClient(1.0, httpx.MockTransport(standby))

    await run_for(ReplicationWorker(primary, rpc, CONFIG), primary, 1.0)

    assert len(standby.requests) == 1
    await rpc.aclose()


async def test_a_term_that_starts_behind_asks_to_go_solo_and_reports_in_sync_once_caught_up(
    primary: NodeState,
) -> None:
    primary.log.append(1, "a", "a")
    primary.try_promote(2, math.inf, standby_in_sync=False)
    rpc = RpcClient(1.0, httpx.MockTransport(ScriptedStandby()))

    await run_for(ReplicationWorker(primary, rpc, CONFIG), primary, 0.1)

    assert primary.current_term.peer_state is PeerState.IN_SYNC  # type: ignore[union-attr]
    await rpc.aclose()


async def test_the_worker_moves_on_to_the_next_term(primary: NodeState) -> None:
    standby = ScriptedStandby()
    rpc = RpcClient(1.0, httpx.MockTransport(standby))
    worker = asyncio.ensure_future(ReplicationWorker(primary, rpc, CONFIG).run())

    primary.try_promote(1, math.inf, standby_in_sync=True)
    await asyncio.sleep(0.02)
    primary.try_promote(2, math.inf, standby_in_sync=True)  # ends term 1 and its replication task
    await asyncio.sleep(0.02)
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    assert {request["epoch"] for request in standby.requests} == {1, 2}
    assert worker.cancelled()
    await rpc.aclose()
