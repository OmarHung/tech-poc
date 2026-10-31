"""Counters behind GET /health, updated by the work loop."""

from __future__ import annotations

from typing import Literal

from worker.config import WorkerConfig
from worker.ledger import EMPTY_LEDGER, Ledger

Phase = Literal["working", "delivering", "completing"]
"""working: doing the task; delivering: writing its result to the sink; completing: reporting it done upstream."""
Outcome = Literal["completed", "abandoned", "stale"]
"""abandoned: the lease was lost (or the sink unreachable for a whole lease) before the result was written.
stale: another attempt owned the task by the time it was reported done."""


class WorkerStatus:
    def __init__(self) -> None:
        self.ledger: Ledger = EMPTY_LEDGER
        """The hot-hot worker's last checkpoint."""
        self._sink_replies: dict[str, int] = {}
        self._outcomes: dict[Outcome, int] = {"completed": 0, "abandoned": 0, "stale": 0}
        self._current_task: int | None = None
        self._phase: Phase | None = None

    @property
    def phase(self) -> Phase | None:
        return self._phase

    def sink_replied(self, status: str) -> None:
        self._sink_replies = {**self._sink_replies, status: self._sink_replies.get(status, 0) + 1}

    def task_phase(self, task_id: int, phase: Phase) -> None:
        self._current_task, self._phase = task_id, phase

    def task_finished(self, outcome: Outcome) -> None:
        self._current_task, self._phase = None, None
        self._outcomes = {**self._outcomes, outcome: self._outcomes[outcome] + 1}

    def health(self, config: WorkerConfig) -> dict[str, object]:
        return {
            "status": "ok",
            "worker": config.worker_id,
            "mode": config.mode.value,
            "offset": self.ledger.offset,
            "sinkReplies": dict(sorted(self._sink_replies.items())),
            "completed": self._outcomes["completed"],
            "abandoned": self._outcomes["abandoned"],
            "staleCompletions": self._outcomes["stale"],
            "currentTask": self._current_task,
            "phase": self._phase,
        }
