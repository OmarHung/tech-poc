"""This worker's own view of its task lease. It starts at the send time of the request that won or renewed it and ends
a margin early, so the worker gives up before the upstream hands the task to someone else."""

from __future__ import annotations

from shared.clock import Clock

SAFETY_FRACTION = 0.9
"""The worker's lease ends after 90% of the TTL: a tenth is left for clock drift."""


def lease_end(sent_at: float, ttl_ms: int) -> float:
    """End of a lease that started at `sent_at` (seconds on the worker's monotonic clock)."""
    return sent_at + ttl_ms * SAFETY_FRACTION / 1000


class LeaseTracker:
    def __init__(self, clock: Clock, until: float) -> None:
        self._clock = clock
        self._until = until
        self._revoked = False

    @property
    def is_valid(self) -> bool:
        return not self._revoked and self._clock.now() < self._until

    @property
    def remaining(self) -> float:
        """Seconds left on the lease; zero or less once it is gone."""
        return 0.0 if self._revoked else self._until - self._clock.now()

    def extend_to(self, until: float) -> None:
        """A renewal. One that was sent earlier than the last can arrive later; it never shortens the lease."""
        self._until = max(self._until, until)

    def revoke(self) -> None:
        """The upstream said another attempt owns the task now."""
        self._revoked = True
