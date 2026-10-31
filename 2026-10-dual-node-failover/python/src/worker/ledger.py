"""What a hot-hot worker has computed so far: running balances per account, and the last event it applied. It depends
only on the events and their order, so two workers that saw the same events hold the same ledger."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from shared.wire import JsonObject, WireError
from shared.work_contracts import LoggedEvent


@dataclass(frozen=True)
class Ledger:
    offset: int
    balances: Mapping[str, int]
    """Read-only; apply returns a new ledger."""

    def apply(self, logged: LoggedEvent) -> tuple[Ledger, int]:
        """Applies the next event and returns the new ledger with the account's balance after it."""
        if logged.seq != self.offset + 1:
            raise ValueError(f"event {logged.seq} does not follow {self.offset}")
        balance = self.balances.get(logged.account, 0) + logged.amount
        return Ledger(logged.seq, MappingProxyType({**self.balances, logged.account: balance})), balance

    def skip_to(self, offset: int) -> Ledger:
        """Moves the position without applying the events in between: whatever they changed is now missing."""
        return replace(self, offset=offset)

    def to_file(self) -> dict[str, object]:
        """The checkpoint on disk, and the body of GET /state."""
        return {"offset": self.offset, "balances": dict(self.balances)}

    @staticmethod
    def from_file(value: object) -> Ledger:
        fields = JsonObject(value)
        balances = fields.integers("balances")
        if balances is None:
            raise WireError(fields.path("balances"))
        return Ledger(fields.integer("offset"), MappingProxyType(balances))


EMPTY_LEDGER = Ledger(0, MappingProxyType({}))
