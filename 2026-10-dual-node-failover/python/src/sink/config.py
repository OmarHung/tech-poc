from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from shared.env import EnvReader, Lookup


@dataclass(frozen=True)
class SinkConfig:
    port: int = 8123

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> SinkConfig:
        source = os.environ if environ is None else environ
        return cls.from_lookup(source.get)

    @classmethod
    def from_lookup(cls, lookup: Lookup) -> SinkConfig:
        return cls(port=EnvReader(lookup).port("PORT", cls().port))
