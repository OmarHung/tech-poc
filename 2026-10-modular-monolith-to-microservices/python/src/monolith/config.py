from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal, cast

CatalogMode = Literal["inprocess", "remote"]
CATALOG_MODES: tuple[CatalogMode, ...] = ("inprocess", "remote")
DEFAULT_CATALOG_URL = "http://127.0.0.1:8082"


@dataclass(frozen=True)
class MonolithConfig:
    # inprocess: catalog module runs inside the monolith (before extraction).
    # remote: catalog lives in catalog-service and is called over HTTP (after extraction).
    catalog_mode: CatalogMode = "inprocess"
    catalog_url: str = DEFAULT_CATALOG_URL

    @classmethod
    def from_env(cls) -> MonolithConfig:
        mode = os.environ.get("CATALOG_MODE", "inprocess")
        if mode not in CATALOG_MODES:
            raise ValueError(f"CATALOG_MODE must be 'inprocess' or 'remote', got '{mode}'")
        return cls(
            catalog_mode=cast(CatalogMode, mode),
            catalog_url=os.environ.get("CATALOG_URL", DEFAULT_CATALOG_URL),
        )
