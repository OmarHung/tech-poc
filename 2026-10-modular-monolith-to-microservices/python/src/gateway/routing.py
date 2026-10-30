from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

CatalogRoute = Literal["monolith", "service"]
CATALOG_ROUTES: tuple[CatalogRoute, ...] = ("monolith", "service")


def parse_route(value: object) -> CatalogRoute:
    if value in CATALOG_ROUTES:
        return cast(CatalogRoute, value)
    raise ValueError(f"catalog must be 'monolith' or 'service', got {value!r}")


@dataclass(frozen=True)
class Upstreams:
    monolith: str
    catalog: str


def is_catalog_path(path: str) -> bool:
    return path == "/products" or path.startswith("/products/")


def resolve_upstream(path: str, route: CatalogRoute, upstreams: Upstreams) -> str:
    """Everything goes to the monolith except catalog paths once they are switched over."""
    if route == "service" and is_catalog_path(path):
        return upstreams.catalog
    return upstreams.monolith


class RoutingTable:
    """Current catalog route. Switching is a single reference assignment, so
    in-flight requests keep the upstream they already resolved."""

    def __init__(self, upstreams: Upstreams, route: CatalogRoute) -> None:
        self._upstreams = upstreams
        self._route = route

    @property
    def catalog_route(self) -> CatalogRoute:
        return self._route

    def switch(self, route: CatalogRoute) -> None:
        self._route = route

    def resolve(self, path: str) -> str:
        return resolve_upstream(path, self._route, self._upstreams)
