"""A one-file control page at / for trying the queue by hand (shared/demo/index.html, the same page
every language version serves). It only calls this instance's own API; open a second instance in
another tab to see both serve one sale."""

from __future__ import annotations

import base64
import secrets
from functools import cache

from fastapi import APIRouter
from starlette.responses import HTMLResponse

from .sale_redis import SHARED_DIR

DEMO_PAGE = SHARED_DIR / "demo" / "index.html"
NONCE_PLACEHOLDER = "__CSP_NONCE__"


@cache
def _page() -> str:
    return DEMO_PAGE.read_text(encoding="utf-8")


def content_security_policy(nonce: str) -> str:
    return (
        f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; connect-src 'self'; "
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    )


def create_demo_router() -> APIRouter:
    _page()  # read at startup: a missing file should stop the process, not the first visitor
    router = APIRouter()

    @router.get("/")
    async def demo_page() -> HTMLResponse:
        # Inline script and style run only with this request's nonce, so injected markup cannot run.
        nonce = base64.b64encode(secrets.token_bytes(16)).decode()
        headers = {"content-security-policy": content_security_policy(nonce), "x-content-type-options": "nosniff"}
        return HTMLResponse(_page().replace(NONCE_PLACEHOLDER, nonce), headers=headers)

    return router
