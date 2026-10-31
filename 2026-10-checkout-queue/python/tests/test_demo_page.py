import re
from dataclasses import replace

import pytest
from redis.asyncio import Redis

from support import running_app

pytestmark = pytest.mark.anyio

NONCE_IN_POLICY = re.compile(r"script-src 'nonce-([A-Za-z0-9+/=]+)'")


async def test_demo_page_is_served_at_root_with_a_fresh_script_nonce_each_time(redis: Redis) -> None:
    async with running_app(redis) as app:
        first = await app.http.get("/")
        second = await app.http.get("/")

    assert first.status_code == 200
    assert first.headers["content-type"] == "text/html; charset=utf-8"
    assert first.headers["x-content-type-options"] == "nosniff"
    assert first.headers["x-served-by"] == "test-instance"
    csp = first.headers["content-security-policy"]
    nonce = NONCE_IN_POLICY.search(csp).group(1)
    assert nonce
    assert csp == (
        f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; connect-src 'self'; "
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    )
    assert "unsafe-inline" not in csp
    html = first.text
    assert f'<script nonce="{nonce}">' in html
    assert f'<style nonce="{nonce}">' in html
    assert "__CSP_NONCE__" not in html
    assert second.headers["content-security-policy"] != csp


async def test_admin_config_shows_the_sale_settings_but_not_where_redis_is(redis: Redis) -> None:
    async with running_app(redis, lambda c: replace(c, max_active=7)) as app:
        response = await app.http.get("/admin/config")

    body = response.json()
    assert response.status_code == 200
    assert body == {
        "instance": "test-instance",
        "saleId": body["saleId"],
        "maxActive": 7,
        "maxPerOrder": 2,
        "admissionWindowMs": 60_000,
        "paymentWindowMs": 120_000,
        "heartbeatTimeoutMs": 15_000,
        "pollIntervalMs": 1_000,
        "tickMs": 200,
    }
    assert body["saleId"].startswith("t")
    assert "redis" not in response.text.lower()
