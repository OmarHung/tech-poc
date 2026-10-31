"""Goes through the real HTTP stack: strict JSON, error mapping, served-by header, wire names."""

from dataclasses import replace

import httpx
import pytest
from redis.asyncio import Redis

from checkout_queue.sale_redis import ScriptReply, connect
from support import AppUnderTest, running_app

pytestmark = pytest.mark.anyio

UNKNOWN_ID = "a" * 32


async def join(app: AppUnderTest, user_id: str) -> str:
    response = await app.http.post("/queue/join", json={"userId": user_id})
    response.raise_for_status()
    return response.json()["ticketId"]


def assert_error(response: httpx.Response, expected: int) -> str:
    """Asserts the status and the {"error": ..., "code": ...} shape; returns the code."""
    assert response.status_code == expected, response.text
    body = response.json()
    assert body["error"].strip()
    return body["code"]


async def test_health_names_the_instance(redis: Redis) -> None:
    async with running_app(redis) as app:
        response = await app.http.get("/health")

    assert response.status_code == 200
    assert response.headers["x-served-by"] == "test-instance"
    assert response.json()["status"] == "ok"


async def test_full_flow_join_wait_checkout_pay(redis: Redis) -> None:
    async with running_app(redis) as app:
        joined = await app.http.post("/queue/join", json={"userId": "alice"})
        ticket = joined.json()
        ticket_id = ticket["ticketId"]
        assert joined.status_code == 201
        assert joined.headers["location"] == f"/queue/tickets/{ticket_id}"
        assert (ticket["status"], ticket["position"]) == ("waiting", 1)
        assert "orderId" not in ticket, "null fields are left out"
        assert "estimatedWaitSeconds" not in ticket

        await app.maintenance.run_once()
        admitted = (await app.http.get(f"/queue/tickets/{ticket_id}")).json()
        assert admitted["status"] == "admitted"

        checkout = await app.http.post("/checkout", json={"ticketId": ticket_id, "quantity": 2})
        order = checkout.json()
        order_id = order["orderId"]
        assert checkout.status_code == 201
        assert checkout.headers["location"] == f"/orders/{order_id}"
        assert order["status"] == "pending_payment"
        assert "paidAtMs" not in order

        repeat = await app.http.post("/checkout", json={"ticketId": ticket_id, "quantity": 2})
        assert repeat.status_code == 200
        assert repeat.json()["orderId"] == order_id

        pay = await app.http.post(f"/orders/{order_id}/pay")
        pay_again = await app.http.post(f"/orders/{order_id}/pay")
        assert pay.status_code == 201
        assert pay.headers["location"] == f"/orders/{order_id}"
        assert pay_again.status_code == 200
        paid = (await app.http.get(f"/orders/{order_id}")).json()
        assert (paid["status"], paid["paidAtMs"]) == ("paid", app.clock.now_ms())

        audit = (await app.http.get("/admin/audit")).json()
        assert (audit["stock"], audit["oversold"], audit["charges"], audit["stockBalanced"]) == (8, 0, 1, True)


@pytest.mark.parametrize(
    ("method", "path", "body", "expected"),
    [
        ("POST", "/queue/join", '{"userId":""}', 400),
        ("POST", "/queue/join", '{"userId":"has space"}', 400),
        ("POST", "/queue/join", '{"userId":"' + "a" * 65 + '"}', 400),
        ("POST", "/queue/join", '{"userId":"alice\\n"}', 400),
        ("POST", "/queue/join", '{"userId":42}', 400),
        ("POST", "/queue/join", '{"userId":null}', 400),
        ("POST", "/queue/join", "{}", 400),
        ("POST", "/queue/join", "[]", 400),
        ("POST", "/queue/join", "null", 400),
        ("POST", "/queue/join", "{not json", 400),
        ("POST", "/queue/join", "", 400),
        ("POST", "/queue/join", None, 400),
        ("GET", "/queue/tickets/not-an-id", None, 400),
        ("GET", "/queue/tickets/" + "A" * 32, None, 400),
        ("GET", "/queue/tickets/{unknown}", None, 404),
        ("GET", "/queue/tickets/{unknown}/events", None, 404),
        ("GET", "/queue/tickets/not-an-id/events", None, 400),
        ("POST", "/checkout", '{"ticketId":"{unknown}","quantity":"1"}', 400),
        ("POST", "/checkout", '{"ticketId":"{unknown}","quantity":true}', 400),
        ("POST", "/checkout", '{"ticketId":"{unknown}","quantity":1.0}', 400),
        ("POST", "/checkout", '{"ticketId":"{unknown}","quantity":NaN}', 400),
        ("POST", "/checkout", '{"ticketId":"{unknown}","quantity":0}', 400),
        ("POST", "/checkout", '{"ticketId":"{unknown}","quantity":3}', 400),
        ("POST", "/checkout", '{"ticketId":"{unknown}"}', 400),
        ("POST", "/checkout", '{"ticketId":"ABC","quantity":1}', 400),
        ("POST", "/checkout", '{"ticketId":"{unknown}","quantity":1}', 404),
        ("GET", "/orders/not-an-id", None, 400),
        ("GET", "/orders/{unknown}", None, 404),
        ("POST", "/orders/{unknown}/pay", None, 404),
        ("POST", "/orders/not-an-id/pay", None, 400),
    ],
)
async def test_errors_use_status_code_and_error_body(
    redis: Redis, method: str, path: str, body: str | None, expected: int
) -> None:
    async with running_app(redis) as app:
        content = None if body is None else body.replace("{unknown}", UNKNOWN_ID).encode()
        headers = {} if body is None else {"content-type": "application/json"}
        response = await app.http.request(
            method, path.replace("{unknown}", UNKNOWN_ID), content=content, headers=headers
        )

    assert_error(response, expected)
    assert response.headers["x-served-by"] == "test-instance"


async def test_a_body_that_is_not_declared_json_is_rejected(redis: Redis) -> None:
    async with running_app(redis) as app:
        plain = await app.http.post(
            "/queue/join", content=b'{"userId":"alice"}', headers={"content-type": "text/plain"}
        )
        suffixed = await app.http.post(
            "/queue/join", content=b'{"userId":"alice"}', headers={"content-type": "application/merge-patch+json"}
        )

    assert assert_error(plain, 400) == "invalid_request"
    assert suffixed.status_code == 201


async def test_unknown_routes_and_methods_use_the_error_body(redis: Redis) -> None:
    async with running_app(redis) as app:
        missing = await app.http.get("/nope")
        trailing_slash = await app.http.get("/health/")
        wrong_method = await app.http.delete("/health")

    assert assert_error(missing, 404) == "not_found"
    assert assert_error(trailing_slash, 404) == "not_found"
    assert assert_error(wrong_method, 405) == "method_not_allowed"
    assert missing.headers["x-served-by"] == "test-instance"


async def test_checkout_before_admission_is_403(redis: Redis) -> None:
    async with running_app(redis) as app:
        ticket_id = await join(app, "eager")

        response = await app.http.post("/checkout", json={"ticketId": ticket_id, "quantity": 1})

    assert assert_error(response, 403) == "not_admitted"
    assert "waiting" in response.json()["error"]


async def test_checkout_after_the_admission_window_is_410(redis: Redis) -> None:
    async with running_app(redis) as app:
        ticket_id = await join(app, "slow")
        await app.maintenance.run_once()
        app.clock.advance(60_000)

        response = await app.http.post("/checkout", json={"ticketId": ticket_id, "quantity": 1})

    assert assert_error(response, 410) == "admission_expired"


async def test_checkout_conflicts_are_reported_with_their_code(redis: Redis) -> None:
    async with running_app(redis, lambda c: replace(c, initial_stock=3)) as app:
        first = await join(app, "first")
        second = await join(app, "second")
        await app.maintenance.run_once()
        await app.http.post("/checkout", json={"ticketId": first, "quantity": 2})

        changed_mind = await app.http.post("/checkout", json={"ticketId": first, "quantity": 1})
        too_many = await app.http.post("/checkout", json={"ticketId": second, "quantity": 2})

    assert assert_error(changed_mind, 409) == "ticket_used"
    assert assert_error(too_many, 409) == "insufficient_stock"


async def test_checkout_when_sold_out_is_409(redis: Redis) -> None:
    async with running_app(redis, lambda c: replace(c, initial_stock=2)) as app:
        first = await join(app, "first")
        second = await join(app, "second")
        await app.maintenance.run_once()
        order = (await app.http.post("/checkout", json={"ticketId": first, "quantity": 2})).json()
        await app.http.post(f"/orders/{order['orderId']}/pay")

        response = await app.http.post("/checkout", json={"ticketId": second, "quantity": 1})

    assert assert_error(response, 409) == "sold_out"


async def test_field_names_match_regardless_of_case_like_the_dotnet_reference(redis: Redis) -> None:
    async with running_app(redis) as app:
        joined = await app.http.post("/queue/join", json={"UserId": "alice"})
        await app.maintenance.run_once()
        ordered = await app.http.post("/checkout", json={"TICKETID": joined.json()["ticketId"], "Quantity": 1})

    assert joined.status_code == 201
    assert ordered.status_code == 201


async def test_a_body_over_the_size_limit_is_rejected_without_reading_it_all(redis: Redis) -> None:
    async with running_app(redis) as app:
        huge = b'{"userId":"alice","padding":"' + b"x" * (2 * 1024 * 1024) + b'"}'
        response = await app.http.post("/queue/join", content=huge, headers={"content-type": "application/json"})

    assert assert_error(response, 400) == "invalid_request"


async def test_join_when_sold_out_is_409(redis: Redis) -> None:
    async with running_app(redis, lambda c: replace(c, initial_stock=0)) as app:
        response = await app.http.post("/queue/join", json={"userId": "late"})

    assert assert_error(response, 409) == "sold_out"


async def test_join_again_is_200_with_the_same_ticket(redis: Redis) -> None:
    async with running_app(redis) as app:
        first = await join(app, "alice")

        again = await app.http.post("/queue/join", json={"userId": "alice"})

    assert again.status_code == 200
    assert "location" not in again.headers
    assert again.json()["ticketId"] == first


async def test_pay_after_the_deadline_is_410(redis: Redis) -> None:
    async with running_app(redis) as app:
        ticket_id = await join(app, "late-payer")
        await app.maintenance.run_once()
        order = (await app.http.post("/checkout", json={"ticketId": ticket_id, "quantity": 1})).json()
        app.clock.advance(120_000)

        response = await app.http.post(f"/orders/{order['orderId']}/pay")

    assert assert_error(response, 410) == "order_expired"


async def test_redis_down_is_503_not_a_crash() -> None:
    dead_redis = connect("127.0.0.1:1")
    try:
        async with running_app(dead_redis, initialize_sale=False) as app:
            health = await app.http.get("/health")
            joined = await app.http.post("/queue/join", json={"userId": "alice"})
            board = await app.http.get("/admin/tickets")
            events = await app.http.get(f"/queue/tickets/{UNKNOWN_ID}/events")
            open_streams = app.services.streams.ticket_ids
    finally:
        await dead_redis.aclose(close_connection_pool=True)

    assert assert_error(health, 503) == "unavailable"
    assert assert_error(joined, 503) == "unavailable"
    assert assert_error(board, 503) == "unavailable"
    assert assert_error(events, 503) == "unavailable"
    assert open_streams == [], "a stream that never started is unregistered"
    assert joined.json()["error"] == "storage unavailable, retry shortly"


async def test_a_script_outcome_nobody_handles_is_a_500_with_the_error_body(redis: Redis) -> None:
    async with running_app(redis) as app:

        async def surprising_script(name: str, *_: object) -> ScriptReply:
            return ScriptReply(name, "surprise", {})

        app.services.redis.run = surprising_script  # type: ignore[method-assign]
        response = await app.http.post("/queue/join", json={"userId": "alice"})

    assert assert_error(response, 500) == "internal"
    assert response.headers["x-served-by"] == "test-instance"
