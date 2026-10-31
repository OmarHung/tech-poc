"""The shared view every demo tab draws: all users, read without touching their heartbeat."""

from dataclasses import replace

import pytest
from redis.asyncio import Redis

from support import SaleUnderTest, running_app

pytestmark = pytest.mark.anyio


async def test_list_shows_each_users_current_ticket_with_its_order(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    order = await sale.place_order("buyer", quantity=2)
    waiting = (await sale.queue.join("waiter")).ticket

    board = await sale.board.list()

    assert [e.ticket.user_id for e in board] == ["waiter", "buyer"]
    assert (board[0].ticket.ticket_id, board[0].ticket.status, board[0].ticket.position) == (
        waiting.ticket_id,
        "waiting",
        1,
    )
    assert board[0].order is None
    assert (board[1].order.order_id, board[1].order.status, board[1].order.quantity) == (
        order.order_id,
        "pending_payment",
        2,
    )


async def test_list_shows_only_the_new_ticket_after_a_user_lines_up_again(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis)
    first = (await sale.join_and_admit("slow"))[0]
    sale.clock.advance(60_000)
    second = (await sale.queue.join("slow")).ticket

    board = await sale.board.list()

    assert len(board) == 1
    assert board[0].ticket.ticket_id != first.ticket_id
    assert board[0].ticket.ticket_id == second.ticket_id


async def test_list_is_not_a_heartbeat_so_watchers_cannot_keep_a_gone_user_in_line(redis: Redis) -> None:
    sale = await SaleUnderTest.start(redis, lambda c: replace(c, max_active=1))
    await sale.queue.join("gone")

    sale.clock.advance(16_000)
    await sale.board.list()
    await sale.queue.admit()

    assert (await sale.board.list())[0].ticket.status == "abandoned"


async def test_admin_tickets_returns_ticket_and_order_objects(redis: Redis) -> None:
    async with running_app(redis) as app:
        await app.http.post("/queue/join", json={"userId": "alice"})
        bob = (await app.http.post("/queue/join", json={"userId": "bob"})).json()["ticketId"]
        await app.maintenance.run_once()
        await app.http.post("/checkout", json={"ticketId": bob, "quantity": 1})

        response = await app.http.get("/admin/tickets")

    body = response.json()
    assert response.status_code == 200
    assert [e["ticket"]["userId"] for e in body] == ["bob", "alice"]
    assert body[0]["order"]["status"] == "pending_payment"
    assert body[1]["ticket"]["status"] == "admitted"
    assert "order" not in body[1], "no order yet, so the field is left out"
