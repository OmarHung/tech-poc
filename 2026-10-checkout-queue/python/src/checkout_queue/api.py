"""The HTTP contract every language version implements (see ../../../README.md, HTTP 介面)."""

from __future__ import annotations

from fastapi import APIRouter, Request
from starlette.responses import Response

from .services import Services
from .web import ApiError, field, json_response, read_json_object, valid_id, valid_quantity, valid_user_id


def create_api_router(services: Services) -> APIRouter:
    router = APIRouter()
    config = services.config

    @router.get("/health")
    async def health() -> Response:
        await services.redis.ping()
        return json_response({"status": "ok", "instance": config.instance_name, "saleId": config.sale_id})

    # ---------------------------------------------------------------- queue

    @router.post("/queue/join")
    async def join(request: Request) -> Response:
        body = await read_json_object(request)
        result = await services.queue.join(valid_user_id(field(body, "userId")))
        if result.ticket is None:
            raise ApiError.conflict("sold_out", "sold out")
        if result.outcome == "created":
            location = f"/queue/tickets/{result.ticket.ticket_id}"
            return json_response(result.ticket.to_json(), 201, {"location": location})
        return json_response(result.ticket.to_json())

    @router.get("/queue/tickets/{ticket_id}")
    async def get_ticket(ticket_id: str) -> Response:
        ticket = await services.queue.get_ticket(valid_id(ticket_id, "ticket id"))
        if ticket is None:
            raise ticket_not_found()
        return json_response(ticket.to_json())

    # ---------------------------------------------------------------- orders

    @router.post("/checkout")
    async def checkout(request: Request) -> Response:
        body = await read_json_object(request)
        ticket_id = valid_id(field(body, "ticketId"), "ticketId")
        quantity = valid_quantity(field(body, "quantity"), config.max_per_order)

        result = await services.orders.checkout(ticket_id, quantity)
        if result.order is not None:
            if result.outcome == "created":
                return json_response(result.order.to_json(), 201, {"location": f"/orders/{result.order.order_id}"})
            return json_response(result.order.to_json())
        raise _checkout_rejection(result.outcome, result.detail)

    @router.get("/orders/{order_id}")
    async def get_order(order_id: str) -> Response:
        order = await services.orders.get_order(valid_id(order_id, "order id"))
        if order is None:
            raise order_not_found()
        return json_response(order.to_json())

    # 201 when this request charged, 200 when an earlier one already had: retries are safe.
    @router.post("/orders/{order_id}/pay")
    async def pay(order_id: str) -> Response:
        result = await services.orders.pay(valid_id(order_id, "order id"))
        if result.outcome == "order_not_found" or result.order is None:
            raise order_not_found()
        if result.outcome == "order_expired":
            raise ApiError.gone("order_expired", "payment window has closed; the stock was released")
        if result.outcome == "paid":
            return json_response(result.order.to_json(), 201, {"location": f"/orders/{order_id}"})
        return json_response(result.order.to_json())

    # ---------------------------------------------------------------- admin
    # Unauthenticated on purpose for the PoC: the process only listens on 127.0.0.1.

    @router.get("/admin/audit")
    async def audit() -> Response:
        return json_response((await services.audit.get()).to_json())

    @router.get("/admin/config")
    async def sale_config() -> Response:
        return json_response(services.settings)

    @router.get("/admin/tickets")
    async def board() -> Response:
        return json_response([entry.to_json() for entry in await services.board.list()])

    return router


def ticket_not_found() -> ApiError:
    return ApiError.not_found("ticket_not_found", "ticket not found")


def order_not_found() -> ApiError:
    return ApiError.not_found("order_not_found", "order not found")


def _checkout_rejection(outcome: str, detail: str | None) -> ApiError:
    match outcome:
        case "ticket_not_found":
            return ticket_not_found()
        case "not_admitted":
            return ApiError.forbidden("not_admitted", f"ticket is not admitted to checkout (status: {detail})")
        case "admission_expired":
            return ApiError.gone("admission_expired", "checkout window has closed; join the queue again")
        case "ticket_used":
            return ApiError.conflict(
                "ticket_used", f"this ticket already placed order {detail} with a different quantity"
            )
        case "sold_out":
            return ApiError.conflict("sold_out", "sold out")
        case "insufficient_stock":
            return ApiError.conflict(
                "insufficient_stock", f"only {detail} left right now; stock held by unpaid orders may come back"
            )
    raise RuntimeError(f"unhandled checkout outcome {outcome}")
