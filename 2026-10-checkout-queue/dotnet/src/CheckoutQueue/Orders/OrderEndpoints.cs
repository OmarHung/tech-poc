using CheckoutQueue.Http;

namespace CheckoutQueue.Orders;

public sealed record CheckoutRequest(string? TicketId, int? Quantity);

public static class OrderEndpoints
{
    public static IEndpointRouteBuilder MapOrderEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/checkout", async (HttpRequest request, OrderStore orders, CheckoutQueueConfig config, CancellationToken ct) =>
        {
            var body = await request.ReadJsonAsync<CheckoutRequest>(ct);
            var ticketId = HttpExtensions.ValidId(body.TicketId, "ticketId");
            var quantity = body.Quantity is { } q && q >= 1 && q <= config.MaxQuantityPerOrder
                ? q
                : throw ApiException.BadRequest($"quantity must be an integer from 1 to {config.MaxQuantityPerOrder}");

            var result = await orders.CheckoutAsync(ticketId, quantity, ct);
            return result.Outcome switch
            {
                CheckoutOutcome.Created => Results.Created($"/orders/{result.Order!.OrderId}", result.Order),
                CheckoutOutcome.Existing => Results.Ok(result.Order),
                CheckoutOutcome.TicketNotFound => throw ApiException.NotFound("ticket_not_found", "ticket not found"),
                CheckoutOutcome.NotAdmitted => throw ApiException.Forbidden(
                    "not_admitted", $"ticket is not admitted to checkout (status: {result.Detail})"),
                CheckoutOutcome.AdmissionExpired => throw ApiException.Gone(
                    "admission_expired", "checkout window has closed; join the queue again"),
                CheckoutOutcome.TicketUsed => throw ApiException.Conflict(
                    "ticket_used", $"this ticket already placed order {result.Detail} with a different quantity"),
                CheckoutOutcome.SoldOut => throw ApiException.Conflict("sold_out", "sold out"),
                CheckoutOutcome.InsufficientStock => throw ApiException.Conflict(
                    "insufficient_stock", $"only {result.Detail} left right now; stock held by unpaid orders may come back"),
                _ => throw new InvalidOperationException($"unhandled checkout outcome {result.Outcome}"),
            };
        });

        app.MapGet("/orders/{orderId}", async (string orderId, OrderStore orders, CancellationToken ct) =>
        {
            var order = await orders.GetOrderAsync(HttpExtensions.ValidId(orderId, "order id"), ct);
            return order is null ? throw OrderNotFound() : Results.Ok(order);
        });

        // 201 when this request charged, 200 when an earlier one already had: retries are safe.
        app.MapPost("/orders/{orderId}/pay", async (string orderId, OrderStore orders, CancellationToken ct) =>
        {
            var result = await orders.PayAsync(HttpExtensions.ValidId(orderId, "order id"), ct);
            return result.Outcome switch
            {
                PayOutcome.Paid => Results.Created($"/orders/{orderId}", result.Order),
                PayOutcome.AlreadyPaid => Results.Ok(result.Order),
                PayOutcome.OrderExpired => throw ApiException.Gone("order_expired", "payment window has closed; the stock was released"),
                PayOutcome.OrderNotFound => throw OrderNotFound(),
                _ => throw new InvalidOperationException($"unhandled pay outcome {result.Outcome}"),
            };
        });

        return app;
    }

    private static ApiException OrderNotFound() => ApiException.NotFound("order_not_found", "order not found");
}
