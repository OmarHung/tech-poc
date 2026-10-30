using Monolith.Modules.Catalog;

namespace Monolith.Modules.Orders;

public static class OrderEndpoints
{
    public static IEndpointRouteBuilder MapOrderEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/orders", async (HttpRequest request, OrderService orders, CancellationToken ct) =>
        {
            var body = await request.ReadJsonAsync<CreateOrderRequest>(ct);
            var order = await orders.PlaceAsync(body.ProductId, body.Quantity, ct);
            return Results.Created($"/orders/{order.Id}", order);
        });

        app.MapGet("/orders/{id}", (string id, OrderService orders) =>
            orders.Find(CatalogRules.ParseId(id, "order id")));

        return app;
    }
}
