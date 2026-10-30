using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Monolith.Modules.Catalog;

public static class CatalogEndpoints
{
    /// <summary>Catalog routes, identical whether hosted by the monolith or by CatalogService.</summary>
    public static IEndpointRouteBuilder MapCatalogEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/products", (ICatalog catalog, CancellationToken ct) => catalog.ListAsync(ct));

        app.MapGet("/products/{id}", (string id, ICatalog catalog, CancellationToken ct) =>
            catalog.GetAsync(CatalogRules.ParseId(id, "product id"), ct));

        app.MapPost("/products/{id}/reserve", async (string id, HttpRequest request, ICatalog catalog, CancellationToken ct) =>
        {
            var productId = CatalogRules.ParseId(id, "product id");
            var body = await request.ReadJsonAsync<ReserveRequest>(ct);
            return await catalog.ReserveAsync(productId, body.Quantity, ct);
        });

        return app;
    }
}
