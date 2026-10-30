namespace Gateway;

/// <summary>Which backend answers catalog paths (<c>/products</c> and below).</summary>
public enum CatalogRoute
{
    Monolith,
    Service,
}

public static class CatalogRoutes
{
    public static bool TryParse(string? value, out CatalogRoute route)
    {
        (var known, route) = value switch
        {
            "monolith" => (true, CatalogRoute.Monolith),
            "service" => (true, CatalogRoute.Service),
            _ => (false, CatalogRoute.Monolith),
        };
        return known;
    }

    public static string ToWireName(this CatalogRoute route) => route == CatalogRoute.Service ? "service" : "monolith";
}

public sealed record Upstreams(string Monolith, string Catalog);

/// <summary>
/// Current catalog route. Switching is a single volatile write, so in-flight
/// requests keep the upstream they already resolved and nothing is dropped.
/// </summary>
public sealed class RoutingTable(Upstreams upstreams, CatalogRoute initialRoute)
{
    private volatile CatalogRoute _catalogRoute = initialRoute;

    public CatalogRoute CatalogRoute => _catalogRoute;

    public void Switch(CatalogRoute route) => _catalogRoute = route;

    /// <summary>Everything goes to the monolith except catalog paths once they are switched over.</summary>
    public string Resolve(PathString path) =>
        _catalogRoute == CatalogRoute.Service && IsCatalogPath(path) ? upstreams.Catalog : upstreams.Monolith;

    public static bool IsCatalogPath(PathString path) =>
        path.StartsWithSegments("/products", StringComparison.Ordinal);
}
