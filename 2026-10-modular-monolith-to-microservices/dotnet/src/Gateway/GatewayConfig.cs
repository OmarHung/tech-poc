namespace Gateway;

public sealed record GatewayConfig(int Port, Upstreams Upstreams, CatalogRoute InitialRoute)
{
    public static readonly TimeSpan UpstreamTimeout = TimeSpan.FromSeconds(5);

    public static GatewayConfig FromEnvironment()
    {
        var rawPort = Env("PORT", "8080");
        if (!int.TryParse(rawPort, out var port) || port is < 1 or > 65535)
        {
            throw new InvalidOperationException($"PORT must be between 1 and 65535, got '{rawPort}'");
        }
        var rawRoute = Env("CATALOG_ROUTE", "monolith");
        if (!CatalogRoutes.TryParse(rawRoute, out var route))
        {
            throw new InvalidOperationException($"CATALOG_ROUTE must be 'monolith' or 'service', got '{rawRoute}'");
        }
        var upstreams = new Upstreams(Env("MONOLITH_URL", "http://127.0.0.1:8081"), Env("CATALOG_URL", "http://127.0.0.1:8082"));
        return new GatewayConfig(port, upstreams, route);
    }

    private static string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) ?? fallback;
}

public sealed record RoutesBody(string Catalog);

public sealed record RouteUpdate(string? Catalog);

public sealed record GatewayError(string Error);
