using Microsoft.AspNetCore.Http;

namespace Gateway.Tests;

public sealed class RoutingTableTests
{
    private static readonly Upstreams Upstreams = new("http://mono", "http://cat");

    [Theory]
    [InlineData("/products")]
    [InlineData("/products/1")]
    [InlineData("/products/1/reserve")]
    public void CatalogPaths_FollowTheSwitch(string path)
    {
        var routing = new RoutingTable(Upstreams, CatalogRoute.Monolith);
        Assert.Equal("http://mono", routing.Resolve(new PathString(path)));

        routing.Switch(CatalogRoute.Service);

        Assert.Equal("http://cat", routing.Resolve(new PathString(path)));
    }

    [Theory]
    [InlineData("/orders")]
    [InlineData("/orders/1")]
    [InlineData("/productsX")]
    [InlineData("/")]
    [InlineData("/health")]
    public void OtherPaths_AlwaysGoToTheMonolith(string path)
    {
        var routing = new RoutingTable(Upstreams, CatalogRoute.Service);

        Assert.Equal("http://mono", routing.Resolve(new PathString(path)));
    }

    [Theory]
    [InlineData("monolith", CatalogRoute.Monolith)]
    [InlineData("service", CatalogRoute.Service)]
    public void TryParse_AcceptsKnownRoutes(string value, CatalogRoute expected)
    {
        Assert.True(CatalogRoutes.TryParse(value, out var route));
        Assert.Equal(expected, route);
        Assert.Equal(value, route.ToWireName());
    }

    [Theory]
    [InlineData("Service")]
    [InlineData("")]
    [InlineData(null)]
    public void TryParse_RejectsUnknownRoutes(string? value)
    {
        Assert.False(CatalogRoutes.TryParse(value, out _));
    }
}
