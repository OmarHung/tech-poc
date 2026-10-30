using System.Net;
using System.Net.Http.Json;
using Monolith.Modules.Catalog;
using Microsoft.Extensions.Logging.Abstractions;

namespace Monolith.Tests;

public sealed class HttpCatalogClientTests
{
    /// <summary>Captures what was sent while the request is still alive; the client disposes it afterwards.</summary>
    private sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public string? LastPath { get; private set; }

        public string? LastBody { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastPath = request.RequestUri?.AbsolutePath;
            LastBody = request.Content is null ? null : await request.Content.ReadAsStringAsync(cancellationToken);
            return respond(request);
        }
    }

    private static HttpCatalogClient Client(HttpMessageHandler handler) =>
        new(new HttpClient(handler) { BaseAddress = new Uri("http://catalog") }, NullLogger<HttpCatalogClient>.Instance);

    private static HttpResponseMessage Json(HttpStatusCode status, object body) =>
        new(status) { Content = JsonContent.Create(body) };

    [Fact]
    public async Task ReserveAsync_PostsQuantity_AndReturnsProduct()
    {
        var handler = new StubHandler(_ => Json(HttpStatusCode.OK, new Product(1, "Keyboard", 300, 3)));

        var product = await Client(handler).ReserveAsync(1, 2);

        Assert.Equal(3, product.Stock);
        Assert.Equal("/products/1/reserve", handler.LastPath);
        Assert.Equal("""{"quantity":2}""", handler.LastBody);
    }

    [Fact]
    public async Task ListAsync_ReturnsProducts()
    {
        var handler = new StubHandler(_ => Json(HttpStatusCode.OK, CatalogSeed.Products));

        Assert.Equal(3, (await Client(handler).ListAsync()).Count);
    }

    [Theory]
    [InlineData(HttpStatusCode.BadRequest, typeof(InvalidInputException))]
    [InlineData(HttpStatusCode.NotFound, typeof(NotFoundException))]
    [InlineData(HttpStatusCode.Conflict, typeof(InsufficientStockException))]
    [InlineData(HttpStatusCode.InternalServerError, typeof(CatalogUnavailableException))]
    public async Task ErrorStatuses_MapBackToCatalogExceptions(HttpStatusCode status, Type expected)
    {
        var handler = new StubHandler(_ => Json(status, new ErrorBody("from catalog-service")));

        var error = await Assert.ThrowsAnyAsync<ApiException>(() => Client(handler).GetAsync(1));

        Assert.IsType(expected, error);
    }

    [Fact]
    public async Task KnownErrors_KeepTheServiceMessage()
    {
        var handler = new StubHandler(_ => Json(HttpStatusCode.NotFound, new ErrorBody("product 9 not found")));

        var error = await Assert.ThrowsAsync<NotFoundException>(() => Client(handler).GetAsync(9));

        Assert.Equal("product 9 not found", error.Message);
    }

    [Fact]
    public async Task NetworkFailure_IsReportedWithoutDetails()
    {
        var handler = new StubHandler(_ => throw new HttpRequestException("connection refused to 10.0.0.1"));

        var error = await Assert.ThrowsAsync<CatalogUnavailableException>(() => Client(handler).GetAsync(1));

        Assert.Equal("catalog unavailable", error.Message);
    }

    [Fact]
    public async Task UnexpectedBody_IsTreatedAsUnavailable()
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("not json") });

        await Assert.ThrowsAsync<CatalogUnavailableException>(() => Client(handler).GetAsync(1));
    }
}
