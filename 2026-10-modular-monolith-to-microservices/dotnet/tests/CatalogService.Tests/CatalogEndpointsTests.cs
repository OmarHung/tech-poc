using System.Net;
using System.Net.Http.Json;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

using CatalogService.Catalog;

namespace CatalogService.Tests;

/// <summary>Goes through the real middleware: strict JSON, error mapping, served-by header.</summary>
public sealed class CatalogEndpointsTests : IAsyncLifetime
{
    private WebApplication _app = null!;
    private HttpClient _client = null!;

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddStrictJson();
        builder.Services.AddSingleton<ICatalog>(InMemoryCatalog.Seeded());
        _app = builder.Build();
        _app.UseServedBy("test-host");
        _app.UseApiErrors();
        _app.MapCatalogEndpoints();
        await _app.StartAsync();
        _client = _app.GetTestClient();
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
    }

    [Fact]
    public async Task Reserve_ReturnsUpdatedProduct_AsCamelCaseJson()
    {
        var response = await _client.PostAsJsonAsync("/products/1/reserve", new { quantity = 2 });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("test-host", response.Headers.GetValues("X-Served-By").Single());
        Assert.Equal(new Product(1, "Mechanical Keyboard", 3200, 48), await response.Content.ReadFromJsonAsync<Product>());
    }

    [Theory]
    [InlineData("GET", "/products/999", null, HttpStatusCode.NotFound)]
    [InlineData("GET", "/products/abc", null, HttpStatusCode.BadRequest)]
    [InlineData("POST", "/products/1/reserve", """{"quantity":"2"}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/products/1/reserve", """{"quantity":0}""", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/products/1/reserve", "{not json", HttpStatusCode.BadRequest)]
    [InlineData("POST", "/products/1/reserve", """{"quantity":99}""", HttpStatusCode.Conflict)]
    public async Task Errors_UseStatusCode_AndErrorBody(string method, string path, string? body, HttpStatusCode expected)
    {
        using var request = new HttpRequestMessage(new HttpMethod(method), path);
        if (body is not null)
        {
            request.Content = new StringContent(body, Encoding.UTF8, "application/json");
        }

        var response = await _client.SendAsync(request);

        Assert.Equal(expected, response.StatusCode);
        var error = await response.Content.ReadFromJsonAsync<ErrorBody>();
        Assert.False(string.IsNullOrEmpty(error?.Error));
        Assert.DoesNotContain("System.", error!.Error, StringComparison.Ordinal);
    }
}
