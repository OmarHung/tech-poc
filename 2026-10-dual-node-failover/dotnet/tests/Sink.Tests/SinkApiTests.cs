using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Time.Testing;
using Shared;

namespace Sink.Tests;

public sealed class SinkApiTests : IAsyncLifetime
{
    private WebApplication _app = null!;
    private HttpClient _client = null!;

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddSink(new FakeTimeProvider());
        _app = builder.Build();
        _app.MapSink();
        await _app.StartAsync();
        _client = _app.GetTestClient();
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
    }

    [Fact]
    public async Task Results_AnswerWithTheirStatus()
    {
        var applied = await Post("a", "x:1");
        var duplicate = await Post("b", "x:1");
        var conflict = await Post("b", "x:2");
        var snapshot = await _client.GetFromJsonAsync<SinkSnapshot>("/results");

        Assert.Equal(HttpStatusCode.Created, applied.StatusCode);
        Assert.Equal(HttpStatusCode.OK, duplicate.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
        Assert.Equal("conflict", (await conflict.Content.ReadFromJsonAsync<ResultReply>())?.Status);
        Assert.Equal(new SinkSummary(3, 1, 1, 1, 0), snapshot?.Summary);
    }

    [Fact]
    public async Task BadResults_Are400()
    {
        var response = await _client.PostAsJsonAsync("/results", new { key = "bad key", node = "a", attempt = 0, value = "v" });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    private Task<HttpResponseMessage> Post(string node, string value) =>
        _client.PostAsJsonAsync("/results", new ResultRecord("event-1", node, 0, value));
}
