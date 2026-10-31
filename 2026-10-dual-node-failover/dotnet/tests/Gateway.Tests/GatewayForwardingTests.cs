using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Shared;

namespace Gateway.Tests;

/// <summary>The real gateway pipeline on a test server, in front of two fake nodes.</summary>
public sealed class GatewayForwardingTests : IAsyncLifetime
{
    private static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(5);

    private readonly FakeNodes _nodes = new();
    private WebApplication _app = null!;
    private HttpClient _client = null!;

    public async Task InitializeAsync()
    {
        var config = new GatewayConfig
        {
            Nodes = [new NodeTarget("a", new Uri("http://a/")), new NodeTarget("b", new Uri("http://b/"))],
            PollInterval = TimeSpan.FromMilliseconds(20),
        };
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddGateway(config, _nodes);
        _app = builder.Build();
        _app.MapGateway();
        await _app.StartAsync();
        _client = _app.GetTestClient();
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
    }

    [Fact]
    public async Task NoPrimary_Is503FromTheGateway()
    {
        _nodes.Set("a", "primary", 1);
        await WaitForPrimaryAsync("a");
        _nodes.Set("a", "standby", 1);
        await WaitForPrimaryAsync(null);

        var response = await _client.PutAsJsonAsync("/kv/k", new { value = "v" });

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        Assert.Equal("no primary available", (await response.Content.ReadFromJsonAsync<ErrorBody>())?.Error);
        Assert.Equal("gateway", ServedBy(response));
    }

    [Fact]
    public async Task Requests_GoToThePrimary()
    {
        _nodes.Set("a", "primary", 1);
        _nodes.Set("b", "standby", 1);
        await WaitForPrimaryAsync("a");

        var response = await _client.PutAsJsonAsync("/kv/k", new { value = "v" });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("node-a", ServedBy(response));
    }

    [Fact]
    public async Task Requests_FollowAFailover()
    {
        _nodes.Set("a", "primary", 1);
        _nodes.Set("b", "standby", 1);
        await WaitForPrimaryAsync("a");

        _nodes.Down["a"] = true;
        _nodes.Set("b", "primary", 2);
        await WaitForPrimaryAsync("b");
        var response = await _client.GetAsync("/kv/k");

        Assert.Equal("node-b", ServedBy(response));
    }

    [Fact]
    public async Task UnreachablePrimary_Is502()
    {
        _nodes.Set("a", "primary", 1);
        _nodes.Set("b", "standby", 1);
        await WaitForPrimaryAsync("a");
        _nodes.FailRequests["a"] = true;

        var response = await _client.GetAsync("/kv/k");

        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
        Assert.Equal("gateway", ServedBy(response));
    }

    private async Task WaitForPrimaryAsync(string? expected)
    {
        var deadline = DateTime.UtcNow + WaitLimit;
        GatewayHealth? health;
        while ((health = await _client.GetFromJsonAsync<GatewayHealth>("/health"))?.Primary != expected)
        {
            Assert.True(DateTime.UtcNow < deadline, $"gateway never settled on {expected ?? "no primary"}; last saw {health?.Primary}");
            await Task.Delay(10);
        }
    }

    private static string ServedBy(HttpResponseMessage response) =>
        response.Headers.GetValues(HostingExtensions.ServedByHeader).Single();

    private sealed record GatewayHealth(string? Primary, long? Epoch);

    private sealed class FakeNodes : HttpMessageHandler
    {
        private readonly ConcurrentDictionary<string, NodeStatus> _health = new();

        public ConcurrentDictionary<string, bool> Down { get; } = new();
        public ConcurrentDictionary<string, bool> FailRequests { get; } = new();

        public void Set(string node, string role, long epoch) => _health[node] = new NodeStatus(role, epoch);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var node = request.RequestUri!.Host;
            if (Down.ContainsKey(node))
            {
                throw new HttpRequestException($"{node} is down");
            }
            if (request.RequestUri.AbsolutePath == "/health")
            {
                var status = _health.GetValueOrDefault(node, new NodeStatus("standby", 0));
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(status, options: Wire.Json) });
            }
            if (FailRequests.ContainsKey(node))
            {
                throw new HttpRequestException($"{node} reset the connection");
            }
            var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(new { node }) };
            response.Headers.Add(HostingExtensions.ServedByHeader, $"node-{node}");
            return Task.FromResult(response);
        }
    }
}
