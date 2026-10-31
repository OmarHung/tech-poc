using System.Net;
using System.Net.Http.Json;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Node.Cluster;
using Node.Kv;
using Node.Storage;
using Shared;

namespace Node.Tests;

/// <summary>The real HTTP pipeline of one node on a test server, with election and replication driven by the test.</summary>
public sealed class NodeApiTests : IAsyncLifetime
{
    private readonly TempDir _dir = new();
    private readonly FakeTimeProvider _time = new();
    private WebApplication _app = null!;
    private HttpClient _client = null!;

    private NodeState State => _app.Services.GetRequiredService<NodeState>();

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddNode(new NodeConfig { DataDir = _dir.Path, Arbiter = ArbiterMode.None }, _time, runWorkers: false);
        _app = builder.Build();
        _app.MapNode();
        await _app.StartAsync();
        _client = _app.GetTestClient();
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
        _dir.Dispose();
    }

    [Fact]
    public async Task Standby_Answers503_AndSaysWhoItIs()
    {
        var put = await _client.PutAsJsonAsync("/kv/k", new { value = "v" });
        var get = await _client.GetAsync("/kv/k");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, put.StatusCode);
        Assert.Equal("not primary", (await put.Content.ReadFromJsonAsync<ErrorBody>())?.Error);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, get.StatusCode);
        Assert.Equal("node-a", put.Headers.GetValues(HostingExtensions.ServedByHeader).Single());
    }

    [Fact]
    public async Task Primary_StoresAndReturnsValues()
    {
        await PromoteAsync();

        var put = await _client.PutAsJsonAsync("/kv/order-1", new { value = "paid" });
        var item = await put.Content.ReadFromJsonAsync<KvItem>();
        var get = await _client.GetFromJsonAsync<KvItem>("/kv/order-1");

        Assert.Equal(HttpStatusCode.OK, put.StatusCode);
        Assert.Equal(new KvItem("order-1", "paid", 1, 1), item);
        Assert.Equal(item, get);
    }

    [Fact]
    public async Task MissingKey_Is404()
    {
        await PromoteAsync();

        var response = await _client.GetAsync("/kv/nothing");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Theory]
    [InlineData("/kv/bad%20key", "{\"value\":\"v\"}")]
    [InlineData("/kv/k", "{\"value\":1}")]
    [InlineData("/kv/k", "{}")]
    [InlineData("/kv/k", "not json")]
    public async Task BadWrites_Are400(string path, string body)
    {
        await PromoteAsync();

        var response = await _client.PutAsync(path, new StringContent(body, Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.NotNull((await response.Content.ReadFromJsonAsync<ErrorBody>())?.Error);
    }

    [Fact]
    public async Task TooLongValue_Is400()
    {
        await PromoteAsync();

        var response = await _client.PutAsJsonAsync("/kv/k", new { value = new string('x', KvService.MaxValueLength + 1) });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Replicate_AppliesEntries_AndAdminDataShowsThem()
    {
        var request = new ReplicateRequest("b", 2, 0, 0, [new LogEntry(1, 2, "k", "v")], 1);

        var response = await _client.PostAsJsonAsync("/internal/replicate", request, Wire.Json);
        var reply = await response.Content.ReadFromJsonAsync<ReplicateReply>(Wire.Json);
        var data = await _client.GetFromJsonAsync<AdminData>("/admin/data");

        Assert.Equal(new ReplicateReply(ReplicateStatus.Ok, 2, 1, 3000, null), reply);
        Assert.Equal(new Dictionary<string, string> { ["k"] = "v" }, data?.Data);
        Assert.Equal(2, data?.Epoch);
        Assert.Contains("\"status\":\"ok\"", await (await _client.PostAsJsonAsync("/internal/replicate", request, Wire.Json)).Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Health_ReportsRoleAndModes()
    {
        var health = await _client.GetFromJsonAsync<NodeHealth>("/health");

        Assert.Equal("standby", health?.Role);
        Assert.Equal("none", health?.Arbiter);
        Assert.Equal("sync", health?.Replication);
    }

    private async Task PromoteAsync()
    {
        State.TryPromote(State.Epoch + 1, TimeSpan.MaxValue, standbyInSync: false);
        Assert.True(await State.CurrentTerm!.TryGoSoloAsync()); // what the replicator does at the start of a term
    }

    private sealed record AdminData(string Node, string Role, long Epoch, long LastIndex, Dictionary<string, string> Data);
}
