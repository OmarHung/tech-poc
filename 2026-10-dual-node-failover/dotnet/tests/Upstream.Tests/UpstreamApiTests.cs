using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Time.Testing;
using Shared;

namespace Upstream.Tests;

public sealed class UpstreamApiTests : IAsyncLifetime
{
    private WebApplication _app = null!;
    private HttpClient _client = null!;

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddUpstream(new UpstreamConfig(), new FakeTimeProvider(), runReaper: false);
        _app = builder.Build();
        _app.MapUpstream();
        await _app.StartAsync();
        _client = _app.GetTestClient();
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
    }

    [Fact]
    public async Task Events_CanBeAppendedAndRead()
    {
        var created = await _client.PostAsJsonAsync("/events", new { account = "acct-1", amount = 5 });
        var page = await _client.GetFromJsonAsync<EventPage>("/events?after=0");

        Assert.Equal(HttpStatusCode.Created, created.StatusCode);
        Assert.Equal([new LoggedEvent(1, "acct-1", 5)], page?.Events);
    }

    [Fact]
    public async Task Tasks_GoThroughLeaseHeartbeatAndComplete()
    {
        await _client.PostAsJsonAsync("/tasks", new { input = 3 });

        var leased = await _client.PostAsJsonAsync("/tasks/lease", new { worker = "a" });
        var lease = await leased.Content.ReadFromJsonAsync<TaskLease>();
        var empty = await _client.PostAsJsonAsync("/tasks/lease", new { worker = "b" });
        var heartbeat = await _client.PostAsJsonAsync("/tasks/1/heartbeat", new { worker = "a", attempt = 1 });
        var complete = await _client.PostAsJsonAsync("/tasks/1/complete", new { worker = "a", attempt = 1 });
        var again = await _client.PostAsJsonAsync("/tasks/1/complete", new { worker = "a", attempt = 1 });
        var other = await _client.PostAsJsonAsync("/tasks/1/complete", new { worker = "b", attempt = 1 });
        var stats = await _client.GetFromJsonAsync<TaskStats>("/tasks/stats");

        Assert.Equal(new TaskLease(1, 3, 1, 2000), lease);
        Assert.Equal(HttpStatusCode.NoContent, empty.StatusCode);
        Assert.Equal(HttpStatusCode.OK, heartbeat.StatusCode);
        Assert.Equal(HttpStatusCode.OK, complete.StatusCode);
        Assert.Equal(HttpStatusCode.OK, again.StatusCode); // the same report twice is fine
        Assert.Equal(HttpStatusCode.Conflict, other.StatusCode);
        Assert.Equal(1, stats?.Done);
    }

    [Fact]
    public async Task BadRequests_Are400()
    {
        var badEvent = await _client.PostAsJsonAsync("/events", new { account = "", amount = 1 });
        var badWorker = await _client.PostAsJsonAsync("/tasks/lease", new { worker = "A B" });

        Assert.Equal(HttpStatusCode.BadRequest, badEvent.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, badWorker.StatusCode);
    }
}
