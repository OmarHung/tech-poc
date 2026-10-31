using System.Collections.Concurrent;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Node.Cluster;
using Witness;

namespace Node.Tests;

/// <summary>
/// Witness and both nodes on test servers, with the real election and replication workers on a short lease.
/// Their traffic goes through <see cref="FakeNetwork"/>, which can take a process down or cut a link.
/// </summary>
public sealed class InProcessCluster : IAsyncDisposable
{
    public static readonly TimeSpan LeaseTtl = TimeSpan.FromMilliseconds(600);
    public static readonly string[] NodeIds = ["a", "b"];

    private readonly TempDir _dir = new();
    private readonly Dictionary<string, WebApplication> _apps = [];

    private InProcessCluster()
    {
    }

    public FakeNetwork Network { get; } = new();

    public static async Task<InProcessCluster> StartAsync()
    {
        var cluster = new InProcessCluster();
        await cluster.StartWitnessAsync();
        foreach (var id in NodeIds)
        {
            await cluster.StartNodeAsync(id);
        }
        return cluster;
    }

    public HttpClient Client(string node) => _apps[node].GetTestClient();

    public async Task<NodeHealth> HealthAsync(string node) =>
        (await Client(node).GetFromJsonAsync<NodeHealth>("/health"))!;

    public async Task<Dictionary<string, string>> DataAsync(string node) =>
        (await Client(node).GetFromJsonAsync<AdminData>("/admin/data"))!.Data;

    /// <summary>Waits for a primary whose standby is in sync and returns (primary, standby).</summary>
    public async Task<(string Primary, string Standby)> WaitForStablePrimaryAsync()
    {
        string? primary = null;
        await WaitUntilAsync(async () =>
        {
            foreach (var id in NodeIds.Where(Network.IsUp))
            {
                var health = await HealthAsync(id);
                if (health is { Role: "primary", Peer: "in-sync" })
                {
                    primary = id;
                    return true;
                }
            }
            return false;
        }, "a primary with an in-sync standby");
        return (primary!, Other(primary!));
    }

    public async Task WaitForRoleAsync(string node, string role) =>
        await WaitUntilAsync(async () => (await HealthAsync(node)).Role == role, $"{node} to be {role}");

    /// <summary>Stops the process and makes it unreachable, like kill -9 plus a dead NIC.</summary>
    public async Task KillAsync(string name)
    {
        Network.Down[name] = true;
        await _apps[name].StopAsync();
    }

    public static string Other(string node) => node == "a" ? "b" : "a";

    public static async Task WaitUntilAsync(Func<Task<bool>> condition, string what)
    {
        var deadline = DateTime.UtcNow + LeaseTtl * 10;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"timed out waiting for {what}");
            }
            await Task.Delay(20);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var (name, app) in _apps)
        {
            Network.Down[name] = true;
            await app.DisposeAsync();
        }
        _dir.Dispose();
    }

    private async Task StartWitnessAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddWitness(new WitnessConfig { DataDir = Path.Combine(_dir.Path, "witness"), LeaseTtl = LeaseTtl }, TimeProvider.System);
        var app = builder.Build();
        app.MapWitness();
        await StartAsync("witness", app);
    }

    private async Task StartNodeAsync(string id)
    {
        var config = new NodeConfig
        {
            NodeId = id,
            PeerUrl = new Uri($"http://{Other(id)}/"),
            WitnessUrl = new Uri("http://witness/"),
            DataDir = Path.Combine(_dir.Path, id),
            LeaseTtl = LeaseTtl,
            RenewInterval = LeaseTtl / 6,
            ReplicationTimeout = LeaseTtl / 3,
        };
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddNode(config, TimeProvider.System);
        builder.Services.AddSingleton(new HttpClient(Network.From(id)) { Timeout = Timeout.InfiniteTimeSpan });
        var app = builder.Build();
        app.MapNode();
        await StartAsync(id, app);
    }

    private async Task StartAsync(string name, WebApplication app)
    {
        await app.StartAsync();
        _apps[name] = app;
        Network.Hosts[name] = app.GetTestServer().CreateHandler();
    }

    private sealed record AdminData(Dictionary<string, string> Data);

    /// <summary>Routes requests by host name ("a", "b", "witness") to the test servers.</summary>
    public sealed class FakeNetwork
    {
        public ConcurrentDictionary<string, HttpMessageHandler> Hosts { get; } = new();
        public ConcurrentDictionary<string, bool> Down { get; } = new();
        public ConcurrentDictionary<(string From, string To), bool> Cut { get; } = new();

        public bool IsUp(string name) => !Down.ContainsKey(name);

        /// <summary>Cuts every link between <paramref name="node"/> and the rest of the cluster, both ways.</summary>
        public void Isolate(string node)
        {
            foreach (var peer in Hosts.Keys.Where(h => h != node))
            {
                Cut[(node, peer)] = true;
                Cut[(peer, node)] = true;
            }
        }

        public void Heal() => Cut.Clear();

        public HttpMessageHandler From(string from) => new Router(this, from);

        private sealed class Router(FakeNetwork network, string from) : HttpMessageHandler
        {
            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            {
                var to = request.RequestUri!.Host;
                if (!network.IsUp(from) || !network.IsUp(to) || network.Cut.ContainsKey((from, to)))
                {
                    throw new HttpRequestException($"{from} cannot reach {to}");
                }
                return new HttpMessageInvoker(network.Hosts[to], disposeHandler: false).SendAsync(request, ct);
            }
        }
    }
}
