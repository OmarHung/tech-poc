using System.Net;
using System.Net.Http.Json;

namespace Node.Tests;

/// <summary>Whole-cluster behaviour in one process. verify/scenario.py covers the same ground with real processes.</summary>
public sealed class ClusterTests : IAsyncLifetime
{
    private InProcessCluster _cluster = null!;

    public async Task InitializeAsync() => _cluster = await InProcessCluster.StartAsync();

    public async Task DisposeAsync() => await _cluster.DisposeAsync();

    [Fact]
    public async Task OnePrimary_ReplicatesEveryAcknowledgedWrite()
    {
        var (primary, standby) = await _cluster.WaitForStablePrimaryAsync();

        var keys = await WriteAsync(primary, "k", 20);

        Assert.Equal("standby", (await _cluster.HealthAsync(standby)).Role);
        var onStandby = await _cluster.DataAsync(standby);
        Assert.All(keys, key => Assert.Equal(key, onStandby.GetValueOrDefault(key)));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, (await Put(standby, "nope")).StatusCode);
    }

    [Fact]
    public async Task StandbyTakesOver_WhenThePrimaryDies_WithEveryAcknowledgedWrite()
    {
        var (primary, standby) = await _cluster.WaitForStablePrimaryAsync();
        var oldEpoch = (await _cluster.HealthAsync(primary)).Epoch;
        var keys = await WriteAsync(primary, "before", 20);

        await _cluster.KillAsync(primary);
        await _cluster.WaitForRoleAsync(standby, "primary");

        var health = await _cluster.HealthAsync(standby);
        Assert.True(health.Epoch > oldEpoch);
        Assert.Equal("solo", health.Peer);
        var data = await _cluster.DataAsync(standby);
        Assert.All(keys, key => Assert.Equal(key, data.GetValueOrDefault(key)));
        Assert.Equal(HttpStatusCode.OK, (await Put(standby, "after")).StatusCode);
    }

    [Fact]
    public async Task IsolatedPrimary_StopsAcknowledging_AndRejoinsAsStandby()
    {
        var (primary, standby) = await _cluster.WaitForStablePrimaryAsync();
        var keys = await WriteAsync(primary, "before", 10);

        _cluster.Network.Isolate(primary);
        var cutOff = await Put(primary, "cut-off");
        await _cluster.WaitForRoleAsync(standby, "primary");
        var accepted = await WriteAsync(standby, "after", 10);
        _cluster.Network.Heal();
        await _cluster.WaitForRoleAsync(primary, "standby");
        await InProcessCluster.WaitUntilAsync(async () => (await _cluster.HealthAsync(standby)).Peer == "in-sync", "the old primary to catch up");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, cutOff.StatusCode);
        var newPrimaryData = await _cluster.DataAsync(standby);
        Assert.Equal(newPrimaryData, await _cluster.DataAsync(primary));
        Assert.All(keys.Concat(accepted), key => Assert.Equal(key, newPrimaryData.GetValueOrDefault(key)));
        Assert.False(newPrimaryData.ContainsKey("cut-off"));
    }

    [Fact]
    public async Task PrimaryKeepsServing_WhileTheWitnessIsDown()
    {
        var (primary, _) = await _cluster.WaitForStablePrimaryAsync();
        var epoch = (await _cluster.HealthAsync(primary)).Epoch;

        await _cluster.KillAsync("witness");
        await Task.Delay(InProcessCluster.LeaseTtl * 3);
        var keys = await WriteAsync(primary, "no-witness", 10);

        var health = await _cluster.HealthAsync(primary);
        Assert.Equal(("primary", epoch), (health.Role, health.Epoch));
        Assert.Equal(10, keys.Count);
    }

    [Fact]
    public async Task PrimaryGoesSolo_WhenTheStandbyDies()
    {
        var (primary, standby) = await _cluster.WaitForStablePrimaryAsync();

        await _cluster.KillAsync(standby);
        var keys = await WriteAsync(primary, "alone", 5);

        Assert.Equal("solo", (await _cluster.HealthAsync(primary)).Peer);
        Assert.Equal(5, keys.Count);
    }

    private Task<HttpResponseMessage> Put(string node, string key) =>
        _cluster.Client(node).PutAsJsonAsync($"/kv/{key}", new { value = key });

    /// <summary>Writes keys (value = key) and asserts each one was acknowledged.</summary>
    private async Task<List<string>> WriteAsync(string node, string prefix, int count)
    {
        var keys = new List<string>();
        for (var i = 0; i < count; i++)
        {
            var key = $"{prefix}-{i}";
            var response = await Put(node, key);
            Assert.True(response.IsSuccessStatusCode, $"{key} on {node}: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
            keys.Add(key);
        }
        return keys;
    }
}
