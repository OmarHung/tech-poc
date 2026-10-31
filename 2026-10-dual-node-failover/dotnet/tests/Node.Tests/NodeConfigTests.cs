namespace Node.Tests;

public sealed class NodeConfigTests
{
    [Fact]
    public void Defaults_ApplyWhenNothingIsSet()
    {
        var config = NodeConfig.FromLookup(_ => null);

        Assert.Equal(8101, config.Port);
        Assert.Equal("a", config.NodeId);
        Assert.Equal("data/a", config.DataDir);
        Assert.Equal(ArbiterMode.Witness, config.Arbiter);
        Assert.Equal(ReplicationMode.Sync, config.Replication);
        Assert.Equal(TimeSpan.FromMilliseconds(300), config.LeaseSafetyMargin);
    }

    [Fact]
    public void Settings_AreReadFromTheEnvironment()
    {
        var env = new Dictionary<string, string>
        {
            ["NODE_ID"] = "b",
            ["PORT"] = "9102",
            ["PEER_URL"] = "http://127.0.0.1:9101",
            ["WITNESS_URL"] = "http://127.0.0.1:9103",
            ["ARBITER"] = "none",
            ["REPLICATION"] = "async",
            ["LEASE_TTL_MS"] = "2000",
            ["RENEW_INTERVAL_MS"] = "200",
            ["REPLICATION_TIMEOUT_MS"] = "400",
            ["ELECTION_DELAY_MS"] = "1500",
        };

        var config = NodeConfig.FromLookup(env.GetValueOrDefault);

        Assert.Equal("b", config.NodeId);
        Assert.Equal("data/b", config.DataDir);
        Assert.Equal(9102, config.Port);
        Assert.Equal(new Uri("http://127.0.0.1:9101"), config.PeerUrl);
        Assert.Equal(new Uri("http://127.0.0.1:9103"), config.WitnessUrl);
        Assert.Equal(ArbiterMode.None, config.Arbiter);
        Assert.Equal(ReplicationMode.Async, config.Replication);
        Assert.Equal(TimeSpan.FromSeconds(2), config.LeaseTtl);
        Assert.Equal(TimeSpan.FromMilliseconds(200), config.RenewInterval);
        Assert.Equal(TimeSpan.FromMilliseconds(400), config.ReplicationTimeout);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), config.ElectionDelay);
    }

    [Theory]
    [InlineData("NODE_ID", "Node A", "NODE_ID")]
    [InlineData("ARBITER", "zookeeper", "ARBITER")]
    [InlineData("REPLICATION", "semi", "REPLICATION")]
    [InlineData("PEER_URL", "not a url", "PEER_URL")]
    [InlineData("PEER_URL", "ftp://host", "PEER_URL")]
    [InlineData("RENEW_INTERVAL_MS", "1500", "LEASE_TTL_MS")]
    [InlineData("REPLICATION_TIMEOUT_MS", "2000", "REPLICATION_TIMEOUT_MS")]
    [InlineData("PORT", "70000", "PORT")]
    public void BadSettings_NameTheVariableToFix(string name, string value, string expectedInMessage)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => NodeConfig.FromLookup(n => n == name ? value : null));

        Assert.Contains(expectedInMessage, ex.Message);
    }
}
