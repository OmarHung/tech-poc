namespace Witness.Tests;

public sealed class WitnessConfigTests
{
    [Fact]
    public void Defaults_ApplyWhenNothingIsSet()
    {
        var config = WitnessConfig.FromLookup(_ => null);

        Assert.Equal(8103, config.Port);
        Assert.Equal(["a", "b"], config.Nodes);
        Assert.Equal(TimeSpan.FromSeconds(3), config.LeaseTtl);
    }

    [Fact]
    public void Settings_AreReadFromTheEnvironment()
    {
        var env = new Dictionary<string, string> { ["PORT"] = "9000", ["NODES"] = "x, y", ["LEASE_TTL_MS"] = "1500", ["DATA_DIR"] = "/tmp/w" };

        var config = WitnessConfig.FromLookup(env.GetValueOrDefault);

        Assert.Equal(9000, config.Port);
        Assert.Equal(["x", "y"], config.Nodes);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), config.LeaseTtl);
        Assert.Equal("/tmp/w", config.DataDir);
    }

    [Theory]
    [InlineData("a")]
    [InlineData("a,a")]
    [InlineData("a,b,c")]
    [InlineData("a,B")]
    public void Nodes_MustBeTwoDistinctValidIds(string nodes)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => WitnessConfig.FromLookup(name => name == "NODES" ? nodes : null));

        Assert.Contains("NODES", ex.Message);
    }

    [Theory]
    [InlineData("PORT", "0")]
    [InlineData("LEASE_TTL_MS", "abc")]
    public void BadNumbers_NameTheVariable(string name, string value)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => WitnessConfig.FromLookup(n => n == name ? value : null));

        Assert.Contains(name, ex.Message);
    }
}
