namespace Gateway.Tests;

public sealed class GatewayConfigTests
{
    [Fact]
    public void Defaults_PointAtTheTwoLocalNodes()
    {
        var config = GatewayConfig.FromLookup(_ => null);

        Assert.Equal(8100, config.Port);
        Assert.Equal(["a", "b"], config.Nodes.Select(n => n.Id));
        Assert.Equal(TimeSpan.FromMilliseconds(100), config.PollInterval);
    }

    [Fact]
    public void NodeUrls_AreParsed()
    {
        var nodes = GatewayConfig.ParseNodes("x=http://127.0.0.1:9001, y=http://127.0.0.1:9002");

        Assert.Equal([new NodeTarget("x", new Uri("http://127.0.0.1:9001")), new NodeTarget("y", new Uri("http://127.0.0.1:9002"))], nodes);
    }

    [Theory]
    [InlineData("a")]
    [InlineData("a=notaurl")]
    [InlineData("A=http://127.0.0.1:1")]
    [InlineData("a=http://127.0.0.1:1,a=http://127.0.0.1:2")]
    [InlineData(",")]
    public void BadNodeUrls_AreRejected(string raw)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => GatewayConfig.ParseNodes(raw));

        Assert.Contains("NODE_URLS", ex.Message);
    }
}
