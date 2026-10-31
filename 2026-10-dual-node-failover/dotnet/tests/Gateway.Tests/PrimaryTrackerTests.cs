namespace Gateway.Tests;

public sealed class PrimaryTrackerTests
{
    private static readonly NodeTarget A = new("a", new Uri("http://127.0.0.1:8101"));
    private static readonly NodeTarget B = new("b", new Uri("http://127.0.0.1:8102"));

    [Fact]
    public void Choose_PicksTheNodeThatSaysPrimary()
    {
        var choice = PrimaryTracker.Choose([(A, new NodeStatus("standby", 3)), (B, new NodeStatus("primary", 3))]);

        Assert.Equal(new PrimaryChoice(B, 3), choice);
    }

    [Fact]
    public void Choose_PrefersTheNewerEpoch_WhenBothClaimIt()
    {
        var choice = PrimaryTracker.Choose([(A, new NodeStatus("primary", 4)), (B, new NodeStatus("primary", 5))]);

        Assert.Equal(new PrimaryChoice(B, 5), choice);
    }

    [Fact]
    public void Choose_ReturnsNothing_WithoutAPrimary()
    {
        Assert.Null(PrimaryTracker.Choose([(A, null), (B, new NodeStatus("standby", 2))]));
        Assert.Null(PrimaryTracker.Choose([]));
    }
}
