using Shared;
using Worker.HotHot;

namespace Worker.Tests;

public sealed class LedgerTests
{
    [Fact]
    public void Apply_KeepsRunningBalancesInOrder()
    {
        var (first, b1) = Ledger.Empty.Apply(new LoggedEvent(1, "x", 10));
        var (second, b2) = first.Apply(new LoggedEvent(2, "y", 3));
        var (third, b3) = second.Apply(new LoggedEvent(3, "x", -4));

        Assert.Equal((10L, 3L, 6L), (b1, b2, b3));
        Assert.Equal(3, third.Offset);
        Assert.Equal(new Dictionary<string, long> { ["x"] = 6, ["y"] = 3 }, third.ToFile().Balances);
    }

    [Fact]
    public void Apply_RefusesAGap()
    {
        Assert.Throws<InvalidOperationException>(() => Ledger.Empty.Apply(new LoggedEvent(2, "x", 1)));
    }

    [Fact]
    public void SkipTo_MovesThePositionButKeepsTheOldBalances()
    {
        var (ledger, _) = Ledger.Empty.Apply(new LoggedEvent(1, "x", 10));

        var skipped = ledger.SkipTo(50);

        Assert.Equal(50, skipped.Offset);
        Assert.Equal(10, skipped.Balances["x"]);
        Assert.Equal(60, skipped.Apply(new LoggedEvent(51, "x", 50)).Balance);
    }

    [Fact]
    public void Checkpoint_RoundTrips()
    {
        var (ledger, _) = Ledger.Empty.Apply(new LoggedEvent(1, "x", 10));

        var restored = Ledger.FromFile(ledger.ToFile());

        Assert.Equal(ledger.Offset, restored.Offset);
        Assert.Equal(ledger.Balances, restored.Balances);
    }
}
