using System.Collections.Immutable;
using System.Globalization;

namespace CapTheorem.Tests;

public sealed class MergeTests
{
    // 跟 demo 一樣的情境：A 寫 x=1 後分區，A 寫 x=2、C 寫 x=3
    private static readonly Sibling X1 = new("1", Clock("A:1"));
    private static readonly Sibling X2OnA = VersionVector.NextVersion([X1], "A", "2");
    private static readonly Sibling X3OnC = VersionVector.NextVersion([X1], "C", "3");

    /// <summary>"A:1,C:1" → {A:1, C:1}；空字串是空時鐘。</summary>
    private static ImmutableSortedDictionary<string, int> Clock(string text) =>
        text.Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(pair => pair.Split(':'))
            .ToImmutableSortedDictionary(
                pair => pair[0],
                pair => int.Parse(pair[1], CultureInfo.InvariantCulture),
                StringComparer.Ordinal);

    // --- version vector ---

    [Theory]
    [InlineData("A:1", "A:2", Order.Before)]
    [InlineData("A:2", "A:1", Order.After)]
    [InlineData("A:1,C:1", "A:1,C:1", Order.Equal)]
    [InlineData("A:2", "A:1,C:1", Order.Concurrent)]
    [InlineData("", "B:1", Order.Before)] // 沒出現的節點當成 0
    public void Compare_OrdersClocksByHappenedBefore(string a, string b, Order expected)
    {
        Assert.Equal(expected, VersionVector.Compare(Clock(a), Clock(b)));
    }

    [Theory]
    [InlineData(Order.Before, "之前")]
    [InlineData(Order.After, "之後")]
    [InlineData(Order.Equal, "相同")]
    [InlineData(Order.Concurrent, "並行")]
    public void Order_ToText_IsTheWordPrintedByTheDemo(Order order, string expected)
    {
        Assert.Equal(expected, order.ToText());
    }

    [Fact]
    public void NextVersion_SupersedesEveryCurrentSibling()
    {
        var resolved = VersionVector.NextVersion([X2OnA, X3OnC], "A", "5");

        Assert.Equal(Clock("A:3,C:1"), resolved.Clock);
        Assert.Equal(Order.Before, VersionVector.Compare(X2OnA.Clock, resolved.Clock));
        Assert.Equal(Order.Before, VersionVector.Compare(X3OnC.Clock, resolved.Clock));
    }

    [Fact]
    public void Reconcile_DropsVersionsThatANewerOneHasSeen()
    {
        // 等同 CP 恢復：C 只是落後，不是衝突
        Assert.Equal(new[] { X2OnA }, VersionVector.Reconcile([X2OnA, X1]));
    }

    [Fact]
    public void Reconcile_KeepsBothConcurrentWrites()
    {
        var siblings = VersionVector.Reconcile([X2OnA, X2OnA, X3OnC]); // A、B、C 三台的版本

        Assert.Equal(new[] { "2", "3" }, siblings.Select(sibling => sibling.Value));
    }

    [Fact]
    public void ResolvedVersion_ReplacesTheSiblingsAfterAnotherReconcile()
    {
        var resolved = VersionVector.NextVersion([X2OnA, X3OnC], "A", "5");

        Assert.Equal(new[] { resolved }, VersionVector.Reconcile([X2OnA, X3OnC, resolved]));
    }

    [Fact]
    public void NextVersion_DoesNotChangeTheSiblingsItWasGiven()
    {
        VersionVector.NextVersion([X1], "A", "2");

        Assert.Equal(Clock("A:1"), X1.Clock);
    }

    // --- CRDT：G-Counter ---

    [Fact]
    public void Counter_IncrementsOnBothSides_MergeWithoutLoss()
    {
        var onA = GCounter.Add(GCounter.Empty, "A", 2);
        var onC = GCounter.Add(GCounter.Empty, "C", 3);

        Assert.Equal(5, GCounter.Total(GCounter.Merge(onA, onC)));
    }

    [Fact]
    public void Counter_Merge_IsCommutativeAndIdempotent()
    {
        var onA = GCounter.Add(GCounter.Add(GCounter.Empty, "A", 2), "B", 1);
        var onC = GCounter.Add(GCounter.Empty, "C", 3);

        Assert.Equal(GCounter.Merge(onA, onC), GCounter.Merge(onC, onA));
        Assert.Equal(onA, GCounter.Merge(onA, onA));
    }

    [Fact]
    public void Counter_OnlyAcceptsPositiveAmounts()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => GCounter.Add(GCounter.Empty, "A", 0));
    }

    [Fact]
    public void LastWriteWins_OnACounterTotal_LosesAnIncrement()
    {
        var (cluster, _) = Cluster.Create(Mode.AP).Write("A", "x", "0");
        cluster = cluster.Partition(["A", "B"], ["C"]);
        (cluster, _) = cluster.Write("A", "x", "2"); // A 那側 +2
        (cluster, _) = cluster.Write("C", "x", "3"); // C 那側 +3

        (cluster, _) = cluster.Heal();

        Assert.Equal("3", cluster.Read("A", "x").Value);
    }
}
