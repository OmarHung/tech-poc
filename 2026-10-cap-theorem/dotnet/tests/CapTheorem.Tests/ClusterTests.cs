namespace CapTheorem.Tests;

public sealed class ClusterTests
{
    /// <summary>共用情境：3 台都有 x=1 之後，網路斷成 {A, B} | {C}。</summary>
    private static Cluster Partitioned(Mode mode) =>
        Cluster.Create(mode).Write("A", "x", "1").Cluster.Partition(["A", "B"], ["C"]);

    /// <summary>分區期間，A 寫 x=2、C 寫 x=3（C 比較晚）。</summary>
    private static Cluster WrittenOnBothSides(Mode mode) =>
        Partitioned(mode).Write("A", "x", "2").Cluster.Write("C", "x", "3").Cluster;

    private static void AssertEveryNodeReads(Cluster cluster, string expected) =>
        Assert.All(cluster.Nodes, node => Assert.Equal(expected, cluster.Read(node, "x").Value));

    // --- CP ---

    [Fact]
    public void Cp_MinoritySide_RejectsWritesAndReadsDuringPartition()
    {
        var cluster = Partitioned(Mode.CP);

        var (_, writeResult) = cluster.Write("C", "x", "3");
        var readResult = cluster.Read("C", "x");

        Assert.False(writeResult.Ok);
        Assert.False(readResult.Ok);
    }

    [Fact]
    public void Cp_MajoritySide_KeepsServingDuringPartition()
    {
        var (cluster, writeResult) = Partitioned(Mode.CP).Write("A", "x", "2");

        Assert.True(writeResult.Ok);
        Assert.Equal("2", cluster.Read("B", "x").Value);
    }

    [Fact]
    public void Cp_Heal_LosesNoAcknowledgedWrite()
    {
        var (cluster, lost) = WrittenOnBothSides(Mode.CP).Heal();

        Assert.Empty(lost);
        AssertEveryNodeReads(cluster, "2");
    }

    [Fact]
    public void Cp_WithTwoNodes_CannotServeOnEitherSideOfAPartition()
    {
        var cluster = Cluster.Create(Mode.CP, "A", "B").Partition(["A"], ["B"]);

        var (_, onA) = cluster.Write("A", "x", "1");
        var (_, onB) = cluster.Write("B", "x", "1");

        Assert.False(onA.Ok);
        Assert.False(onB.Ok);
    }

    // --- AP ---

    [Fact]
    public void Ap_BothSides_AcceptWritesDuringPartition()
    {
        var (cluster, onA) = Partitioned(Mode.AP).Write("A", "x", "2");
        var (_, onC) = cluster.Write("C", "x", "3");

        Assert.True(onA.Ok);
        Assert.True(onC.Ok);
    }

    [Fact]
    public void Ap_Reads_DivergeDuringPartition()
    {
        var cluster = WrittenOnBothSides(Mode.AP);

        Assert.Equal("2", cluster.Read("A", "x").Value);
        Assert.Equal("3", cluster.Read("C", "x").Value);
    }

    [Fact]
    public void Ap_MinoritySide_ReadsStaleValueDuringPartition()
    {
        var (cluster, _) = Partitioned(Mode.AP).Write("A", "x", "2");

        Assert.Equal("1", cluster.Read("C", "x").Value);
    }

    [Fact]
    public void Ap_HealWithLastWriteWins_DropsAnAcknowledgedWrite()
    {
        var (cluster, lost) = WrittenOnBothSides(Mode.AP).Heal();

        var dropped = Assert.Single(lost);
        Assert.Equal(("2", "A"), (dropped.Value, dropped.Origin));
        AssertEveryNodeReads(cluster, "3");
    }

    // --- 共通行為 ---

    [Fact]
    public void Write_ReturnsNewCluster_WithoutChangingTheOriginal()
    {
        var original = Cluster.Create(Mode.AP);

        var (updated, _) = original.Write("A", "x", "1");

        Assert.Null(original.Read("A", "x").Value);
        Assert.Equal("1", updated.Read("A", "x").Value);
    }

    [Fact]
    public void Heal_WithoutPartition_LosesNothing()
    {
        var (cluster, _) = Cluster.Create(Mode.AP).Write("A", "x", "1");

        var (_, lost) = cluster.Heal();

        Assert.Empty(lost);
    }

    public static TheoryData<string[][]> InvalidPartitions =>
    [
        [["A"], ["C"]], // 漏掉 B
        [["A", "B"], ["B", "C"]], // B 重複
        [["A", "B"], ["C", "Z"]], // Z 不存在
    ];

    [Theory]
    [MemberData(nameof(InvalidPartitions))]
    public void Partition_RequiresEveryNodeExactlyOnce(string[][] groups)
    {
        var cluster = Cluster.Create(Mode.CP);

        Assert.Throws<ArgumentException>(() => cluster.Partition(groups));
    }

    [Fact]
    public void UnknownNode_IsRejected()
    {
        var cluster = Cluster.Create(Mode.CP);

        Assert.Throws<ArgumentException>(() => cluster.Read("Z", "x"));
    }
}
