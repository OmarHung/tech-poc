using System.Collections.Immutable;

namespace CapTheorem;

/// <summary>
/// CAP 定理的最小模擬：同一組節點切換 CP / AP 兩種模式，看網路分區時的行為差異。
/// <para>
/// 網路分區用「節點分組」表示：同一組內可以互相連線，跨組連不到。
/// 寫入會立刻複製到同一組的所有節點，另一組收不到。
/// 整個模擬跑在單一程序內，沒有真的網路，所以每次結果都一樣。
/// 叢集是不可變的：每個操作都回傳新的叢集，原本的不會變。
/// </para>
/// </summary>
/// <param name="Mode">CP 或 AP</param>
/// <param name="Nodes">所有節點</param>
/// <param name="Stores">節點 -> (key -> 版本)</param>
/// <param name="Groups">目前連得到彼此的節點分組</param>
/// <param name="Clock">全域邏輯時鐘</param>
/// <param name="PartitionedAt">分區發生時的時鐘；沒有分區時為 null</param>
public sealed record Cluster(
    Mode Mode,
    ImmutableArray<string> Nodes,
    ImmutableDictionary<string, ImmutableDictionary<string, Version>> Stores,
    ImmutableArray<ImmutableHashSet<string>> Groups,
    int Clock = 0,
    int? PartitionedAt = null)
{
    public int Quorum => Nodes.Length / 2 + 1;

    public static Cluster Create(Mode mode) => Create(mode, "A", "B", "C");

    public static Cluster Create(Mode mode, params string[] nodes) =>
        new(
            mode,
            [.. nodes],
            nodes.ToImmutableDictionary(node => node, _ => ImmutableDictionary<string, Version>.Empty),
            [nodes.ToImmutableHashSet()]);

    /// <summary>把網路切成幾組。每個節點必須剛好出現在一組裡。</summary>
    public Cluster Partition(params string[][] groups)
    {
        var members = groups.SelectMany(group => group).Order(StringComparer.Ordinal);
        if (!members.SequenceEqual(Nodes.Order(StringComparer.Ordinal)))
        {
            throw new ArgumentException($"分組必須剛好涵蓋每個節點一次：{string.Join(", ", Nodes)}", nameof(groups));
        }
        return this with { Groups = [.. groups.Select(group => group.ToImmutableHashSet())], PartitionedAt = Clock };
    }

    public (Cluster Cluster, Result Result) Write(string node, string key, string value)
    {
        var peers = Reachable(node);
        if (RejectWithoutQuorum(peers) is { } rejection)
        {
            return (this, rejection);
        }

        var clock = Clock + 1;
        var version = new Version(value, clock, node);
        var stores = Stores.ToImmutableDictionary(
            entry => entry.Key,
            entry => peers.Contains(entry.Key) ? entry.Value.SetItem(key, version) : entry.Value);
        return (this with { Stores = stores, Clock = clock }, new Result(Ok: true));
    }

    public Result Read(string node, string key)
    {
        if (RejectWithoutQuorum(Reachable(node)) is { } rejection)
        {
            return rejection;
        }

        // 同一組的節點資料永遠一致，所以讀本地就等於讀到這一組的最新值。
        // AP 少數派讀到的「最新」其實是分區前的舊資料。
        return new Result(Ok: true, Value: Stores[node].TryGetValue(key, out var version) ? version.Value : null);
    }

    /// <summary>
    /// 恢復連線，用 last-write-wins 合併各節點的資料。
    /// 回傳合併後的叢集，以及「分區期間被確認過、合併時卻被丟掉」的寫入。
    /// </summary>
    public (Cluster Cluster, ImmutableArray<Version> Lost) Heal()
    {
        // 每個 key 在各節點上的不同版本，依時間由舊到新排好；最後一個就是 LWW 的贏家
        var candidates = Stores.Values
            .SelectMany(store => store)
            .GroupBy(entry => entry.Key, entry => entry.Value)
            .OrderBy(group => group.Key, StringComparer.Ordinal)
            .Select(group => (Key: group.Key, Versions: group.Distinct().OrderBy(v => v.Timestamp).ToImmutableArray()))
            .ToImmutableArray();
        var merged = candidates.ToImmutableDictionary(c => c.Key, c => c.Versions[^1]);

        // 分區前就存在的舊版本輸掉只是「落後」，分區期間寫入的版本輸掉才是「遺失」。
        var since = PartitionedAt ?? Clock;
        var lost = candidates
            .SelectMany(c => c.Versions.SkipLast(1))
            .Where(v => v.Timestamp > since)
            .ToImmutableArray();

        var healed = this with
        {
            Stores = Nodes.ToImmutableDictionary(node => node, _ => merged),
            Groups = [Nodes.ToImmutableHashSet()],
            PartitionedAt = null,
        };
        return (healed, lost);
    }

    private ImmutableHashSet<string> Reachable(string node) =>
        Groups.FirstOrDefault(group => group.Contains(node))
        ?? throw new ArgumentException($"不存在的節點：{node}", nameof(node));

    private Result? RejectWithoutQuorum(ImmutableHashSet<string> peers) =>
        Mode == Mode.AP || peers.Count >= Quorum
            ? null
            : new Result(Ok: false, Reason: $"只連得到 {peers.Count} 台，需要 {Quorum} 台才過半");
}
