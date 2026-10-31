using System.Collections.Immutable;

namespace CapTheorem;

/// <summary>
/// AP 不丟資料的合併方式之二：CRDT 計數器。每個節點只累加自己那一格，
/// 合併時每格取最大值，總數是各格相加。合併的順序和次數都不影響結果，所以可以自動合併。
/// </summary>
public static class GCounter
{
    public static ImmutableSortedDictionary<string, int> Empty => PerNode.Empty;

    /// <summary>在 node 那一格累加。G-Counter 只能增加；要能減少，就用兩個組成 PN-Counter。</summary>
    public static ImmutableSortedDictionary<string, int> Add(ImmutableSortedDictionary<string, int> counter, string node, int amount)
    {
        if (amount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(amount), amount, $"G-Counter 只能加正數，收到 {amount}");
        }
        return counter.SetItem(node, PerNode.Get(counter, node) + amount);
    }

    /// <summary>每一格取最大值。</summary>
    public static ImmutableSortedDictionary<string, int> Merge(params ImmutableSortedDictionary<string, int>[] counters) =>
        PerNode.Max(counters);

    public static int Total(IReadOnlyDictionary<string, int> counter) => counter.Values.Sum();
}
