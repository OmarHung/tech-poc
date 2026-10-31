using System.Collections.Immutable;

namespace CapTheorem;

/// <summary>
/// AP 不丟資料的合併方式之一：每個版本記下「各節點寫過幾次」。
/// 恢復時比較時鐘，已經被別的版本看過的舊版本才丟掉；
/// 互相沒看過的並行寫入全部保留（兄弟版本），交給應用層決定怎麼合併。
/// </summary>
public static class VersionVector
{
    public static Order Compare(IReadOnlyDictionary<string, int> a, IReadOnlyDictionary<string, int> b)
    {
        var nodes = a.Keys.Union(b.Keys).ToArray();
        var aAhead = nodes.Any(node => PerNode.Get(a, node) > PerNode.Get(b, node));
        var bAhead = nodes.Any(node => PerNode.Get(b, node) > PerNode.Get(a, node));
        return (aAhead, bAhead) switch
        {
            (true, true) => Order.Concurrent,
            (true, false) => Order.After,
            (false, true) => Order.Before,
            (false, false) => Order.Equal,
        };
    }

    /// <summary>在 node 寫入 value。新時鐘涵蓋目前所有兄弟版本，所以會取代它們。</summary>
    public static Sibling NextVersion(IEnumerable<Sibling> siblings, string node, string value)
    {
        var seen = PerNode.Max(siblings.Select(sibling => sibling.Clock));
        return new Sibling(value, seen.SetItem(node, PerNode.Get(seen, node) + 1));
    }

    /// <summary>合併各節點手上的版本：已經被別的版本看過的丟掉，並行的全部保留。</summary>
    public static ImmutableArray<Sibling> Reconcile(IEnumerable<Sibling> versions)
    {
        var distinct = versions.Distinct().ToImmutableArray();
        return [.. distinct.Where(version => !distinct.Any(other => Compare(version.Clock, other.Clock) == Order.Before))];
    }
}
