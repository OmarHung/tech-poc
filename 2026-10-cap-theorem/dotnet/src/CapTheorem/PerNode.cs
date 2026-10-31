using System.Collections.Immutable;

namespace CapTheorem;

/// <summary>version vector 和 G-Counter 共用的「每個節點一格」運算。</summary>
internal static class PerNode
{
    public static ImmutableSortedDictionary<string, int> Empty { get; } =
        ImmutableSortedDictionary.Create<string, int>(StringComparer.Ordinal);

    /// <summary>沒出現的節點當成 0。</summary>
    public static int Get(IReadOnlyDictionary<string, int> table, string node) =>
        table.TryGetValue(node, out var count) ? count : 0;

    /// <summary>每個節點取最大值。</summary>
    public static ImmutableSortedDictionary<string, int> Max(IEnumerable<IReadOnlyDictionary<string, int>> tables)
    {
        var all = tables.ToArray();
        return all
            .SelectMany(table => table.Keys)
            .Distinct()
            .ToImmutableSortedDictionary(node => node, node => all.Max(table => Get(table, node)), StringComparer.Ordinal);
    }
}
