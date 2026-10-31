using System.Collections.Immutable;

namespace CapTheorem;

/// <summary>帶著 version vector 的一個版本。並行寫入時，同一個 key 會同時有好幾個兄弟版本。</summary>
/// <param name="Value">寫入的值</param>
/// <param name="Clock">節點 -> 這個節點寫過幾次</param>
public sealed record Sibling(string Value, ImmutableSortedDictionary<string, int> Clock)
{
    // record 預設用參考比較字典，這裡改成比較時鐘的內容，去重時才認得出同一個版本
    public bool Equals(Sibling? other) =>
        other is not null && Value == other.Value && Clock.SequenceEqual(other.Clock);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Value);
        foreach (var (node, count) in Clock)
        {
            hash.Add(node);
            hash.Add(count);
        }
        return hash.ToHashCode();
    }
}
