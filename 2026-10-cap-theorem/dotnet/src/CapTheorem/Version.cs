namespace CapTheorem;

/// <param name="Value">寫入的值</param>
/// <param name="Timestamp">全域邏輯時鐘；分區恢復時用它決定 last-write-wins 誰贏</param>
/// <param name="Origin">接受這筆寫入的節點</param>
public sealed record Version(string Value, int Timestamp, string Origin);
