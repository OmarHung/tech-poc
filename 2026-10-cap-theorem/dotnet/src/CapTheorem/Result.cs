namespace CapTheorem;

/// <param name="Ok">是否成功</param>
/// <param name="Value">讀到的值；寫入時為 null</param>
/// <param name="Reason">被拒絕的原因</param>
public sealed record Result(bool Ok, string? Value = null, string Reason = "");
