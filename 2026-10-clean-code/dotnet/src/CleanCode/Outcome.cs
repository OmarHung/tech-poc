namespace CleanCode;

/// <summary>一組輸入算出來的結果：運費，或輸入無效。</summary>
/// <param name="Fee">運費；輸入無效時是 null</param>
public readonly record struct Outcome(int? Fee)
{
    /// <summary>舊版回 -1、重構版丟 InvalidParcelException，都對應到這個值</summary>
    public static readonly Outcome Invalid = new(null);

    public bool IsInvalid => Fee is null;
}
