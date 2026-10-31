// 先拿幾筆訂單讓舊版 Calc() 和重構版 ShippingFee() 各算一次，
// 再窮舉 728 組輸入，確認重構沒有改變行為。

using System.Collections.Frozen;
using CleanCode;

var regionLabels = new Dictionary<Region, string>
{
    [Region.MainIsland] = "本島",
    [Region.OutlyingIsland] = "離島",
}.ToFrozenDictionary();

var membershipLabels = new Dictionary<Membership, string>
{
    [Membership.Normal] = "一般會員",
    [Membership.Gold] = "金卡會員",
}.ToFrozenDictionary();

Parcel[] samples =
[
    new(2500, Region.MainIsland, Membership.Normal, 800, false),
    new(800, Region.MainIsland, Membership.Normal, 1000, false),
    new(2500, Region.MainIsland, Membership.Gold, 600, true),
    new(7200, Region.OutlyingIsland, Membership.Gold, 3000, true),
    new(0, Region.MainIsland, Membership.Normal, 800, false),
];

ShowSamples();
ShowCharacterization();

string Describe(Parcel parcel)
{
    var temperature = parcel.NeedsColdChain ? "冷藏" : "常溫";
    return $"{regionLabels[parcel.Region]}、{membershipLabels[parcel.Membership]}、"
        + $"訂單 {parcel.OrderAmount} 元、{parcel.WeightGrams} 公克、{temperature}";
}

static string RefactoredResult(Parcel parcel)
{
    try
    {
        return Shipping.ShippingFee(parcel).ToString();
    }
    catch (InvalidParcelException error)
    {
        return $"錯誤：{error.Message}";
    }
}

void ShowSamples()
{
    Console.WriteLine("\n=== 同一筆訂單，舊版和重構版各算一次運費 ===");
    foreach (var parcel in samples)
    {
        var old = Legacy.Calc(
            parcel.WeightGrams,
            LegacyCodes.ToCode(parcel.Region),
            LegacyCodes.ToCode(parcel.Membership),
            parcel.OrderAmount,
            parcel.NeedsColdChain);
        Console.WriteLine($"  {Describe(parcel)} → 舊版 {old}｜重構版 {RefactoredResult(parcel)}");
    }
}

static void ShowCharacterization()
{
    var result = Characterization.Compare();
    Console.WriteLine($"\n=== Characterization test：窮舉 {result.Total} 組輸入 ===");
    var agreedFees = result.Total - result.Invalid - result.Mismatches.Length;
    Console.WriteLine($"  有效輸入 {agreedFees} 組：兩版運費相同");
    Console.WriteLine($"  無效輸入 {result.Invalid} 組：舊版回 -1，重構版回報明確的錯誤");
    Console.WriteLine($"  結果不同：{result.Mismatches.Length} 組");
}
