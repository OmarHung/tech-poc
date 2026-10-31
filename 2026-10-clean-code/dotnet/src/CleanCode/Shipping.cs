using System.Collections.Frozen;

namespace CleanCode;

/// <summary>
/// 運費計算，Legacy.cs 的 Calc() 照 Clean Code 原則重構後的版本。
/// <list type="bullet">
/// <item>重量 1000 公克以內 60 元，5000 公克以內 100 元，超過的部分每 1000 公克（不足也算）加 20 元</item>
/// <item>寄離島加 80 元</item>
/// <item>本島訂單滿門檻免運：一般會員 1000 元、金卡會員 500 元。離島不提供免運</item>
/// <item>冷藏另外收費：本島 50 元、離島 100 元，免運也照收</item>
/// </list>
/// </summary>
public static class Shipping
{
    private const int LightParcelMaxGrams = 1_000;
    private const int LightParcelFee = 60;
    private const int StandardParcelMaxGrams = 5_000;
    private const int StandardParcelFee = 100;
    private const int GramsPerKilogram = 1_000;
    private const int FeePerExtraKilogram = 20;
    private const int OutlyingIslandSurcharge = 80;

    private static readonly FrozenDictionary<Membership, int> FreeShippingThreshold =
        new Dictionary<Membership, int>
        {
            [Membership.Normal] = 1_000,
            [Membership.Gold] = 500,
        }.ToFrozenDictionary();

    // Python 版叫 COLD_CHAIN_FEE；C# 的欄位不能和下面的 ColdChainFee() 同名
    private static readonly FrozenDictionary<Region, int> ColdChainFeeByRegion =
        new Dictionary<Region, int>
        {
            [Region.MainIsland] = 50,
            [Region.OutlyingIsland] = 100,
        }.ToFrozenDictionary();

    public static int ShippingFee(Parcel parcel)
    {
        RejectInvalid(parcel);
        return DeliveryFee(parcel) + ColdChainFee(parcel);
    }

    private static void RejectInvalid(Parcel parcel)
    {
        if (parcel.WeightGrams <= 0)
        {
            throw new InvalidParcelException($"重量必須大於 0 公克，收到 {parcel.WeightGrams}");
        }
        if (parcel.OrderAmount < 0)
        {
            throw new InvalidParcelException($"訂單金額不能是負數，收到 {parcel.OrderAmount}");
        }
    }

    private static int DeliveryFee(Parcel parcel)
    {
        if (ShipsFree(parcel))
        {
            return 0;
        }
        return WeightFee(parcel.WeightGrams) + RegionSurcharge(parcel.Region);
    }

    private static bool ShipsFree(Parcel parcel)
    {
        // 離島的物流成本高，行銷部決定不開放免運
        if (parcel.Region == Region.OutlyingIsland)
        {
            return false;
        }
        return parcel.OrderAmount >= FreeShippingThreshold[parcel.Membership];
    }

    private static int WeightFee(int weightGrams)
    {
        if (weightGrams <= LightParcelMaxGrams)
        {
            return LightParcelFee;
        }
        if (weightGrams <= StandardParcelMaxGrams)
        {
            return StandardParcelFee;
        }
        var extraKilograms = CeilDiv(weightGrams - StandardParcelMaxGrams, GramsPerKilogram);
        return StandardParcelFee + extraKilograms * FeePerExtraKilogram;
    }

    private static int RegionSurcharge(Region region) =>
        region == Region.OutlyingIsland ? OutlyingIslandSurcharge : 0;

    private static int ColdChainFee(Parcel parcel)
    {
        // 冷藏車是另外跟物流商計費的，所以免運也要收
        return parcel.NeedsColdChain ? ColdChainFeeByRegion[parcel.Region] : 0;
    }

    // C# 的整數除法往 0 截斷，不能照抄 Python 的 -(-a // b)（那要靠往下取整）。
    // 這裡只會收到正數，所以用 (a + b - 1) / b
    private static int CeilDiv(int dividend, int divisor) => (dividend + divisor - 1) / divisor;
}
