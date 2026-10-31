// S：單一職責原則（Single Responsibility Principle）。
//
// 一個模組只該為一種角色（actor）改變。這裡有兩種角色：
// 財務在意「發票金額」，物流在意「免不免運」。
//
// 情境：財務要求發票金額改成含 5% 營業稅。物流的規則沒變：未稅小計滿 1000 元免運。

using System.Collections.Immutable;

namespace SolidPrinciples.Srp;

public static class CheckoutRules
{
    public const int TaxRatePercent = 5;
    public const int FreeShippingThreshold = 1000;
}

public sealed record LineItem(string Name, int UnitPrice, int Quantity);

// --- 違反：一個類別同時服務財務和物流，兩邊共用 Amount() ---

public sealed record LegacyOrder(ImmutableArray<LineItem> Items)
{
    public int InvoiceAmount() => Amount(); // 財務

    public bool ShipsFree() => Amount() >= CheckoutRules.FreeShippingThreshold; // 物流

    private int Amount()
    {
        // 為了財務的需求在這裡加上稅，用到這個方法的免運判斷也跟著變了
        var untaxed = Items.Sum(item => item.UnitPrice * item.Quantity);
        return untaxed + untaxed * CheckoutRules.TaxRatePercent / 100;
    }
}

// --- 遵守：財務和物流各有自己的類別，改一邊不會動到另一邊 ---

public static class LineItemExtensions
{
    /// <summary>未稅小計。兩種角色都認同的事實，不帶任何一方的規則，所以可以共用。</summary>
    public static int Subtotal(this IEnumerable<LineItem> items) =>
        items.Sum(item => item.UnitPrice * item.Quantity);
}

public sealed class InvoiceCalculator // 財務
{
    public int Amount(IEnumerable<LineItem> items)
    {
        var untaxed = items.Subtotal();
        return untaxed + untaxed * CheckoutRules.TaxRatePercent / 100;
    }
}

public sealed class ShippingPolicy // 物流
{
    public bool ShipsFree(IEnumerable<LineItem> items) =>
        items.Subtotal() >= CheckoutRules.FreeShippingThreshold;
}
