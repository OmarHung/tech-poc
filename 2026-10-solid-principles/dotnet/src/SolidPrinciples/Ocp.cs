// O：開放封閉原則（Open-Closed Principle）。
//
// 對擴充開放、對修改封閉：加新功能時是寫新的程式碼，不是回去改已經能用的程式碼。
//
// 情境：原本有「VIP 九折」「員工八折」，行銷部新增「滿 1000 折 100」。

namespace SolidPrinciples.Ocp;

// --- 違反：每多一種折扣，就要回來改這串 switch 分支 ---

public static class LegacyPricing
{
    public static int TotalAfterDiscount(int subtotal, string kind) => kind switch
    {
        "none" => subtotal,
        "vip" => subtotal - subtotal / 10,
        "staff" => subtotal - subtotal / 5,
        // 不帶 paramName：帶了的話 Message 後面會多出「 (Parameter 'kind')」
        _ => throw new ArgumentException($"不支援的折扣：{kind}"),
    };
}

// --- 遵守：折扣是一個介面，新折扣就是新類別，TotalAfterDiscount() 不用改 ---

/// <summary>契約：0 ≤ Discount(subtotal) ≤ subtotal。Lsp.cs 會檢查這件事。</summary>
public interface IDiscountRule
{
    int Discount(int subtotal);
}

public sealed record NoDiscount : IDiscountRule
{
    public int Discount(int subtotal) => 0;
}

public sealed record PercentOff(int Percent) : IDiscountRule
{
    public int Discount(int subtotal) => subtotal * Percent / 100;
}

public static class Pricing
{
    public static int TotalAfterDiscount(int subtotal, IDiscountRule rule) =>
        subtotal - rule.Discount(subtotal);
}
