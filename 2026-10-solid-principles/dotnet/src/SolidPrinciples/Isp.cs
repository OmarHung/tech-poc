// I：介面隔離原則（Interface Segregation Principle）。
//
// 不要逼實作者依賴它用不到的方法。介面太肥，實作者只好用「丟例外」交差，
// 呼叫端從型別上看不出哪些方法其實不能用，等到執行才爆炸。
//
// 情境：貨到付款能收款，但不能線上退款，也沒有付款資訊可以記住給下次用。

namespace SolidPrinciples.Isp;

// --- 違反：一個肥介面，每種付款方式都得實作三個方法 ---

public interface ILegacyPaymentMethod
{
    string Charge(int amount);

    string Refund(int amount);

    /// <summary>記住付款資訊，下次一鍵付款。</summary>
    string Remember();
}

public sealed class LegacyCreditCard : ILegacyPaymentMethod
{
    public string Charge(int amount) => $"信用卡扣款 {amount} 元";

    public string Refund(int amount) => $"信用卡退款 {amount} 元";

    public string Remember() => "已記住這張信用卡";
}

public sealed class LegacyCashOnDelivery : ILegacyPaymentMethod
{
    public string Charge(int amount) => $"送達時收現 {amount} 元";

    public string Refund(int amount) => throw new NotSupportedException("貨到付款不能線上退款");

    public string Remember() => throw new NotSupportedException("貨到付款沒有付款資訊可以記住");
}

// --- 遵守：一種能力一個介面，付款方式做得到哪些就實作哪些 ---

public interface IChargeable
{
    string Charge(int amount);
}

public interface IRefundable
{
    string Refund(int amount);
}

public interface IRememberable
{
    string Remember();
}

public sealed class CreditCard : IChargeable, IRefundable, IRememberable
{
    public string Charge(int amount) => $"信用卡扣款 {amount} 元";

    public string Refund(int amount) => $"信用卡退款 {amount} 元";

    public string Remember() => "已記住這張信用卡";
}

public sealed class CashOnDelivery : IChargeable
{
    public string Charge(int amount) => $"送達時收現 {amount} 元";
}

public static class Refunds
{
    /// <summary>退款流程只收 IRefundable。傳 CashOnDelivery 進來，編譯就不會過。</summary>
    public static string RefundOrder(IRefundable method, int amount) => method.Refund(amount);
}
