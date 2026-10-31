// SOLID 五個原則各用一個電商結帳的小情境，把「違反版」和「遵守版」的差別印出來。
//
// 輸出只用型別名稱（三種語言都一樣），不印方法名稱（各語言的命名慣例不同），
// 所以和 Python、Rust 版本的輸出可以逐字相同。

using System.Collections.Immutable;
using System.Globalization;
using System.Net.Mail;
using SolidPrinciples.Dip;
using SolidPrinciples.Isp;
using SolidPrinciples.Lsp;
using SolidPrinciples.Ocp;
using SolidPrinciples.Srp;

const string CustomerEmail = "alice@example.com";

// 數字不跟著系統地區設定格式化（有些地區的負號不是 '-'），輸出才會和其他語言逐字相同
CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

ShowSrp();
ShowOcp();
ShowLsp();
ShowIsp();
ShowDip();

// 只接住示範裡預期會發生的三種例外，其他例外照樣往外丟
static string Attempt<T>(Func<T> action)
{
    try
    {
        return $"{action()}";
    }
    catch (Exception error) when (error is ArgumentException or NotSupportedException or SmtpException)
    {
        return $"錯誤：{error.Message}";
    }
}

static string YesNo(bool flag) => flag ? "是" : "否";

static void ShowSrp()
{
    Console.WriteLine("\n=== S：單一職責原則（SRP）===");
    Console.WriteLine("  情境：訂單未稅 960 元。財務要求發票改成含 5% 稅；物流規則沒變：未稅滿 1000 元免運");
    ImmutableArray<LineItem> items = [new("滑鼠", 480, 2)];
    var legacy = new LegacyOrder(items);
    Console.WriteLine(
        $"  [違反] LegacyOrder：發票 {legacy.InvoiceAmount()} 元、"
        + $"免運：{YesNo(legacy.ShipsFree())}  ← 財務的改動連帶改到物流");
    Console.WriteLine(
        $"  [遵守] InvoiceCalculator：發票 {new InvoiceCalculator().Amount(items)} 元；"
        + $"ShippingPolicy：免運：{YesNo(new ShippingPolicy().ShipsFree(items))}");
}

static void ShowOcp()
{
    Console.WriteLine("\n=== O：開放封閉原則（OCP）===");
    Console.WriteLine("  情境：小計 1200 元。原本有 VIP 九折、員工八折，行銷部新增「滿 1000 折 100」");
    var vip = LegacyPricing.TotalAfterDiscount(1200, "vip");
    var threshold = Attempt(() => LegacyPricing.TotalAfterDiscount(1200, "threshold"));
    Console.WriteLine($"  [違反] 套 VIP → {vip} 元");
    Console.WriteLine($"  [違反] 套滿千折百 → {threshold}  ← 只能回去改這串判斷");
    var percent = Pricing.TotalAfterDiscount(1200, new PercentOff(10));
    var spend = Pricing.TotalAfterDiscount(1200, new SpendThresholdOff(1000, 100));
    Console.WriteLine($"  [遵守] 套 PercentOff(10) → {percent} 元");
    Console.WriteLine(
        $"  [遵守] 套 SpendThresholdOff(1000, 100) → {spend} 元"
        + "  ← 新類別寫在示範程式裡，ocp 模組沒改");
}

static void ShowLsp()
{
    Console.WriteLine("\n=== L：里氏替換原則（LSP）===");
    Console.WriteLine("  情境：小計 150 元的訂單，套用「折 200 元」");
    (string Label, IDiscountRule Rule)[] candidates =
    [
        ("[違反] UncappedAmountOff(200)", new UncappedAmountOff(200)),
        ("[遵守] FixedAmountOff(200)", new FixedAmountOff(200)),
    ];
    foreach (var (label, rule) in candidates)
    {
        var violations = DiscountContract.Violations(rule);
        var violationText = violations.IsEmpty ? "無" : string.Join("、", violations);
        var total = Pricing.TotalAfterDiscount(150, rule);
        Console.WriteLine($"  {label} → 總額 {total} 元；違反契約的小計：{violationText}");
    }

    (string Name, IDiscountRule Rule)[] rules =
    [
        ("NoDiscount", new NoDiscount()),
        ("PercentOff(10)", new PercentOff(10)),
        ("PercentOff(20)", new PercentOff(20)),
        ("SpendThresholdOff(1000, 100)", new SpendThresholdOff(1000, 100)),
        ("FixedAmountOff(200)", new FixedAmountOff(200)),
    ];
    var passed = rules.All(entry => DiscountContract.Violations(entry.Rule).IsEmpty);
    var names = string.Join("、", rules.Select(entry => entry.Name));
    Console.WriteLine($"  契約檢查 {names}：{(passed ? "全部通過" : "有規則違反契約")}");
}

static void ShowIsp()
{
    Console.WriteLine("\n=== I：介面隔離原則（ISP）===");
    Console.WriteLine("  情境：貨到付款能收款，但不能線上退款，也沒有付款資訊可以記住");
    var legacy = new LegacyCashOnDelivery();
    Console.WriteLine("  [違反] LegacyCashOnDelivery 被迫實作肥介面的 3 個方法");
    Console.WriteLine($"    收款 300 元 → {Attempt(() => legacy.Charge(300))}");
    Console.WriteLine($"    退款 300 元 → {Attempt(() => legacy.Refund(300))}");
    Console.WriteLine($"    記住付款資訊 → {Attempt(legacy.Remember)}");

    // 宣告成 IChargeable：如果宣告成 CashOnDelivery（sealed），編譯器早就知道
    // 「cash is IRefundable」永遠是 false，會發出 CS0184 警告（警告視為錯誤，建置失敗）
    IChargeable cash = new CashOnDelivery();
    Console.WriteLine("  [遵守] CashOnDelivery 只實作收款介面");
    Console.WriteLine($"    收款 300 元 → {Attempt(() => cash.Charge(300))}");
    var refundable = cash is IRefundable;
    Console.WriteLine($"    能退款嗎 → {YesNo(refundable)}：沒有實作退款介面，退款流程在型別檢查時就收不了它");
}

static void ShowDip()
{
    Console.WriteLine("\n=== D：依賴反轉原則（DIP）===");
    Console.WriteLine($"  情境：結帳後寄通知給 {CustomerEmail}。這個環境沒有 SMTP 伺服器");
    var legacy = new LegacyCheckoutService();
    Console.WriteLine("  [違反] LegacyCheckoutService 自己建立 SmtpMailer");
    Console.WriteLine($"    結帳 1080 元 → {Attempt(() => legacy.Checkout(CustomerEmail, 1080))}");

    Console.WriteLine("  [遵守] CheckoutService 只依賴通知介面，由外面注入");
    var recorder = new RecordingNotifier();
    var receipt = Attempt(() => new CheckoutService(recorder).Checkout(CustomerEmail, 1080));
    Console.WriteLine(
        $"    注入 RecordingNotifier：結帳 1080 元 → {receipt}；"
        + $"記下 {recorder.Sent.Count} 則通知給 {recorder.Sent[0].To}");
    var smtp = Attempt(() => new CheckoutService(new SmtpMailer()).Checkout(CustomerEmail, 1080));
    Console.WriteLine($"    注入 SmtpMailer：結帳 1080 元 → {smtp}");
}

/// <summary>新需求：滿 Threshold 折 Amount。寫在示範程式裡，Ocp.cs 一行都沒改。</summary>
file sealed record SpendThresholdOff(int Threshold, int Amount) : IDiscountRule
{
    public int Discount(int subtotal) => subtotal >= Threshold ? Amount : 0;
}
