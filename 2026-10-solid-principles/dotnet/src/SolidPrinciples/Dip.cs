// D：依賴反轉原則（Dependency Inversion Principle）。
//
// 高層的業務流程不該依賴低層的實作細節，兩者都該依賴抽象。
//
// 情境：結帳後要通知客人。這個 PoC 沒有 SMTP 伺服器，真的寄信一定失敗。

using System.Net.Mail;

namespace SolidPrinciples.Dip;

/// <summary>
/// 低層細節：用 SMTP 寄信。模擬的環境沒有 SMTP 伺服器，所以一定失敗。
/// <para>C# 的介面要明確宣告，所以這裡寫出 <c>: INotifier</c>：低層細節反過來依賴高層定義的抽象。</para>
/// </summary>
public sealed class SmtpMailer : INotifier
{
    public const string Host = "smtp.example.com";

    // .NET 的 SmtpClient 連不上伺服器時也是丟 SmtpException
    public void Send(string to, string message) => throw new SmtpException($"連不到 {Host}:25");
}

// --- 違反：結帳流程自己建立 SmtpMailer，外面換不掉 ---

public sealed class LegacyCheckoutService
{
    private readonly SmtpMailer _mailer = new();

    public string Checkout(string email, int amount)
    {
        var receipt = $"已收款 {amount} 元";
        _mailer.Send(email, receipt);
        return receipt;
    }
}

// --- 遵守：結帳流程只依賴 INotifier 介面，接哪一種由外面決定 ---

public interface INotifier
{
    void Send(string to, string message);
}

/// <summary>把通知記在記憶體裡，給測試和示範用。</summary>
public sealed class RecordingNotifier : INotifier
{
    private readonly List<(string To, string Message)> _sent = [];

    public IReadOnlyList<(string To, string Message)> Sent => _sent;

    public void Send(string to, string message) => _sent.Add((to, message));
}

public sealed class CheckoutService(INotifier notifier)
{
    public string Checkout(string email, int amount)
    {
        var receipt = $"已收款 {amount} 元";
        notifier.Send(email, receipt);
        return receipt;
    }
}
