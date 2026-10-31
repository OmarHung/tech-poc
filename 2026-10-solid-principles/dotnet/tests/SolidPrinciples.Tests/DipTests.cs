using System.Net.Mail;
using SolidPrinciples.Dip;

namespace SolidPrinciples.Tests;

public sealed class DipTests
{
    [Fact]
    public void LegacyCheckout_CannotRunWithoutAnSmtpServer()
    {
        var error = Assert.Throws<SmtpException>(() => new LegacyCheckoutService().Checkout("alice@example.com", 1080));

        Assert.Equal("連不到 smtp.example.com:25", error.Message);
    }

    [Fact]
    public void Checkout_WithAFakeNotifier_NeedsNoSmtpServer()
    {
        var notifier = new RecordingNotifier();

        var receipt = new CheckoutService(notifier).Checkout("alice@example.com", 1080);

        Assert.Equal("已收款 1080 元", receipt);
        Assert.Equal([("alice@example.com", "已收款 1080 元")], notifier.Sent);
    }

    [Fact]
    public void Checkout_UsesWhateverNotifierItIsGiven()
    {
        var error = Assert.Throws<SmtpException>(() => new CheckoutService(new SmtpMailer()).Checkout("alice@example.com", 1080));

        Assert.Equal("連不到 smtp.example.com:25", error.Message);
    }
}
