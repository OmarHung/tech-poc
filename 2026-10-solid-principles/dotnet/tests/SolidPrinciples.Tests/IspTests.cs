using SolidPrinciples.Isp;

namespace SolidPrinciples.Tests;

public sealed class IspTests
{
    [Fact]
    public void LegacyCreditCard_SupportsEveryMethod()
    {
        var card = new LegacyCreditCard();

        Assert.Equal("信用卡扣款 300 元", card.Charge(300));
        Assert.Equal("信用卡退款 300 元", card.Refund(300));
        Assert.Equal("已記住這張信用卡", card.Remember());
    }

    [Fact]
    public void LegacyCashOnDelivery_Charges()
    {
        Assert.Equal("送達時收現 300 元", new LegacyCashOnDelivery().Charge(300));
    }

    [Fact]
    public void LegacyCashOnDelivery_IsForcedToStubTwoMethods()
    {
        var cash = new LegacyCashOnDelivery();

        var refund = Assert.Throws<NotSupportedException>(() => cash.Refund(300));
        var remember = Assert.Throws<NotSupportedException>(() => cash.Remember());

        Assert.Equal("貨到付款不能線上退款", refund.Message);
        Assert.Equal("貨到付款沒有付款資訊可以記住", remember.Message);
    }

    [Fact]
    public void CashOnDelivery_OnlyImplementsCharging()
    {
        // 用反射檢查，因為這件事在編譯時就決定了：
        // Refunds.RefundOrder(new CashOnDelivery(), 300) 編譯不會過（CS1503：無法從 CashOnDelivery 轉換成 IRefundable）
        Assert.True(typeof(IChargeable).IsAssignableFrom(typeof(CashOnDelivery)));
        Assert.False(typeof(IRefundable).IsAssignableFrom(typeof(CashOnDelivery)));
        Assert.False(typeof(IRememberable).IsAssignableFrom(typeof(CashOnDelivery)));
        Assert.Equal("送達時收現 300 元", new CashOnDelivery().Charge(300));
    }

    [Fact]
    public void CreditCard_ImplementsEveryRole()
    {
        var card = new CreditCard();

        Assert.True(typeof(IChargeable).IsAssignableFrom(typeof(CreditCard)));
        Assert.True(typeof(IRefundable).IsAssignableFrom(typeof(CreditCard)));
        Assert.True(typeof(IRememberable).IsAssignableFrom(typeof(CreditCard)));
        Assert.Equal("信用卡扣款 300 元", card.Charge(300));
        Assert.Equal("已記住這張信用卡", card.Remember());
    }

    [Fact]
    public void RefundOrder_RefundsARefundableMethod()
    {
        Assert.Equal("信用卡退款 300 元", Refunds.RefundOrder(new CreditCard(), 300));
    }
}
