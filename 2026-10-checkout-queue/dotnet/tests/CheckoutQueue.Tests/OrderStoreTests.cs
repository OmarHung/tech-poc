using CheckoutQueue.Orders;
using CheckoutQueue.Queue;

namespace CheckoutQueue.Tests;

[Collection(RedisCollection.Name)]
public sealed class OrderStoreTests(RedisFixture redis)
{
    [Fact]
    public async Task Checkout_BeforeAdmission_IsRejected()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var ticket = (await sale.Queue.JoinAsync("eager")).Ticket!;

        var result = await sale.Orders.CheckoutAsync(ticket.TicketId, 1);

        Assert.Equal(CheckoutOutcome.NotAdmitted, result.Outcome);
        Assert.Equal("waiting", result.Detail);
    }

    [Fact]
    public async Task Checkout_ReservesStock_AndStartsThePaymentClock()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var ticket = (await sale.JoinAndAdmitAsync("alice"))[0];

        var result = await sale.Orders.CheckoutAsync(ticket.TicketId, 2);

        Assert.Equal(CheckoutOutcome.Created, result.Outcome);
        var order = result.Order!;
        Assert.Equal((ticket.TicketId, "alice", 2, OrderStatus.PendingPayment), (order.TicketId, order.UserId, order.Quantity, order.Status));
        Assert.Equal(sale.NowMs + 120_000, order.PaymentExpiresAtMs);
        Assert.Null(order.PaidAtMs);
        Assert.Equal(8, (await sale.Audit.GetAsync()).Stock);
        var after = (await sale.Queue.GetTicketAsync(ticket.TicketId))!;
        Assert.Equal((TicketStatus.Ordered, order.OrderId), (after.Status, after.OrderId));
    }

    [Fact]
    public async Task Checkout_Repeated_ReturnsTheSameOrder_AndReservesOnce()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var ticket = (await sale.JoinAndAdmitAsync("double-clicker"))[0];

        var first = await sale.Orders.CheckoutAsync(ticket.TicketId, 2);
        var second = await sale.Orders.CheckoutAsync(ticket.TicketId, 2);

        Assert.Equal(CheckoutOutcome.Existing, second.Outcome);
        Assert.Equal(first.Order!.OrderId, second.Order!.OrderId);
        Assert.Equal(8, (await sale.Audit.GetAsync()).Stock);
    }

    [Fact]
    public async Task Checkout_RepeatedWithADifferentQuantity_IsRejected()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var ticket = (await sale.JoinAndAdmitAsync("indecisive"))[0];
        var first = await sale.Orders.CheckoutAsync(ticket.TicketId, 1);

        var second = await sale.Orders.CheckoutAsync(ticket.TicketId, 2);

        Assert.Equal(CheckoutOutcome.TicketUsed, second.Outcome);
        Assert.Equal(first.Order!.OrderId, second.Detail);
        Assert.Equal(9, (await sale.Audit.GetAsync()).Stock);
    }

    [Fact]
    public async Task Checkout_MoreThanIsLeft_IsRejected_AndKeepsTheAdmission()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { InitialStock = 1 });
        var ticket = (await sale.JoinAndAdmitAsync("alice"))[0];

        var tooMany = await sale.Orders.CheckoutAsync(ticket.TicketId, 2);
        var retry = await sale.Orders.CheckoutAsync(ticket.TicketId, 1);

        Assert.Equal((CheckoutOutcome.InsufficientStock, "1"), (tooMany.Outcome, tooMany.Detail));
        Assert.Equal(CheckoutOutcome.Created, retry.Outcome);
    }

    [Fact]
    public async Task Checkout_WhileUnpaidOrdersHoldTheRest_IsNotSoldOutYet()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { InitialStock = 2 });
        var tickets = await sale.JoinAndAdmitAsync("first", "second");
        var held = (await sale.Orders.CheckoutAsync(tickets[0].TicketId, 2)).Order!;

        var whileHeld = await sale.Orders.CheckoutAsync(tickets[1].TicketId, 1);
        await sale.Orders.PayAsync(held.OrderId);
        var afterPaid = await sale.Orders.CheckoutAsync(tickets[1].TicketId, 1);

        // "sold_out" is final everywhere; while stock may still come back it is only "insufficient".
        Assert.Equal((CheckoutOutcome.InsufficientStock, "0"), (whileHeld.Outcome, whileHeld.Detail));
        Assert.Equal(CheckoutOutcome.SoldOut, afterPaid.Outcome);
        Assert.Equal(TicketStatus.SoldOut, (await sale.Queue.GetTicketAsync(tickets[1].TicketId))!.Status);
    }

    [Fact]
    public async Task Checkout_AfterTheAdmissionWindow_IsRejected()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var ticket = (await sale.JoinAndAdmitAsync("slow"))[0];
        sale.Time.Advance(TimeSpan.FromSeconds(60));

        var result = await sale.Orders.CheckoutAsync(ticket.TicketId, 1);

        Assert.Equal(CheckoutOutcome.AdmissionExpired, result.Outcome);
        Assert.Equal(0, (await sale.Audit.GetAsync()).Active);
        Assert.Equal(10, (await sale.Audit.GetAsync()).Stock);
    }

    [Fact]
    public async Task Checkout_UnknownTicket_IsNotFound()
    {
        await using var sale = await TestSale.StartAsync(redis);

        var result = await sale.Orders.CheckoutAsync(Ids.New(), 1);

        Assert.Equal(CheckoutOutcome.TicketNotFound, result.Outcome);
    }

    [Fact]
    public async Task Pay_ChargesOnce_EvenWhenRepeated()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var order = await sale.PlaceOrderAsync("payer", quantity: 2);

        var first = await sale.Orders.PayAsync(order.OrderId);
        var second = await sale.Orders.PayAsync(order.OrderId);

        Assert.Equal(PayOutcome.Paid, first.Outcome);
        Assert.Equal((OrderStatus.Paid, sale.NowMs), (first.Order!.Status, first.Order.PaidAtMs));
        Assert.Equal(PayOutcome.AlreadyPaid, second.Outcome);
        var audit = await sale.Audit.GetAsync();
        Assert.Equal((1, 0, 1, 2), (audit.Charges, audit.DuplicateCharges, audit.PaidOrders, audit.PaidQuantity));
    }

    [Fact]
    public async Task Pay_AfterTheDeadline_ExpiresTheOrder_AndReturnsTheStock()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var order = await sale.PlaceOrderAsync("late", quantity: 2);
        sale.Time.Advance(TimeSpan.FromSeconds(120));

        var shownBeforeSweep = await sale.Orders.GetOrderAsync(order.OrderId);
        var result = await sale.Orders.PayAsync(order.OrderId);
        var again = await sale.Orders.PayAsync(order.OrderId);

        Assert.Equal(OrderStatus.Expired, shownBeforeSweep!.Status);
        Assert.Equal(PayOutcome.OrderExpired, result.Outcome);
        Assert.Equal(PayOutcome.OrderExpired, again.Outcome);
        var audit = await sale.Audit.GetAsync();
        Assert.Equal((10, 1, 2, 0), (audit.Stock, audit.ExpiredOrders, audit.ExpiredQuantity, audit.Charges));
    }

    [Fact]
    public async Task Pay_UnknownOrder_IsNotFound()
    {
        await using var sale = await TestSale.StartAsync(redis);

        Assert.Equal(PayOutcome.OrderNotFound, (await sale.Orders.PayAsync(Ids.New())).Outcome);
        Assert.Null(await sale.Orders.GetOrderAsync(Ids.New()));
    }

    [Fact]
    public async Task Maintenance_ReleasesUnpaidOrders_AndTheStockGoesToTheNextInLine()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { InitialStock = 1, MaxActiveSessions = 1 });
        var tickets = await sale.JoinAndAdmitAsync("never-pays", "next");
        await sale.Orders.CheckoutAsync(tickets[0].TicketId, 1);
        Assert.Equal(TicketStatus.Waiting, (await sale.Queue.GetTicketAsync(tickets[1].TicketId))!.Status);

        sale.Time.Advance(TimeSpan.FromSeconds(120));
        await sale.Queue.GetTicketAsync(tickets[1].TicketId);
        var pass = await sale.Maintenance.RunOnceAsync();
        var checkout = await sale.Orders.CheckoutAsync(tickets[1].TicketId, 1);

        Assert.Equal(1, pass.ReleasedOrders);
        Assert.Equal(1, pass.Admission.Admitted);
        Assert.Equal(CheckoutOutcome.Created, checkout.Outcome);
    }

    [Fact]
    public async Task Rejoin_AfterTheOrderExpired_GetsANewTicket_ButAfterPaying_DoesNot()
    {
        await using var sale = await TestSale.StartAsync(redis);
        var expired = await sale.PlaceOrderAsync("forgetful");
        var paid = await sale.PlaceOrderAsync("buyer");
        await sale.Orders.PayAsync(paid.OrderId);
        sale.Time.Advance(TimeSpan.FromSeconds(120));

        var forgetful = await sale.Queue.JoinAsync("forgetful");
        var buyer = await sale.Queue.JoinAsync("buyer");

        Assert.Equal(JoinOutcome.Created, forgetful.Outcome);
        Assert.NotEqual(expired.TicketId, forgetful.Ticket!.TicketId);
        Assert.Equal(JoinOutcome.Existing, buyer.Outcome);
        Assert.Equal((TicketStatus.Ordered, paid.OrderId), (buyer.Ticket!.Status, buyer.Ticket.OrderId));
    }

    [Fact]
    public async Task SoldOut_TurnsAwayNewcomers_AndTellsTheLine()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { InitialStock = 1 });
        var order = await sale.PlaceOrderAsync("lucky");
        var waiting = (await sale.Queue.JoinAsync("hopeful")).Ticket!;

        var whilePending = await sale.Queue.GetTicketAsync(waiting.TicketId);
        await sale.Orders.PayAsync(order.OrderId);
        var afterPaid = await sale.Queue.GetTicketAsync(waiting.TicketId);
        var newcomer = await sale.Queue.JoinAsync("late");

        Assert.Equal(TicketStatus.Waiting, whilePending!.Status);
        Assert.Equal(TicketStatus.SoldOut, afterPaid!.Status);
        Assert.Equal(JoinOutcome.SoldOut, newcomer.Outcome);
        Assert.Null(newcomer.Ticket);
    }
}
