using CheckoutQueue.Orders;
using CheckoutQueue.Queue;

namespace CheckoutQueue.Tests;

/// <summary>
/// Fires many requests at once. The multiplexer pipelines them, so they interleave on Redis
/// exactly like requests coming from several app instances.
/// </summary>
[Collection(RedisCollection.Name)]
public sealed class ConcurrencyTests(RedisFixture redis)
{
    private const int ClicksPerUser = 3;

    [Fact]
    public async Task ParallelCheckouts_NeverOversell_NorOrderTwicePerTicket()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { InitialStock = 100, MaxActiveSessions = 100 });
        var tickets = await sale.JoinAndAdmitAsync(Users(100));
        Assert.All(tickets, t => Assert.Equal(TicketStatus.Admitted, t.Status));

        // 100 admitted users each want 2 of the 100 left and click three times: 300 requests at once.
        var attempts = tickets.SelectMany(t => Enumerable.Repeat(t.TicketId, ClicksPerUser));
        var results = await Task.WhenAll(attempts.Select(id => sale.Orders.CheckoutAsync(id, 2)));

        var orders = results.Where(r => r.Order is not null).Select(r => r.Order!).ToList();
        Assert.Equal(50, orders.Select(o => o.OrderId).Distinct().Count());
        Assert.Equal(50, results.Count(r => r.Outcome == CheckoutOutcome.Created));
        Assert.All(orders.GroupBy(o => o.TicketId), g => Assert.Single(g.Select(o => o.OrderId).Distinct()));
        var audit = await sale.Audit.GetAsync();
        Assert.Equal((0, 0, 100), (audit.Stock, audit.Oversold, audit.PendingQuantity));
        Assert.True(audit.StockBalanced);
        Assert.Equal(0, audit.UsersWithMultipleLiveOrders);
    }

    [Fact]
    public async Task ParallelPayments_ChargeEachOrderOnce()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { InitialStock = 20, MaxActiveSessions = 10 });
        var tickets = await sale.JoinAndAdmitAsync(Users(10));
        var orders = await Task.WhenAll(tickets.Select(t => sale.Orders.CheckoutAsync(t.TicketId, 1)));

        var payments = orders.SelectMany(o => Enumerable.Repeat(o.Order!.OrderId, 10));
        var results = await Task.WhenAll(payments.Select(id => sale.Orders.PayAsync(id)));

        Assert.Equal(10, results.Count(r => r.Outcome == PayOutcome.Paid));
        Assert.Equal(90, results.Count(r => r.Outcome == PayOutcome.AlreadyPaid));
        var audit = await sale.Audit.GetAsync();
        Assert.Equal((10, 0, 10), (audit.Charges, audit.DuplicateCharges, audit.PaidOrders));
    }

    [Fact]
    public async Task ParallelJoins_FromTheSameUser_YieldOneTicket()
    {
        await using var sale = await TestSale.StartAsync(redis);

        var results = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => sale.Queue.JoinAsync("refresher")));

        Assert.Single(results, r => r.Outcome == JoinOutcome.Created);
        Assert.Single(results.Select(r => r.Ticket!.TicketId).Distinct());
        Assert.Equal(1, (await sale.Audit.GetAsync()).Tickets);
    }

    [Fact]
    public async Task ParallelAdmitters_NeverExceedMaxActive()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { InitialStock = 100, MaxActiveSessions = 5 });
        foreach (var user in Users(50))
        {
            await sale.Queue.JoinAsync(user);
        }

        // Every app instance runs its own admitter; ten of them tick at the same moment.
        var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => sale.Queue.AdmitAsync()));

        Assert.Equal(5, results.Sum(r => r.Admitted));
        var audit = await sale.Audit.GetAsync();
        Assert.Equal((5, 45), (audit.Active, audit.Waiting));
    }

    [Fact]
    public async Task PaymentsRacingTheExpirySweep_ReturnEachOrdersStockOnce()
    {
        await using var sale = await TestSale.StartAsync(redis, c => c with { MaxActiveSessions = 10 });
        var tickets = await sale.JoinAndAdmitAsync(Users(10));
        var orders = await Task.WhenAll(tickets.Select(t => sale.Orders.CheckoutAsync(t.TicketId, 1)));
        sale.Time.Advance(TimeSpan.FromSeconds(120));

        var late = orders.SelectMany(o => Enumerable.Repeat(o.Order!.OrderId, ClicksPerUser)).Select(id => sale.Orders.PayAsync(id));
        var sweeps = Enumerable.Range(0, ClicksPerUser).Select(_ => sale.Maintenance.RunOnceAsync());
        await Task.WhenAll(late.Cast<Task>().Concat(sweeps));

        var audit = await sale.Audit.GetAsync();
        Assert.Equal((10, 10, 0), (audit.Stock, audit.ExpiredOrders, audit.Charges));
        Assert.True(audit.StockBalanced);
    }

    [Fact]
    public async Task AdmitterWithAClockSlightlyBehind_StillCompletesItsPass()
    {
        await using var ahead = await TestSale.StartAsync(redis);
        await using var behind = await TestSale.StartAsync(redis, _ => ahead.Config);
        await ahead.Queue.JoinAsync("first");
        await ahead.Queue.JoinAsync("second");

        // "ahead" crosses into the next second first and marks it as when the line formed.
        ahead.Time.Advance(TimeSpan.FromSeconds(1));
        behind.Time.Advance(TimeSpan.FromMilliseconds(995));
        await ahead.Queue.AdmitAsync();
        var result = await behind.Queue.AdmitAsync();

        Assert.Equal(new AdmitResult(Admitted: 0, Abandoned: 0, Expired: 0), result);
    }

    private static string[] Users(int count) => Enumerable.Range(0, count).Select(i => $"user-{i}").ToArray();
}
