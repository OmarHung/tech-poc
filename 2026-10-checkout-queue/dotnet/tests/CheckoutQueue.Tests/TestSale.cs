using CheckoutQueue.Admin;
using CheckoutQueue.Maintenance;
using CheckoutQueue.Orders;
using CheckoutQueue.Queue;
using CheckoutQueue.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace CheckoutQueue.Tests;

/// <summary>A fresh sale on the shared Redis, wired the same way as the app and driven by a fake clock.</summary>
public sealed class TestSale : IAsyncDisposable
{
    public static readonly DateTimeOffset Start = new(2026, 10, 30, 0, 0, 0, TimeSpan.Zero);

    private readonly ServiceProvider _services;

    private TestSale(ServiceProvider services, FakeTimeProvider time, CheckoutQueueConfig config)
    {
        _services = services;
        Time = time;
        Config = config;
    }

    public FakeTimeProvider Time { get; }
    public CheckoutQueueConfig Config { get; }
    public QueueStore Queue => _services.GetRequiredService<QueueStore>();
    public OrderStore Orders => _services.GetRequiredService<OrderStore>();
    public AuditStore Audit => _services.GetRequiredService<AuditStore>();
    public TicketBoardStore Board => _services.GetRequiredService<TicketBoardStore>();
    public SaleMaintenance Maintenance => _services.GetRequiredService<SaleMaintenance>();

    /// <summary>Small numbers so every rule is easy to hit: 10 in stock, 3 at a time, up to 2 each.</summary>
    public static CheckoutQueueConfig NewConfig() => new()
    {
        SaleId = $"t{Guid.NewGuid():N}",
        InitialStock = 10,
        MaxQuantityPerOrder = 2,
        MaxActiveSessions = 3,
        AdmissionWindow = TimeSpan.FromSeconds(60),
        PaymentWindow = TimeSpan.FromSeconds(120),
        HeartbeatTimeout = TimeSpan.FromSeconds(15),
        PollInterval = TimeSpan.FromSeconds(1),
    };

    public static async Task<TestSale> StartAsync(RedisFixture redis, Func<CheckoutQueueConfig, CheckoutQueueConfig>? tweak = null)
    {
        var config = (tweak ?? (c => c))(NewConfig());
        var time = new FakeTimeProvider(Start);
        var services = new ServiceCollection()
            .AddLogging()
            .AddCheckoutQueue(config, redis.Connection, time, runMaintenance: false)
            .BuildServiceProvider();
        await services.GetRequiredService<SaleRedis>().InitializeAsync(config.InitialStock);
        return new TestSale(services, time, config);
    }

    /// <summary>Joins every user and runs one admission pass.</summary>
    public async Task<IReadOnlyList<TicketView>> JoinAndAdmitAsync(params string[] userIds)
    {
        foreach (var userId in userIds)
        {
            await Queue.JoinAsync(userId);
        }
        await Queue.AdmitAsync();
        var tickets = new List<TicketView>();
        foreach (var userId in userIds)
        {
            tickets.Add((await Queue.JoinAsync(userId)).Ticket!);
        }
        return tickets;
    }

    /// <summary>Puts one user through the line and returns their pending order.</summary>
    public async Task<OrderView> PlaceOrderAsync(string userId, int quantity = 1)
    {
        var ticket = (await JoinAndAdmitAsync(userId))[0];
        var result = await Orders.CheckoutAsync(ticket.TicketId, quantity);
        Assert.Equal(CheckoutOutcome.Created, result.Outcome);
        return result.Order!;
    }

    public long NowMs => Time.GetUtcNow().ToUnixTimeMilliseconds();

    public ValueTask DisposeAsync() => _services.DisposeAsync();
}
