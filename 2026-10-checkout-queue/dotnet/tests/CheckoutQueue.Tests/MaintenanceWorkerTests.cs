using CheckoutQueue.Maintenance;
using CheckoutQueue.Queue;
using CheckoutQueue.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;

namespace CheckoutQueue.Tests;

[Collection(RedisCollection.Name)]
public sealed class MaintenanceWorkerTests(RedisFixture redis)
{
    [Fact]
    public async Task Worker_AdmitsOnEveryTick_WithoutBeingAsked()
    {
        var config = TestSale.NewConfig();
        var time = new FakeTimeProvider(TestSale.Start);
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddCheckoutQueue(config, redis.Connection, time)
            .BuildServiceProvider();
        await services.GetRequiredService<SaleRedis>().InitializeAsync(config.InitialStock);
        var worker = services.GetServices<IHostedService>().OfType<SaleMaintenanceWorker>().Single();
        var queue = services.GetRequiredService<QueueStore>();
        var ticket = (await queue.JoinAsync("alice")).Ticket!;

        await worker.StartAsync(CancellationToken.None);
        var status = TicketStatus.Waiting;
        for (var attempt = 0; attempt < 50 && status == TicketStatus.Waiting; attempt++)
        {
            time.Advance(config.TickInterval);
            await Task.Delay(20);
            status = (await queue.GetTicketAsync(ticket.TicketId))!.Status;
        }
        await worker.StopAsync(CancellationToken.None);

        Assert.Equal(TicketStatus.Admitted, status);
    }
}
