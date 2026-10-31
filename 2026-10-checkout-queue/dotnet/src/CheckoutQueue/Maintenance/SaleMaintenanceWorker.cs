using StackExchange.Redis;

namespace CheckoutQueue.Maintenance;

/// <summary>Runs <see cref="SaleMaintenance"/> every TICK_MS on this instance.</summary>
public sealed class SaleMaintenanceWorker(
    SaleMaintenance maintenance,
    CheckoutQueueConfig config,
    TimeProvider time,
    ILogger<SaleMaintenanceWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(config.TickInterval, time);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await maintenance.RunOnceAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is RedisException or TimeoutException)
            {
                // Redis blips are expected; the next tick retries. Anything else is a bug and stops the host.
                logger.LogWarning(ex, "Maintenance tick failed for sale {SaleId}; retrying next tick", config.SaleId);
            }
        }
    }
}
