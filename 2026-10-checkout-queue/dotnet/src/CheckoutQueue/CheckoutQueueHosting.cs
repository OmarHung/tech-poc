using System.Text.Json;
using System.Text.Json.Serialization;
using CheckoutQueue.Admin;
using CheckoutQueue.Demo;
using CheckoutQueue.Http;
using CheckoutQueue.Maintenance;
using CheckoutQueue.Orders;
using CheckoutQueue.Queue;
using CheckoutQueue.Redis;
using StackExchange.Redis;

namespace CheckoutQueue;

public static class CheckoutQueueHosting
{
    /// <param name="runMaintenance">Tests turn the background tick off and drive <see cref="SaleMaintenance"/> themselves.</param>
    public static IServiceCollection AddCheckoutQueue(
        this IServiceCollection services,
        CheckoutQueueConfig config,
        IConnectionMultiplexer redis,
        TimeProvider time,
        bool runMaintenance = true)
    {
        config.Validate();
        services.AddSingleton(config);
        services.AddSingleton(redis);
        services.AddSingleton(time);
        services.AddSingleton<SaleRedis>();
        services.AddSingleton<QueueStore>();
        services.AddSingleton<OrderStore>();
        services.AddSingleton<AuditStore>();
        services.AddSingleton<TicketBoardStore>();
        services.AddSingleton<SaleMaintenance>();
        if (runMaintenance)
        {
            services.AddHostedService<SaleMaintenanceWorker>();
        }

        services.ConfigureHttpJsonOptions(options =>
        {
            // ASP.NET's web defaults accept "2" for 2; the contract says that is a 400.
            options.SerializerOptions.NumberHandling = JsonNumberHandling.Strict;
            options.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
            options.SerializerOptions.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower));
        });
        return services;
    }

    public static WebApplication MapCheckoutQueue(this WebApplication app)
    {
        var config = app.Services.GetRequiredService<CheckoutQueueConfig>();
        app.UseServedBy(config.InstanceName);
        app.UseApiErrors();

        app.MapGet("/health", async (IConnectionMultiplexer redis) =>
        {
            await redis.GetDatabase().PingAsync();
            return Results.Ok(new { status = "ok", instance = config.InstanceName, saleId = config.SaleId });
        });
        app.MapQueueEndpoints();
        app.MapOrderEndpoints();
        // Unauthenticated on purpose for the PoC: the process only listens on 127.0.0.1.
        app.MapGet("/admin/audit", (AuditStore audit, CancellationToken ct) => audit.GetAsync(ct));
        app.MapGet("/admin/config", () => SaleSettings.From(config));
        app.MapGet("/admin/tickets", (TicketBoardStore board, CancellationToken ct) => board.ListAsync(ct));
        app.MapDemoPage();
        return app;
    }
}
