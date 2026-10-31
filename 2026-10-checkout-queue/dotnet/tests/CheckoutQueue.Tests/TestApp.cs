using CheckoutQueue.Maintenance;
using CheckoutQueue.Redis;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using StackExchange.Redis;

namespace CheckoutQueue.Tests;

/// <summary>The real HTTP pipeline on a test server, with the admission tick driven by the test.</summary>
public sealed class TestApp : IAsyncDisposable
{
    private readonly WebApplication _app;

    private TestApp(WebApplication app, FakeTimeProvider time)
    {
        _app = app;
        Time = time;
        Client = app.GetTestClient();
    }

    public HttpClient Client { get; }
    public FakeTimeProvider Time { get; }
    public SaleMaintenance Maintenance => _app.Services.GetRequiredService<SaleMaintenance>();

    public static async Task<TestApp> StartAsync(
        IConnectionMultiplexer redis,
        Func<CheckoutQueueConfig, CheckoutQueueConfig>? tweak = null,
        bool initializeSale = true)
    {
        var config = (tweak ?? (c => c))(TestSale.NewConfig()) with { InstanceName = "test-instance" };
        var time = new FakeTimeProvider(TestSale.Start);
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddCheckoutQueue(config, redis, time, runMaintenance: false);
        var app = builder.Build();
        if (initializeSale)
        {
            await app.Services.GetRequiredService<SaleRedis>().InitializeAsync(config.InitialStock);
        }
        app.MapCheckoutQueue();
        await app.StartAsync();
        return new TestApp(app, time);
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _app.DisposeAsync();
    }
}
