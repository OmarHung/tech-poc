using CheckoutQueue;
using CheckoutQueue.Redis;
using StackExchange.Redis;

var config = CheckoutQueueConfig.FromEnvironment();
var redis = await ConnectionMultiplexer.ConnectAsync(config.RedisUrl);

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://127.0.0.1:{config.Port}");
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
builder.Services.AddCheckoutQueue(config, redis, TimeProvider.System);

var app = builder.Build();
var startedWith = await app.Services.GetRequiredService<SaleRedis>().InitializeAsync(config.InitialStock);
if (startedWith != config.InitialStock)
{
    app.Logger.LogWarning(
        "Sale {SaleId} already started with {Stock} in stock; ignoring INITIAL_STOCK={Requested}",
        config.SaleId, startedWith, config.InitialStock);
}

app.MapCheckoutQueue();
app.Logger.LogInformation(
    "{Instance} serving sale {SaleId} on http://127.0.0.1:{Port} (max active {MaxActive})",
    config.InstanceName, config.SaleId, config.Port, config.MaxActiveSessions);
app.Run();
