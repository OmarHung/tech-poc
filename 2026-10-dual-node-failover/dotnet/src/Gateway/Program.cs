// Gateway: the clients' single entry point. Forwards everything to whichever node is primary right now.
using Gateway;

var config = GatewayConfig.FromEnvironment();

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://127.0.0.1:{config.Port}");
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning).AddFilter("Yarp", LogLevel.Error)
    .AddFilter("System.Net.Http", LogLevel.Warning);
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss.fff ";
});
builder.Services.AddGateway(config);

var app = builder.Build();
app.MapGateway();
app.Logger.LogInformation(
    "Gateway on http://127.0.0.1:{Port} for {Nodes}, polling every {PollMs} ms",
    config.Port, string.Join(", ", config.Nodes.Select(n => $"{n.Id}={n.Url}")), config.PollInterval.TotalMilliseconds);
app.Run();
