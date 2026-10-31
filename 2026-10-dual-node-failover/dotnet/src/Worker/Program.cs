// Worker: one of two identical processes that never talk to each other. MODE=hothot reads and processes every
// event (like the other worker does); MODE=dispatch takes tasks the upstream leases to it.
using Worker;

var config = WorkerConfig.FromEnvironment();

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://127.0.0.1:{config.Port}");
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning).AddFilter("System.Net.Http", LogLevel.Warning);
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss.fff ";
});
builder.Services.AddWorker(config, TimeProvider.System);

var app = builder.Build();
app.MapWorker();
app.Logger.LogInformation(
    "Worker {Worker} ({Mode}) on http://127.0.0.1:{Port}, upstream {Upstream}, sink {Sink}, data {DataDir}",
    config.WorkerId, WorkerConfig.WireName(config.Mode), config.Port, config.UpstreamUrl, config.SinkUrl,
    Path.GetFullPath(config.DataDir));
app.Run();
