// Upstream: where the work comes from. An event log both hot-hot workers read in full, and a task queue that leases
// each task to one worker. In production this would be Kafka, a message queue or a job table, itself replicated.
using Upstream;

var config = UpstreamConfig.FromEnvironment();

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://127.0.0.1:{config.Port}");
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss.fff ";
});
builder.Services.AddUpstream(config, TimeProvider.System);

var app = builder.Build();
app.MapUpstream();
app.Logger.LogInformation(
    "Upstream on http://127.0.0.1:{Port}, task lease {TtlMs} ms", config.Port, config.TaskLeaseTtl.TotalMilliseconds);
app.Run();
