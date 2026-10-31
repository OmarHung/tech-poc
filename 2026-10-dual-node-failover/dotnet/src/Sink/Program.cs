// Sink: where results land. Applies each key once and counts every extra copy, so the cost of redundancy is visible.
using Shared;
using Sink;

var port = new EnvReader(Environment.GetEnvironmentVariable).Port("PORT", 8123);

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss.fff ";
});
builder.Services.AddSink(TimeProvider.System);

var app = builder.Build();
app.MapSink();
app.Logger.LogInformation("Sink on http://127.0.0.1:{Port}", port);
app.Run();
