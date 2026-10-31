// Witness: the third vote that lets two nodes fail over without both becoming primary.
using Witness;

var config = WitnessConfig.FromEnvironment();

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://127.0.0.1:{config.Port}");
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss.fff ";
});
builder.Services.AddWitness(config, TimeProvider.System);

var app = builder.Build();
app.MapWitness();
app.Logger.LogInformation(
    "Witness for nodes {Nodes} on http://127.0.0.1:{Port}, lease {TtlMs} ms",
    string.Join(", ", config.Nodes), config.Port, config.LeaseTtl.TotalMilliseconds);
app.Run();
