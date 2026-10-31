// One of the two data nodes. Which one is primary is decided at runtime (see Cluster/ElectionWorker.cs).
using Node;

var config = NodeConfig.FromEnvironment();

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://127.0.0.1:{config.Port}");
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning).AddFilter("System.Net.Http", LogLevel.Warning);
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss.fff ";
});
builder.Services.AddNode(config, TimeProvider.System);

var app = builder.Build();
app.MapNode();
app.Logger.LogInformation(
    "Node {NodeId} on http://127.0.0.1:{Port}: arbiter {Arbiter}, replication {Replication}, peer {Peer}, data {DataDir}",
    config.NodeId, config.Port, NodeConfig.WireName(config.Arbiter), NodeConfig.WireName(config.Replication),
    config.PeerUrl, Path.GetFullPath(config.DataDir));
app.Run();
