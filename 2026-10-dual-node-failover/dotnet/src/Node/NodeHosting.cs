using Node.Cluster;
using Node.Kv;
using Node.Storage;
using Shared;

namespace Node;

public sealed record PutBody(string? Value);

public static class NodeHosting
{
    private const string LogFileName = "log.jsonl";

    /// <param name="runWorkers">Tests turn election and replication off and drive <see cref="NodeState"/> themselves.</param>
    public static IServiceCollection AddNode(
        this IServiceCollection services,
        NodeConfig config,
        TimeProvider time,
        bool runWorkers = true)
    {
        config.Validate();
        services.AddSingleton(config);
        services.AddSingleton(time);
        services.AddSingleton(_ => ReplicatedLog.Open(Path.Combine(config.DataDir, LogFileName)));
        services.AddSingleton<NodeState>();
        services.AddSingleton(_ => RpcClient.CreateHttpClient());
        services.AddSingleton<RpcClient>();
        services.AddSingleton<WitnessClient>();
        if (config.Arbiter == ArbiterMode.Witness)
        {
            services.AddSingleton<IArbiter>(sp => sp.GetRequiredService<WitnessClient>());
        }
        else
        {
            services.AddSingleton<IArbiter, NoArbiter>();
        }
        services.AddSingleton<KvService>();
        if (runWorkers)
        {
            services.AddHostedService<ElectionWorker>();
            services.AddHostedService<ReplicationWorker>();
        }
        services.AddStrictJson();
        return services;
    }

    // No auth anywhere: the PoC binds to 127.0.0.1. A real deployment must at least keep /internal and /admin
    // off the client network and authenticate the peer (mTLS or similar).
    public static WebApplication MapNode(this WebApplication app)
    {
        var config = app.Services.GetRequiredService<NodeConfig>();
        app.UseServedBy($"node-{config.NodeId}");
        app.UseApiErrors();

        app.MapGet("/health", (NodeState state) => state.Health());

        app.MapPut("/kv/{key}", async (string key, HttpRequest request, KvService kv, CancellationToken ct) =>
        {
            KvService.ValidateKey(key);
            var body = await request.ReadJsonAsync<PutBody>(ct);
            KvService.ValidateValue(body.Value);
            return await kv.PutAsync(key, body.Value!, ct);
        });

        app.MapGet("/kv/{key}", (string key, KvService kv, CancellationToken ct) =>
        {
            KvService.ValidateKey(key);
            return kv.GetAsync(key, ct);
        });

        app.MapPost("/internal/replicate", async (HttpRequest request, NodeState state, CancellationToken ct) =>
            state.HandleReplicate(await request.ReadJsonAsync<ReplicateRequest>(ct)));

        // Everything this node holds, primary or not. The verify script compares the two nodes with it.
        app.MapGet("/admin/data", (NodeState state) =>
        {
            var health = state.Health();
            return new { node = health.Node, role = health.Role, epoch = health.Epoch, lastIndex = health.LastIndex, data = state.Log.Snapshot() };
        });
        return app;
    }
}
