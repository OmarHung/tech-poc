using Shared;
using Worker.Dispatch;
using Worker.HotHot;

namespace Worker;

public static class WorkerHosting
{
    /// <param name="runLoop">Tests turn the work loop off and drive the pieces themselves.</param>
    public static IServiceCollection AddWorker(this IServiceCollection services, WorkerConfig config, TimeProvider time, bool runLoop = true)
    {
        config.Validate();
        services.AddSingleton(config);
        services.AddSingleton(time);
        services.AddSingleton(_ => InternalHttp.CreateClient());
        services.AddSingleton<WorkClient>();
        services.AddSingleton<WorkerStatus>();
        if (runLoop)
        {
            if (config.Mode == WorkerMode.HotHot)
            {
                services.AddHostedService<HotHotWorker>();
            }
            else
            {
                services.AddHostedService<DispatchWorker>();
            }
        }
        services.AddStrictJson();
        return services;
    }

    // No auth: the PoC binds to 127.0.0.1 only.
    public static WebApplication MapWorker(this WebApplication app)
    {
        var config = app.Services.GetRequiredService<WorkerConfig>();
        app.UseServedBy($"worker-{config.WorkerId}");
        app.UseApiErrors();
        app.MapGet("/health", (WorkerStatus status) => status.Health(config));
        // The hot-hot ledger, so the verify script can check a restarted worker caught up to the same balances.
        app.MapGet("/state", (WorkerStatus status) => status.Ledger.ToFile());
        return app;
    }
}
