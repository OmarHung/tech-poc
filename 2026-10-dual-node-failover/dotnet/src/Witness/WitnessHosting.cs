using Shared;

namespace Witness;

public static class WitnessHosting
{
    public static IServiceCollection AddWitness(this IServiceCollection services, WitnessConfig config, TimeProvider time)
    {
        config.Validate();
        services.AddSingleton(config);
        services.AddSingleton(time);
        services.AddSingleton<LeaseTable>();
        services.AddStrictJson();
        return services;
    }

    // No auth: the PoC binds to 127.0.0.1 only. A real witness must only accept the two nodes (mTLS or similar).
    public static WebApplication MapWitness(this WebApplication app)
    {
        app.UseServedBy("witness");
        app.UseApiErrors();
        app.MapGet("/health", (LeaseTable leases) => leases.Health());
        app.MapPost("/lease", async (HttpRequest request, LeaseTable leases, CancellationToken ct) =>
            leases.Acquire((await request.ReadJsonAsync<LeaseRequest>(ct)).Node));
        app.MapPost("/sync", async (HttpRequest request, LeaseTable leases, CancellationToken ct) =>
            leases.SetStandbyInSync(await request.ReadJsonAsync<SyncRequest>(ct)));
        return app;
    }
}
