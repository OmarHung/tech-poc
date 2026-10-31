using Shared;

namespace Sink;

public static class SinkHosting
{
    public static IServiceCollection AddSink(this IServiceCollection services, TimeProvider time)
    {
        services.AddSingleton(time);
        services.AddSingleton<ResultStore>();
        services.AddStrictJson();
        return services;
    }

    // No auth: the PoC binds to 127.0.0.1 only.
    public static WebApplication MapSink(this WebApplication app)
    {
        app.UseServedBy("sink");
        app.UseApiErrors();
        app.MapGet("/health", (ResultStore store) => new { status = "ok", service = "sink", summary = store.Snapshot().Summary });
        app.MapPost("/results", async (HttpRequest request, ResultStore store, CancellationToken ct) =>
        {
            var status = store.Record(await request.ReadJsonAsync<ResultRecord>(ct));
            var reply = new ResultReply(WireName(status));
            return status switch
            {
                ResultStatus.Applied => Results.Created((string?)null, reply),
                ResultStatus.Duplicate => Results.Ok(reply),
                _ => Results.Conflict(reply),
            };
        });
        app.MapGet("/results", (ResultStore store) => store.Snapshot());
        return app;
    }

    public static string WireName(ResultStatus status) => status switch
    {
        ResultStatus.Applied => "applied",
        ResultStatus.Duplicate => "duplicate",
        ResultStatus.Conflict => "conflict",
        _ => "stale",
    };
}
