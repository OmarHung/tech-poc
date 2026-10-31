using Shared;

namespace Upstream;

public static class UpstreamHosting
{
    private static readonly TimeSpan MaxWait = TimeSpan.FromSeconds(10);

    /// <param name="runReaper">Tests turn the expiry timer off and call <see cref="TaskQueue.ExpireOverdue"/> themselves.</param>
    public static IServiceCollection AddUpstream(this IServiceCollection services, UpstreamConfig config, TimeProvider time, bool runReaper = true)
    {
        services.AddSingleton(config);
        services.AddSingleton(time);
        services.AddSingleton<EventLog>();
        services.AddSingleton<TaskQueue>();
        if (runReaper)
        {
            services.AddHostedService<LeaseReaper>();
        }
        services.AddStrictJson();
        return services;
    }

    // No auth: the PoC binds to 127.0.0.1 only.
    public static WebApplication MapUpstream(this WebApplication app)
    {
        app.UseServedBy("upstream");
        app.UseApiErrors();
        app.MapGet("/health", (EventLog events, TaskQueue tasks) =>
            new { status = "ok", service = "upstream", lastEvent = events.Last, tasks = tasks.Stats() });

        app.MapPost("/events", async (HttpRequest request, EventLog events, CancellationToken ct) =>
            Results.Created((string?)null, events.Append(await request.ReadJsonAsync<NewEvent>(ct))));
        app.MapGet("/events", (long? after, int? limit, int? waitMs, EventLog events, TimeProvider time, CancellationToken ct) =>
            events.ReadAsync(after ?? 0, limit ?? 100, Wait(waitMs), time, ct));

        app.MapPost("/tasks", async (HttpRequest request, TaskQueue tasks, CancellationToken ct) =>
            Results.Created((string?)null, new NewTaskReply(tasks.Add((await request.ReadJsonAsync<NewTask>(ct)).Input))));
        app.MapPost("/tasks/lease", async (HttpRequest request, TaskQueue tasks, CancellationToken ct) =>
        {
            var body = await request.ReadJsonAsync<TaskLeaseRequest>(ct);
            var lease = await tasks.LeaseAsync(body.Worker, Wait(body.WaitMs), ct);
            return lease is null ? Results.NoContent() : Results.Ok(lease);
        });
        app.MapPost("/tasks/{id:long}/heartbeat", async (long id, HttpRequest request, TaskQueue tasks, CancellationToken ct) =>
            tasks.Heartbeat(id, await request.ReadJsonAsync<TaskAttempt>(ct))
                ? Results.Ok()
                : Results.Conflict(new ErrorBody("lease lost")));
        app.MapPost("/tasks/{id:long}/complete", async (long id, HttpRequest request, TaskQueue tasks, CancellationToken ct) =>
            tasks.Complete(id, await request.ReadJsonAsync<TaskAttempt>(ct))
                ? Results.Ok()
                : Results.Conflict(new ErrorBody("lease lost; another attempt owns this task")));
        app.MapGet("/tasks/stats", (TaskQueue tasks) => tasks.Stats());
        return app;
    }

    private static TimeSpan Wait(int? waitMs)
    {
        var wait = TimeSpan.FromMilliseconds(waitMs ?? 0);
        return wait < TimeSpan.Zero ? TimeSpan.Zero : wait > MaxWait ? MaxWait : wait;
    }
}

/// <summary>Requeues expired task leases even when no worker is asking, so a waiting worker is woken right away.</summary>
public sealed class LeaseReaper(TaskQueue tasks, UpstreamConfig config, TimeProvider time) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(config.ReapInterval, time);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            tasks.ExpireOverdue();
        }
    }
}
