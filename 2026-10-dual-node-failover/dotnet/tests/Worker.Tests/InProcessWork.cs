using System.Collections.Concurrent;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Shared;
using Sink;
using Upstream;

namespace Worker.Tests;

/// <summary>Upstream, sink and two workers on test servers, with the real work loops on short timings.</summary>
public sealed class InProcessWork : IAsyncDisposable
{
    public static readonly TimeSpan TaskLeaseTtl = TimeSpan.FromMilliseconds(400);

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "worker-tests-" + Guid.NewGuid().ToString("N"));
    private readonly ConcurrentDictionary<string, HttpMessageHandler> _hosts = new();
    private readonly ConcurrentDictionary<string, bool> _down = new();
    private readonly ConcurrentDictionary<(string From, string To), bool> _cut = new();
    private readonly Dictionary<string, WebApplication> _apps = [];

    private InProcessWork()
    {
    }

    public EventLog Events => Service<EventLog>("upstream");
    public TaskQueue Tasks => Service<TaskQueue>("upstream");
    public ResultStore Results => Service<ResultStore>("sink");

    public static async Task<InProcessWork> StartAsync(WorkerMode mode, int workMs = 150)
    {
        var work = new InProcessWork();
        await work.StartAsync("upstream", builder =>
            builder.Services.AddUpstream(new UpstreamConfig { TaskLeaseTtl = TaskLeaseTtl, ReapInterval = TimeSpan.FromMilliseconds(20) }, TimeProvider.System),
            app => app.MapUpstream());
        await work.StartAsync("sink", builder => builder.Services.AddSink(TimeProvider.System), app => app.MapSink());
        foreach (var id in new[] { "a", "b" })
        {
            var config = new WorkerConfig
            {
                WorkerId = id,
                Mode = mode,
                UpstreamUrl = new Uri("http://upstream/"),
                SinkUrl = new Uri("http://sink/"),
                DataDir = Path.Combine(work._dataDir, id),
                WorkTime = TimeSpan.FromMilliseconds(workMs),
                PollWait = TimeSpan.FromMilliseconds(100),
                RpcTimeout = TimeSpan.FromMilliseconds(500),
            };
            await work.StartAsync(id, builder =>
            {
                builder.Services.AddWorker(config, TimeProvider.System);
                builder.Services.AddSingleton(new HttpClient(new Router(work, id)) { Timeout = Timeout.InfiniteTimeSpan });
            }, app => app.MapWorker());
        }
        return work;
    }

    public WorkerStatus Status(string worker) => Service<WorkerStatus>(worker);

    /// <summary>Cuts the link from <paramref name="from"/> to <paramref name="to"/> only.</summary>
    public void Cut(string from, string to) => _cut[(from, to)] = true;

    /// <summary>Stops the worker and makes it unreachable, like kill -9.</summary>
    public async Task KillAsync(string name)
    {
        _down[name] = true;
        await _apps[name].StopAsync();
    }

    public static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"timed out waiting for {what}");
            }
            await Task.Delay(20);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var (name, app) in _apps)
        {
            _down[name] = true;
            await app.DisposeAsync();
        }
        if (Directory.Exists(_dataDir))
        {
            Directory.Delete(_dataDir, recursive: true);
        }
    }

    private T Service<T>(string app)
        where T : notnull => _apps[app].Services.GetRequiredService<T>();

    private async Task StartAsync(string name, Action<WebApplicationBuilder> configure, Action<WebApplication> map)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        configure(builder);
        var app = builder.Build();
        map(app);
        await app.StartAsync();
        _apps[name] = app;
        _hosts[name] = app.GetTestServer().CreateHandler();
    }

    private sealed class Router(InProcessWork work, string from) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var to = request.RequestUri!.Host;
            if (work._down.ContainsKey(from) || work._down.ContainsKey(to) || work._cut.ContainsKey((from, to)))
            {
                throw new HttpRequestException($"{from} cannot reach {to}");
            }
            return new HttpMessageInvoker(work._hosts[to], disposeHandler: false).SendAsync(request, ct);
        }
    }
}
