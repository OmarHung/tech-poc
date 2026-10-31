using System.Net.Http.Json;
using Shared;

namespace Gateway;

/// <summary>The part of a node's /health the gateway needs.</summary>
public sealed record NodeStatus(string? Role, long Epoch);

public sealed record PrimaryChoice(NodeTarget Target, long Epoch);

/// <summary>
/// Polls every node's /health and remembers which one is primary, the way HAProxy is pointed at
/// Patroni's /primary check. If two nodes claim it (only possible without a witness), the newer epoch wins.
/// </summary>
public sealed class PrimaryTracker(GatewayConfig config, HttpMessageInvoker upstream, ILogger<PrimaryTracker> logger) : BackgroundService
{
    private readonly HttpClient _http = new(new InvokerHandler(upstream), disposeHandler: false) { Timeout = Timeout.InfiniteTimeSpan };
    private readonly SemaphoreSlim _nudge = new(0, 1);
    private volatile PrimaryChoice? _current;

    public PrimaryChoice? Current => _current;

    /// <summary>Polls again right away, e.g. after the primary failed a request.</summary>
    public void Nudge()
    {
        if (_nudge.CurrentCount == 0)
        {
            try
            {
                _nudge.Release();
            }
            catch (SemaphoreFullException)
            {
                // Someone else nudged first; one pending poll is enough.
            }
        }
    }

    public static PrimaryChoice? Choose(IEnumerable<(NodeTarget Target, NodeStatus? Status)> polled) =>
        polled
            .Where(p => p.Status?.Role == "primary")
            .OrderByDescending(p => p.Status!.Epoch)
            .Select(p => new PrimaryChoice(p.Target, p.Status!.Epoch))
            .FirstOrDefault();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var polled = await Task.WhenAll(config.Nodes.Select(node => PollAsync(node, stoppingToken)));
            var choice = Choose(polled);
            var previous = _current;
            _current = choice;
            if (choice?.Target.Id != previous?.Target.Id || choice?.Epoch != previous?.Epoch)
            {
                logger.LogWarning("Primary is now {Node} (epoch {Epoch})", choice?.Target.Id ?? "none", choice?.Epoch);
            }
            await _nudge.WaitAsync(config.PollInterval, stoppingToken);
        }
    }

    private async Task<(NodeTarget, NodeStatus?)> PollAsync(NodeTarget node, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(HealthTimeout);
        try
        {
            return (node, await _http.GetFromJsonAsync<NodeStatus>(new Uri(node.Url, "health"), Wire.Json, timeout.Token));
        }
        // NotSupportedException: a 200 that is not JSON. A node answering nonsense is a node to skip, not a reason to stop polling.
        catch (Exception ex) when (ex is HttpRequestException or System.Text.Json.JsonException or NotSupportedException
            or OperationCanceledException && !ct.IsCancellationRequested)
        {
            return (node, null);
        }
    }

    private TimeSpan HealthTimeout => config.PollInterval * 3 > MinHealthTimeout ? config.PollInterval * 3 : MinHealthTimeout;

    private static readonly TimeSpan MinHealthTimeout = TimeSpan.FromMilliseconds(300);

    public override void Dispose()
    {
        _http.Dispose();
        _nudge.Dispose();
        base.Dispose();
    }

    /// <summary>Lets the health polls share the forwarder's connection pool (and the test handler).</summary>
    private sealed class InvokerHandler(HttpMessageInvoker invoker) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) =>
            invoker.SendAsync(request, ct);
    }
}
