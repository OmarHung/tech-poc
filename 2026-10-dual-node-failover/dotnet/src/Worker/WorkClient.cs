using System.Net;
using System.Net.Http.Json;
using Shared;

namespace Worker;

/// <summary>Calls to the upstream and the sink. Every call is bounded; unreachable means null, never an exception.</summary>
public sealed class WorkClient(HttpClient http, WorkerConfig config, ILogger<WorkClient> logger)
{
    private readonly Uri _upstream = config.UpstreamUrl;
    private readonly Uri _sink = config.SinkUrl;

    public async Task<EventPage?> ReadEventsAsync(long after, int limit, CancellationToken ct)
    {
        var waitMs = (int)config.PollWait.TotalMilliseconds;
        using var response = await SendAsync(
            () => new HttpRequestMessage(HttpMethod.Get, new Uri(_upstream, $"events?after={after}&limit={limit}&waitMs={waitMs}")),
            config.PollWait + config.RpcTimeout, ct);
        return response is { IsSuccessStatusCode: true } ? await ReadAsync<EventPage>(response, ct) : null;
    }

    public async Task<long?> LastEventAsync(CancellationToken ct)
    {
        using var response = await SendAsync(
            () => new HttpRequestMessage(HttpMethod.Get, new Uri(_upstream, "events?after=0&limit=1&waitMs=0")), config.RpcTimeout, ct);
        return response is { IsSuccessStatusCode: true } ? (await ReadAsync<EventPage>(response, ct))?.Last : null;
    }

    /// <returns>
    /// Reached: whether the upstream answered at all (false: refused, reset or timed out, so wait before asking again).
    /// Lease: the task, or null if none came up within the poll wait.
    /// </returns>
    public async Task<(bool Reached, TaskLease? Lease)> LeaseTaskAsync(CancellationToken ct)
    {
        var body = new TaskLeaseRequest(config.WorkerId, (int)config.PollWait.TotalMilliseconds);
        using var response = await SendAsync(() => Post(_upstream, "tasks/lease", body), config.PollWait + config.RpcTimeout, ct);
        return response?.StatusCode switch
        {
            HttpStatusCode.OK => (true, await ReadAsync<TaskLease>(response, ct)),
            HttpStatusCode.NoContent => (true, null),
            _ => (false, null),
        };
    }

    /// <returns>True if the lease was extended, false if it is lost, null if the upstream could not be reached in <paramref name="timeout"/>.</returns>
    public Task<bool?> HeartbeatAsync(TaskLease lease, TimeSpan timeout, CancellationToken ct) => TaskCallAsync(lease, "heartbeat", timeout, ct);

    /// <returns>True if the task is done, false if another attempt owns it now (or the task is gone), null if the upstream could not be reached.</returns>
    public Task<bool?> CompleteAsync(TaskLease lease, CancellationToken ct) => TaskCallAsync(lease, "complete", config.RpcTimeout, ct);

    /// <summary>Whether the sink answers its health check.</summary>
    public async Task<bool> SinkReachableAsync(CancellationToken ct)
    {
        using var response = await SendAsync(() => new HttpRequestMessage(HttpMethod.Get, new Uri(_sink, "health")), config.RpcTimeout, ct);
        return response is { IsSuccessStatusCode: true };
    }

    /// <param name="timeout">Defaults to RPC_TIMEOUT_MS; a dispatched worker passes what is left of its lease.</param>
    /// <returns>The sink's verdict ("applied", "duplicate", "conflict", "stale"), or null if it could not be reached.</returns>
    public async Task<string?> DeliverAsync(ResultRecord record, CancellationToken ct, TimeSpan? timeout = null)
    {
        using var response = await SendAsync(() => Post(_sink, "results", record), timeout ?? config.RpcTimeout, ct);
        if (response is null || (int)response.StatusCode >= 500)
        {
            return null;
        }
        return (await ReadAsync<ResultReply>(response, ct))?.Status;
    }

    private async Task<bool?> TaskCallAsync(TaskLease lease, string action, TimeSpan timeout, CancellationToken ct)
    {
        var body = new TaskAttempt(config.WorkerId, lease.Attempt);
        using var response = await SendAsync(() => Post(_upstream, $"tasks/{lease.Id}/{action}", body), timeout, ct);
        return response?.StatusCode switch
        {
            HttpStatusCode.OK => true,
            // 404: the upstream no longer knows the task (it restarted); retrying will not bring it back.
            HttpStatusCode.Conflict or HttpStatusCode.NotFound => false,
            _ => null,
        };
    }

    private static HttpRequestMessage Post<T>(Uri baseUrl, string path, T body) =>
        new(HttpMethod.Post, new Uri(baseUrl, path)) { Content = JsonContent.Create(body, options: Wire.Json) };

    private async Task<HttpResponseMessage?> SendAsync(Func<HttpRequestMessage> build, TimeSpan timeout, CancellationToken ct)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(timeout);
        HttpResponseMessage? response = null;
        try
        {
            using var request = build();
            response = await http.SendAsync(request, bounded.Token);
            await response.Content.LoadIntoBufferAsync(bounded.Token);
            return response;
        }
        catch (Exception ex) when (ex is HttpRequestException || (ex is OperationCanceledException && !ct.IsCancellationRequested))
        {
            response?.Dispose();
            logger.LogDebug("Call failed: {Error}", ex.Message);
            return null;
        }
    }

    private static async Task<T?> ReadAsync<T>(HttpResponseMessage response, CancellationToken ct)
        where T : class
    {
        try
        {
            return await response.Content.ReadFromJsonAsync<T>(Wire.Json, ct);
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
    }
}
