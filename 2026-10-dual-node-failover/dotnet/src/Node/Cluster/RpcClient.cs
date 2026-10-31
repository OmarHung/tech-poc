using System.Net.Http.Json;
using Shared;

namespace Node.Cluster;

/// <summary>JSON POSTs to the peer or the witness, each bounded by RENEW_INTERVAL_MS. Unreachable means null, never an exception.</summary>
public sealed class RpcClient(HttpClient http, NodeConfig config, ILogger<RpcClient> logger)
{
    /// <summary>Returns the response (any status) or null if the target could not be reached in time.</summary>
    public async Task<HttpResponseMessage?> PostAsync<T>(Uri url, T body, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(config.RenewInterval);
        HttpResponseMessage? response = null;
        try
        {
            response = await http.PostAsJsonAsync(url, body, Wire.Json, timeout.Token);
            await response.Content.LoadIntoBufferAsync(timeout.Token);
            return response;
        }
        catch (Exception ex) when (ex is HttpRequestException || (ex is OperationCanceledException && !ct.IsCancellationRequested))
        {
            response?.Dispose();
            logger.LogDebug("{Url} unreachable: {Error}", url, ex.Message);
            return null;
        }
    }

    public static async Task<T?> ReadAsync<T>(HttpResponseMessage response, CancellationToken ct)
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

    public static HttpClient CreateHttpClient() => InternalHttp.CreateClient();
}
