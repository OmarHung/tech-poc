namespace Shared;

/// <summary>HttpClient for calls between our own processes. Callers bound every call with their own timeout.</summary>
public static class InternalHttp
{
    public static HttpClient CreateClient() => new(new SocketsHttpHandler
    {
        UseProxy = false,
        AllowAutoRedirect = false,
        // Drop pooled connections quickly: after a network blip, a fresh connection beats a half-dead one.
        PooledConnectionIdleTimeout = TimeSpan.FromSeconds(5),
    })
    {
        Timeout = Timeout.InfiniteTimeSpan,
    };
}
