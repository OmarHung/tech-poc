using Shared;

namespace Gateway;

public sealed record NodeTarget(string Id, Uri Url);

public sealed record GatewayConfig
{
    public int Port { get; init; } = 8100;

    public IReadOnlyList<NodeTarget> Nodes { get; init; } =
    [
        new("a", new Uri("http://127.0.0.1:8101")),
        new("b", new Uri("http://127.0.0.1:8102")),
    ];

    /// <summary>How often every node's /health is polled to find the primary. Part of the failover time.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(100);

    public static readonly TimeSpan UpstreamTimeout = TimeSpan.FromSeconds(5);

    public static GatewayConfig FromEnvironment() => FromLookup(Environment.GetEnvironmentVariable);

    public static GatewayConfig FromLookup(Func<string, string?> lookup)
    {
        var env = new EnvReader(lookup);
        var d = new GatewayConfig();
        var rawNodes = lookup("NODE_URLS");
        return new GatewayConfig
        {
            Port = env.Port("PORT", d.Port),
            Nodes = string.IsNullOrEmpty(rawNodes) ? d.Nodes : ParseNodes(rawNodes),
            PollInterval = env.Millis("POLL_INTERVAL_MS", d.PollInterval, 10, 10_000),
        };
    }

    /// <summary>Parses <c>a=http://host:port,b=http://host:port</c>.</summary>
    public static IReadOnlyList<NodeTarget> ParseNodes(string raw)
    {
        var nodes = raw.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Select(part => part.Split('=', 2))
            .Select(pair => pair.Length == 2 && NodeIds.IsValid(pair[0])
                ? new NodeTarget(pair[0], EnvReader.ParseUrl("NODE_URLS", pair[1]))
                : throw new InvalidOperationException($"NODE_URLS entries must look like a=http://127.0.0.1:8101, got '{string.Join('=', pair)}'"))
            .ToList();
        if (nodes.Count == 0 || nodes.Select(n => n.Id).Distinct().Count() != nodes.Count)
        {
            throw new InvalidOperationException($"NODE_URLS must list distinct node ids, got '{raw}'");
        }
        return nodes;
    }
}
