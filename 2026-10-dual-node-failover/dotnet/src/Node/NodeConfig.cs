using Shared;

namespace Node;

public enum ArbiterMode
{
    /// <summary>A third process hands out the primary lease (the design under test).</summary>
    Witness,

    /// <summary>The standby promotes itself when the primary goes quiet. The classic setup that can split-brain; kept as the control group.</summary>
    None,
}

public enum ReplicationMode
{
    /// <summary>A write is acknowledged once it is on the standby too (or the witness agreed the standby is out).</summary>
    Sync,

    /// <summary>A write is acknowledged once it is on this node; the standby catches up in the background.</summary>
    Async,
}

public sealed record NodeConfig
{
    public int Port { get; init; } = 8101;
    public string NodeId { get; init; } = "a";
    public Uri PeerUrl { get; init; } = new("http://127.0.0.1:8102");
    public Uri WitnessUrl { get; init; } = new("http://127.0.0.1:8103");
    public string DataDir { get; init; } = "data/a";
    public ArbiterMode Arbiter { get; init; } = ArbiterMode.Witness;
    public ReplicationMode Replication { get; init; } = ReplicationMode.Sync;

    /// <summary>How long a lease lasts, and how long a standby must hear nothing from the primary before it tries to take over.</summary>
    public TimeSpan LeaseTtl { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>How often the primary renews its lease and heartbeats the standby. Also the timeout of every call to them.</summary>
    public TimeSpan RenewInterval { get; init; } = TimeSpan.FromMilliseconds(500);

    /// <summary>How long a sync write waits for the standby before asking the witness to go on without it.</summary>
    public TimeSpan ReplicationTimeout { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Extra wait before this node tries to become primary. Without a witness it plays the part of keepalived's priority.</summary>
    public TimeSpan ElectionDelay { get; init; } = TimeSpan.Zero;

    /// <summary>The primary treats its leases as ending this much earlier than the other side does, to absorb clock drift.</summary>
    public TimeSpan LeaseSafetyMargin => LeaseTtl / 10;

    public static NodeConfig FromEnvironment() => FromLookup(Environment.GetEnvironmentVariable);

    public static NodeConfig FromLookup(Func<string, string?> lookup)
    {
        var env = new EnvReader(lookup);
        var d = new NodeConfig();
        var nodeId = env.String("NODE_ID", d.NodeId);
        var config = new NodeConfig
        {
            Port = env.Port("PORT", d.Port),
            NodeId = nodeId,
            PeerUrl = env.Url("PEER_URL", d.PeerUrl),
            WitnessUrl = env.Url("WITNESS_URL", d.WitnessUrl),
            DataDir = env.String("DATA_DIR", $"data/{nodeId}"),
            Arbiter = env.Choice("ARBITER", d.Arbiter, new Dictionary<string, ArbiterMode>
            {
                ["witness"] = ArbiterMode.Witness,
                ["none"] = ArbiterMode.None,
            }),
            Replication = env.Choice("REPLICATION", d.Replication, new Dictionary<string, ReplicationMode>
            {
                ["sync"] = ReplicationMode.Sync,
                ["async"] = ReplicationMode.Async,
            }),
            LeaseTtl = env.Millis("LEASE_TTL_MS", d.LeaseTtl, 100, 60_000),
            RenewInterval = env.Millis("RENEW_INTERVAL_MS", d.RenewInterval, 10, 20_000),
            ReplicationTimeout = env.Millis("REPLICATION_TIMEOUT_MS", d.ReplicationTimeout, 10, 30_000),
            ElectionDelay = env.Millis("ELECTION_DELAY_MS", d.ElectionDelay, 0, 60_000),
        };
        config.Validate();
        return config;
    }

    /// <summary>Checks the rules that span several settings. Throws with the variable name to fix.</summary>
    public void Validate()
    {
        if (!NodeIds.IsValid(NodeId))
        {
            throw new InvalidOperationException($"NODE_ID must be 1-32 lowercase letters, digits or '-', got '{NodeId}'");
        }
        // One slow round trip must not cost the primary its lease: it gets several renewals per lease.
        if (LeaseTtl < RenewInterval * 3)
        {
            throw new InvalidOperationException(
                $"LEASE_TTL_MS ({LeaseTtl.TotalMilliseconds}) must be at least 3 x RENEW_INTERVAL_MS ({RenewInterval.TotalMilliseconds})");
        }
        // A write stuck on the standby must still have lease left when it asks the witness to go on alone.
        if (ReplicationTimeout * 2 > LeaseTtl)
        {
            throw new InvalidOperationException(
                $"REPLICATION_TIMEOUT_MS ({ReplicationTimeout.TotalMilliseconds}) must be at most half of LEASE_TTL_MS ({LeaseTtl.TotalMilliseconds})");
        }
    }

    public static string WireName(ArbiterMode mode) => mode == ArbiterMode.None ? "none" : "witness";

    public static string WireName(ReplicationMode mode) => mode == ReplicationMode.Async ? "async" : "sync";
}
