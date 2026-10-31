using Shared;

namespace Witness;

public sealed record WitnessConfig
{
    public int Port { get; init; } = 8103;
    public string DataDir { get; init; } = "data/witness";
    public TimeSpan LeaseTtl { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>The two node ids allowed to hold the lease.</summary>
    public IReadOnlyList<string> Nodes { get; init; } = ["a", "b"];

    public static WitnessConfig FromEnvironment() => FromLookup(Environment.GetEnvironmentVariable);

    public static WitnessConfig FromLookup(Func<string, string?> lookup)
    {
        var env = new EnvReader(lookup);
        var d = new WitnessConfig();
        var config = new WitnessConfig
        {
            Port = env.Port("PORT", d.Port),
            DataDir = env.String("DATA_DIR", d.DataDir),
            LeaseTtl = env.Millis("LEASE_TTL_MS", d.LeaseTtl, 100, 60_000),
            Nodes = env.String("NODES", string.Join(',', d.Nodes)).Split(',', StringSplitOptions.TrimEntries),
        };
        config.Validate();
        return config;
    }

    public void Validate()
    {
        if (Nodes.Count != 2 || Nodes[0] == Nodes[1] || !Nodes.All(NodeIds.IsValid))
        {
            throw new InvalidOperationException(
                $"NODES must be two different ids of lowercase letters, digits or '-', got '{string.Join(',', Nodes)}'");
        }
    }
}
