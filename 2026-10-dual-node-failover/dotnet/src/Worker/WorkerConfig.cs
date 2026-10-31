using Shared;

namespace Worker;

public enum WorkerMode
{
    /// <summary>Hot-hot redundancy: read every event from the upstream log and process all of them, like the other worker does.</summary>
    HotHot,

    /// <summary>Dispatched worker: take one task at a time from the upstream queue, under a lease.</summary>
    Dispatch,
}

public enum StartFrom
{
    /// <summary>Resume from the saved position and replay what was missed while down.</summary>
    Checkpoint,

    /// <summary>Jump to the newest event, skipping whatever happened while down. The naive way to rejoin; kept as a control group.</summary>
    Latest,
}

public sealed record WorkerConfig
{
    public int Port { get; init; } = 8121;
    public string WorkerId { get; init; } = "a";
    public WorkerMode Mode { get; init; } = WorkerMode.HotHot;
    public Uri UpstreamUrl { get; init; } = new("http://127.0.0.1:8120");
    public Uri SinkUrl { get; init; } = new("http://127.0.0.1:8123");
    public string DataDir { get; init; } = "data/worker-a";
    public StartFrom StartFrom { get; init; } = StartFrom.Checkpoint;

    /// <summary>When false, every result carries this worker's clock, so the two hot-hot workers never agree. Shows why processing must be deterministic.</summary>
    public bool Deterministic { get; init; } = true;

    /// <summary>When false, a dispatched worker writes its result even after losing the lease. Control group for the frozen-worker case.</summary>
    public bool LeaseCheck { get; init; } = true;

    /// <summary>How long one dispatched task takes.</summary>
    public TimeSpan WorkTime { get; init; } = TimeSpan.FromMilliseconds(300);

    /// <summary>Timeout of every call to the upstream or the sink, on top of any long-poll wait.</summary>
    public TimeSpan RpcTimeout { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>How long a read of the event log or a task lease request waits for new work.</summary>
    public TimeSpan PollWait { get; init; } = TimeSpan.FromMilliseconds(500);

    public TimeSpan RetryDelay => TimeSpan.FromMilliseconds(100);

    public static WorkerConfig FromEnvironment() => FromLookup(Environment.GetEnvironmentVariable);

    public static WorkerConfig FromLookup(Func<string, string?> lookup)
    {
        var env = new EnvReader(lookup);
        var d = new WorkerConfig();
        var id = env.String("WORKER_ID", d.WorkerId);
        var config = new WorkerConfig
        {
            Port = env.Port("PORT", d.Port),
            WorkerId = id,
            Mode = env.Choice("MODE", d.Mode, new Dictionary<string, WorkerMode>
            {
                ["hothot"] = WorkerMode.HotHot,
                ["dispatch"] = WorkerMode.Dispatch,
            }),
            UpstreamUrl = env.Url("UPSTREAM_URL", d.UpstreamUrl),
            SinkUrl = env.Url("SINK_URL", d.SinkUrl),
            DataDir = env.String("DATA_DIR", $"data/worker-{id}"),
            StartFrom = env.Choice("START_FROM", d.StartFrom, new Dictionary<string, StartFrom>
            {
                ["checkpoint"] = StartFrom.Checkpoint,
                ["latest"] = StartFrom.Latest,
            }),
            Deterministic = env.Choice("DETERMINISTIC", d.Deterministic, new Dictionary<string, bool> { ["true"] = true, ["false"] = false }),
            LeaseCheck = env.Choice("LEASE_CHECK", d.LeaseCheck, new Dictionary<string, bool> { ["on"] = true, ["off"] = false }),
            WorkTime = env.Millis("WORK_MS", d.WorkTime, 0, 60_000),
            RpcTimeout = env.Millis("RPC_TIMEOUT_MS", d.RpcTimeout, 10, 30_000),
        };
        config.Validate();
        return config;
    }

    public void Validate()
    {
        if (!NodeIds.IsValid(WorkerId))
        {
            throw new InvalidOperationException($"WORKER_ID must be 1-32 lowercase letters, digits or '-', got '{WorkerId}'");
        }
    }

    public static string WireName(WorkerMode mode) => mode == WorkerMode.Dispatch ? "dispatch" : "hothot";
}
