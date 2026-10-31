using System.Globalization;
using StackExchange.Redis;

namespace CheckoutQueue.Redis;

/// <summary>
/// All Redis access for one sale. Every state change is one Lua script (see Scripts/),
/// so each is atomic on its own and safe to call from any number of instances.
/// </summary>
public sealed class SaleRedis(IConnectionMultiplexer connection, CheckoutQueueConfig config, TimeProvider time)
{
    private readonly IDatabase _db = connection.GetDatabase();
    private readonly RedisValue _prefix = $"cq:{{{config.SaleId}}}:";

    public long NowMs => time.GetUtcNow().ToUnixTimeMilliseconds();

    /// <summary>Sets the stock if this sale has never started. Returns the stock the sale actually started with.</summary>
    public async Task<int> InitializeAsync(int initialStock, CancellationToken ct = default)
    {
        var reply = await RunAsync(SaleScripts.Init, ct, initialStock);
        return reply.Int("initialStock");
    }

    /// <summary>Runs a script with the sale prefix as ARGV[1], followed by <paramref name="args"/>.</summary>
    public async Task<ScriptReply> RunAsync(SaleScript script, CancellationToken ct, params RedisValue[] args)
    {
        var result = await _db.ScriptEvaluateAsync(script.Source, values: [_prefix, .. args]).WaitAsync(ct);
        return ScriptReply.Parse(script.Name, result);
    }

    /// <summary>For scripts that return a list of replies, each shaped like a single script reply.</summary>
    public async Task<IReadOnlyList<ScriptReply>> RunRowsAsync(SaleScript script, CancellationToken ct, params RedisValue[] args)
    {
        var result = await _db.ScriptEvaluateAsync(script.Source, values: [_prefix, .. args]).WaitAsync(ct);
        return ((RedisResult[])result!).Select(row => ScriptReply.Parse(script.Name, row)).ToList();
    }
}

/// <summary>A script's reply: an outcome word, then name/value pairs.</summary>
public sealed record ScriptReply(string Script, string Outcome, IReadOnlyDictionary<string, string> Fields)
{
    public static ScriptReply Parse(string script, RedisResult result)
    {
        var items = (RedisResult[])result!;
        var fields = new Dictionary<string, string>();
        for (var i = 1; i + 1 < items.Length; i += 2)
        {
            fields[items[i].ToString()] = items[i + 1].ToString();
        }
        return new ScriptReply(script, items[0].ToString(), fields);
    }

    public string? Find(string name) => Fields.GetValueOrDefault(name);

    public string Get(string name) =>
        Find(name) ?? throw new InvalidOperationException($"{Script}.lua reply '{Outcome}' is missing '{name}'");

    public long Long(string name) => long.Parse(Get(name), CultureInfo.InvariantCulture);

    public int Int(string name) => int.Parse(Get(name), CultureInfo.InvariantCulture);

    public long? OptionalLong(string name) => Find(name) is { } value ? long.Parse(value, CultureInfo.InvariantCulture) : null;

    /// <summary>For outcomes a caller does not handle: these mean the script and the C# side disagree.</summary>
    public InvalidOperationException Unexpected() => new($"{Script}.lua returned unexpected outcome '{Outcome}'");
}
