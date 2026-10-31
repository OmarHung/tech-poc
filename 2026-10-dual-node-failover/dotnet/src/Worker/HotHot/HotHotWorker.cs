using Shared;

namespace Worker.HotHot;

/// <summary>
/// Reads every event from the upstream log, applies it to the ledger and sends the result to the sink. The other
/// worker does exactly the same; neither knows the other exists. After each batch the position and balances are
/// saved, so a restart replays only what was not yet saved (and re-sends those results, which the sink ignores).
/// </summary>
public sealed class HotHotWorker(
    WorkClient client,
    WorkerStatus status,
    WorkerConfig config,
    TimeProvider time,
    ILogger<HotHotWorker> logger) : BackgroundService
{
    public const int Batch = 100;
    private const string CheckpointFile = "checkpoint.json";

    private readonly string _checkpointPath = Path.Combine(config.DataDir, CheckpointFile);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var ledger = Load();
        if (config.StartFrom == StartFrom.Latest)
        {
            ledger = await SkipToLatestAsync(ledger, stoppingToken);
        }
        status.Ledger = ledger;
        logger.LogInformation("Hot-hot worker {Worker} starting after event {Offset}", config.WorkerId, ledger.Offset);
        while (!stoppingToken.IsCancellationRequested)
        {
            var page = await client.ReadEventsAsync(ledger.Offset, Batch, stoppingToken);
            if (page is null)
            {
                await Task.Delay(config.RetryDelay, time, stoppingToken);
                continue;
            }
            if (page.Last < ledger.Offset)
            {
                // The upstream has fewer events than this worker already applied: it lost history (an in-memory
                // upstream restarted). New events would reuse old numbers, so stop rather than send wrong results.
                logger.LogError(
                    "Upstream ends at event {Last} but this worker is at {Offset}; it lost events. Not processing.",
                    page.Last, ledger.Offset);
                await Task.Delay(config.PollWait, time, stoppingToken);
                continue;
            }
            foreach (var logged in page.Events)
            {
                var (next, balance) = ledger.Apply(logged);
                await DeliverAsync(new ResultRecord($"event-{logged.Seq}", config.WorkerId, 0, Format(logged, balance)), stoppingToken);
                ledger = next;
            }
            if (page.Events.Count > 0)
            {
                // After delivering: a crash before this line means re-sending the batch, never skipping it.
                Save(ledger);
                status.Ledger = ledger;
            }
        }
    }

    private string Format(LoggedEvent logged, long balance) =>
        config.Deterministic
            ? $"{logged.Account}:{balance}"
            : $"{logged.Account}:{balance}@{time.GetUtcNow().ToUnixTimeMilliseconds()}";

    /// <summary>Retries until the sink answers. Any answer counts: a conflict is recorded there, retrying will not change it.</summary>
    private async Task DeliverAsync(ResultRecord record, CancellationToken ct)
    {
        string? reply;
        while ((reply = await client.DeliverAsync(record, ct)) is null)
        {
            await Task.Delay(config.RetryDelay, time, ct);
        }
        status.SinkReplied(reply);
        if (reply == "conflict")
        {
            logger.LogWarning("Sink holds a different result for {Key} than {Value}", record.Key, record.Value);
        }
    }

    private async Task<Ledger> SkipToLatestAsync(Ledger ledger, CancellationToken ct)
    {
        long? last;
        while ((last = await client.LastEventAsync(ct)) is null)
        {
            await Task.Delay(config.RetryDelay, time, ct);
        }
        if (last <= ledger.Offset)
        {
            return ledger;
        }
        logger.LogWarning("Skipping events {From}-{To} (START_FROM=latest); balances will miss them", ledger.Offset + 1, last);
        var skipped = ledger.SkipTo(last.Value);
        Save(skipped);
        return skipped;
    }

    private Ledger Load() =>
        JsonStateFile.Load<LedgerFile>(_checkpointPath) is { } file ? Ledger.FromFile(file) : Ledger.Empty;

    private void Save(Ledger ledger) => JsonStateFile.Save(_checkpointPath, ledger.ToFile());
}
