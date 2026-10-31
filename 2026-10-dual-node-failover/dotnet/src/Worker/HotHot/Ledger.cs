using System.Collections.Immutable;
using Shared;

namespace Worker.HotHot;

/// <summary>
/// What a hot-hot worker has computed so far: running balances per account, and the last event it applied.
/// It depends only on the events and their order, so two workers that saw the same events hold the same ledger.
/// </summary>
public sealed record Ledger(long Offset, ImmutableDictionary<string, long> Balances)
{
    public static readonly Ledger Empty = new(0, ImmutableDictionary.Create<string, long>(StringComparer.Ordinal));

    /// <summary>Applies the next event and returns the new ledger with the account's balance after it.</summary>
    public (Ledger Next, long Balance) Apply(LoggedEvent logged)
    {
        if (logged.Seq != Offset + 1)
        {
            throw new InvalidOperationException($"event {logged.Seq} does not follow {Offset}");
        }
        var balance = Balances.GetValueOrDefault(logged.Account) + logged.Amount;
        return (new Ledger(logged.Seq, Balances.SetItem(logged.Account, balance)), balance);
    }

    /// <summary>Moves the position without applying the events in between: whatever they changed is now missing.</summary>
    public Ledger SkipTo(long offset) => this with { Offset = offset };

    public LedgerFile ToFile() => new(Offset, Balances.ToDictionary(StringComparer.Ordinal));

    public static Ledger FromFile(LedgerFile file) =>
        new(file.Offset, file.Balances.ToImmutableDictionary(StringComparer.Ordinal));
}

/// <summary>The checkpoint on disk.</summary>
public sealed record LedgerFile(long Offset, Dictionary<string, long> Balances);
