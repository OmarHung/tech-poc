using System.Globalization;

namespace Shared;

/// <summary>Reads settings from environment-style lookups and fails with the variable name to fix.</summary>
public sealed class EnvReader(Func<string, string?> lookup)
{
    public string String(string name, string fallback) => lookup(name) is { Length: > 0 } value ? value : fallback;

    public int Port(string name, int fallback) => Int(name, fallback, 1, 65535);

    public int Int(string name, int fallback, int min, int max)
    {
        var raw = lookup(name);
        if (string.IsNullOrEmpty(raw))
        {
            return fallback;
        }
        return int.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var value) && value >= min && value <= max
            ? value
            : throw new InvalidOperationException($"{name} must be an integer between {min} and {max}, got '{raw}'");
    }

    public TimeSpan Millis(string name, TimeSpan fallback, int minMs, int maxMs) =>
        TimeSpan.FromMilliseconds(Int(name, (int)fallback.TotalMilliseconds, minMs, maxMs));

    public Uri Url(string name, Uri fallback)
    {
        var raw = lookup(name);
        if (string.IsNullOrEmpty(raw))
        {
            return fallback;
        }
        return ParseUrl(name, raw);
    }

    public T Choice<T>(string name, T fallback, IReadOnlyDictionary<string, T> options)
    {
        var raw = lookup(name);
        if (string.IsNullOrEmpty(raw))
        {
            return fallback;
        }
        return options.TryGetValue(raw, out var value)
            ? value
            : throw new InvalidOperationException($"{name} must be one of {string.Join(", ", options.Keys)}, got '{raw}'");
    }

    public static Uri ParseUrl(string name, string raw) =>
        Uri.TryCreate(raw, UriKind.Absolute, out var url) && url.Scheme is "http" or "https"
            ? url
            : throw new InvalidOperationException($"{name} must be an http(s) URL, got '{raw}'");
}
