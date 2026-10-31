using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shared;

/// <summary>JSON settings for traffic between our own processes and for state files.</summary>
public static class Wire
{
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        NumberHandling = JsonNumberHandling.Strict,
    };
}

/// <summary>Node ids appear in URLs, file names and logs, so they are kept to a safe alphabet.</summary>
public static class NodeIds
{
    private const int MaxLength = 32;

    public static bool IsValid(string? id) =>
        id is { Length: > 0 and <= MaxLength } && id.All(c => c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or '-');
}
