namespace Monolith;

public enum CatalogMode
{
    /// <summary>Catalog module runs inside the monolith process (before extraction).</summary>
    InProcess,

    /// <summary>Catalog lives in CatalogService and is called over HTTP (after extraction).</summary>
    Remote,
}

public sealed record MonolithConfig(int Port, CatalogMode CatalogMode, Uri CatalogUrl)
{
    public static MonolithConfig FromEnvironment() => new(
        ParsePort(Env("PORT", "8081")),
        ParseMode(Env("CATALOG_MODE", "inprocess")),
        new Uri(Env("CATALOG_URL", "http://127.0.0.1:8082")));

    public static CatalogMode ParseMode(string value) => value switch
    {
        "inprocess" => CatalogMode.InProcess,
        "remote" => CatalogMode.Remote,
        _ => throw new InvalidOperationException($"CATALOG_MODE must be 'inprocess' or 'remote', got '{value}'"),
    };

    private static int ParsePort(string value) =>
        int.TryParse(value, out var port) && port is >= 1 and <= 65535
            ? port
            : throw new InvalidOperationException($"PORT must be between 1 and 65535, got '{value}'");

    private static string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) ?? fallback;
}

public static class CatalogModeExtensions
{
    public static string ToWireName(this CatalogMode mode) => mode == CatalogMode.Remote ? "remote" : "inprocess";
}
