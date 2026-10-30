namespace CatalogService.Catalog;

public sealed record Product(long Id, string Name, long Price, long Stock);

public sealed record ReserveRequest(long Quantity);

/// <summary>JSON body of every error response: <c>{"error": "..."}</c>.</summary>
public sealed record ErrorBody(string Error);

public static class CatalogSeed
{
    /// <summary>
    /// Same seed data in every language, so the verify scenario sees the same catalog.
    /// Product 3 has a huge stock so load tests never run out.
    /// </summary>
    public static IReadOnlyList<Product> Products { get; } =
    [
        new(1, "Mechanical Keyboard", 3200, 50),
        new(2, "Wireless Mouse", 1500, 100),
        new(3, "USB-C Cable", 290, 1_000_000),
    ];
}
