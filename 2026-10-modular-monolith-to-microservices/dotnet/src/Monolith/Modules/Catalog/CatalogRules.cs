using System.Globalization;

namespace Monolith.Modules.Catalog;

public static class CatalogRules
{
    public const long MaxQuantity = 100;

    public static long ValidateQuantity(long quantity) =>
        quantity is >= 1 and <= MaxQuantity
            ? quantity
            : throw new InvalidInputException($"quantity must be between 1 and {MaxQuantity}");

    public static long ValidateId(long id, string field) => id >= 1 ? id : throw NotPositive(field);

    /// <summary>Parses an id taken from a URL path segment: ASCII digits only, no sign or spaces.</summary>
    public static long ParseId(string raw, string field) =>
        long.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var id)
            ? ValidateId(id, field)
            : throw NotPositive(field);

    private static InvalidInputException NotPositive(string field) => new($"{field} must be a positive integer");
}
