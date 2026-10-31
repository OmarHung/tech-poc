namespace CheckoutQueue.Redis;

/// <summary>Scripts and JSON both use snake_case ("pending_payment"); C# uses PascalCase enums.</summary>
public static class WireName
{
    public static T Parse<T>(string wire)
        where T : struct, Enum =>
        Enum.TryParse<T>(wire.Replace("_", ""), ignoreCase: true, out var value)
            ? value
            : throw new InvalidOperationException($"unknown {typeof(T).Name} '{wire}'");
}
