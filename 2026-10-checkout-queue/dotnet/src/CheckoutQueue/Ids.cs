using System.Security.Cryptography;

namespace CheckoutQueue;

public static class Ids
{
    /// <summary>
    /// 128 random bits as 32 lowercase hex characters. Holding a ticket id is what lets you
    /// check out, so it must not be guessable from someone else's.
    /// </summary>
    public static string New() => Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16));

    public static bool IsValid(string? id) => id is { Length: 32 } && id.All(char.IsAsciiHexDigitLower);
}
