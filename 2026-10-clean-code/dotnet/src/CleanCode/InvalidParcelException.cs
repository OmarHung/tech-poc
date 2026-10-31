namespace CleanCode;

/// <summary>
/// 對應 Python 版繼承 ValueError 的 InvalidParcelError。
/// 只傳訊息、不傳 paramName：ArgumentException 有 paramName 時會在 Message 後面加上「(Parameter '...')」。
/// </summary>
public sealed class InvalidParcelException(string message) : ArgumentException(message);
