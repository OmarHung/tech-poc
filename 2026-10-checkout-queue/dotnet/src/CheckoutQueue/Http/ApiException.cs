namespace CheckoutQueue.Http;

/// <summary>
/// An error that maps to an HTTP status. The message is safe to show callers; the code is
/// what a client branches on (several different 409s need different handling).
/// </summary>
public sealed class ApiException(int statusCode, string code, string message) : Exception(message)
{
    public int StatusCode { get; } = statusCode;
    public string Code { get; } = code;

    public static ApiException BadRequest(string message) => new(StatusCodes.Status400BadRequest, "invalid_request", message);

    public static ApiException NotFound(string code, string message) => new(StatusCodes.Status404NotFound, code, message);

    public static ApiException Forbidden(string code, string message) => new(StatusCodes.Status403Forbidden, code, message);

    public static ApiException Conflict(string code, string message) => new(StatusCodes.Status409Conflict, code, message);

    public static ApiException Gone(string code, string message) => new(StatusCodes.Status410Gone, code, message);
}

public sealed record ErrorBody(string Error, string Code);
