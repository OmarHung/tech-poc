using Microsoft.AspNetCore.Http;

namespace Shared;

/// <summary>An error that maps to an HTTP status. The message is safe to return to callers.</summary>
public abstract class ApiException(int statusCode, string message) : Exception(message)
{
    public int StatusCode { get; } = statusCode;
}

public sealed class InvalidInputException(string message)
    : ApiException(StatusCodes.Status400BadRequest, message);

public sealed class NotFoundException(string message)
    : ApiException(StatusCodes.Status404NotFound, message);

public sealed class ConflictException(string message)
    : ApiException(StatusCodes.Status409Conflict, message);

/// <summary>The request was valid but this process cannot serve it right now; retrying (maybe elsewhere) can work.</summary>
public sealed class UnavailableException(string message)
    : ApiException(StatusCodes.Status503ServiceUnavailable, message);

public sealed record ErrorBody(string Error);
