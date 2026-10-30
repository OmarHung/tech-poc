using Microsoft.AspNetCore.Http;

namespace CatalogService.Catalog;

/// <summary>
/// An error that maps to an HTTP status. The message is safe to return to callers,
/// and the remote client turns a status code plus message back into the same type.
/// </summary>
public abstract class ApiException(int statusCode, string message) : Exception(message)
{
    public int StatusCode { get; } = statusCode;
}

public sealed class InvalidInputException(string message)
    : ApiException(StatusCodes.Status400BadRequest, message);

public sealed class NotFoundException(string message)
    : ApiException(StatusCodes.Status404NotFound, message);

public sealed class InsufficientStockException(string message)
    : ApiException(StatusCodes.Status409Conflict, message);

public sealed class CatalogUnavailableException()
    : ApiException(StatusCodes.Status502BadGateway, "catalog unavailable");

public static class CatalogErrors
{
    public static NotFoundException ProductNotFound(long id) => new($"product {id} not found");

    public static InsufficientStockException InsufficientStock(long id, long requested, long available) =>
        new($"insufficient stock for product {id}: requested {requested}, available {available}");
}
