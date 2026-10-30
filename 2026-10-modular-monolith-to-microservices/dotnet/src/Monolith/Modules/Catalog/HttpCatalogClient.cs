using System.Net;
using System.Text.Json;

namespace Monolith.Modules.Catalog;

/// <summary>
/// <see cref="ICatalog"/> over HTTP: what the monolith uses once the catalog is extracted.
/// Throws the same exceptions as the in-process catalog, so callers cannot tell the
/// difference. Network details are logged, never returned.
/// </summary>
public sealed class HttpCatalogClient(HttpClient http, ILogger<HttpCatalogClient> logger) : ICatalog
{
    public static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(2);

    public static HttpCatalogClient Create(Uri baseUrl, ILogger<HttpCatalogClient> logger)
    {
        var handler = new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(2) };
        return new HttpCatalogClient(new HttpClient(handler) { BaseAddress = baseUrl, Timeout = RequestTimeout }, logger);
    }

    public async Task<IReadOnlyList<Product>> ListAsync(CancellationToken cancellationToken = default) =>
        await SendAsync<List<Product>>(new HttpRequestMessage(HttpMethod.Get, "/products"), cancellationToken);

    public Task<Product> GetAsync(long id, CancellationToken cancellationToken = default) =>
        SendAsync<Product>(new HttpRequestMessage(HttpMethod.Get, $"/products/{id}"), cancellationToken);

    public Task<Product> ReserveAsync(long id, long quantity, CancellationToken cancellationToken = default)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, $"/products/{id}/reserve")
        {
            Content = JsonContent.Create(new ReserveRequest(quantity)),
        };
        return SendAsync<Product>(request, cancellationToken);
    }

    private async Task<T> SendAsync<T>(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        using var _ = request;
        HttpResponseMessage response;
        try
        {
            response = await http.SendAsync(request, cancellationToken);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            logger.LogWarning(ex, "catalog-service request {Method} {Path} failed", request.Method, request.RequestUri);
            throw new CatalogUnavailableException();
        }

        using (response)
        {
            if (!response.IsSuccessStatusCode)
            {
                throw await ToApiExceptionAsync(response, cancellationToken);
            }
            try
            {
                return await response.Content.ReadFromJsonAsync<T>(cancellationToken)
                    ?? throw new JsonException("empty body");
            }
            catch (JsonException ex)
            {
                logger.LogWarning(ex, "catalog-service sent an unexpected body");
                throw new CatalogUnavailableException();
            }
        }
    }

    private async Task<ApiException> ToApiExceptionAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var message = await ReadErrorMessageAsync(response, cancellationToken);
        switch (response.StatusCode)
        {
            case HttpStatusCode.BadRequest:
                return new InvalidInputException(message);
            case HttpStatusCode.NotFound:
                return new NotFoundException(message);
            case HttpStatusCode.Conflict:
                return new InsufficientStockException(message);
            default:
                logger.LogWarning("catalog-service returned {Status}: {Message}", (int)response.StatusCode, message);
                return new CatalogUnavailableException();
        }
    }

    private static async Task<string> ReadErrorMessageAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var fallback = $"catalog-service returned {(int)response.StatusCode}";
        try
        {
            var body = await response.Content.ReadFromJsonAsync<ErrorBody>(cancellationToken);
            return body?.Error ?? fallback;
        }
        catch (JsonException)
        {
            return fallback;
        }
    }
}
