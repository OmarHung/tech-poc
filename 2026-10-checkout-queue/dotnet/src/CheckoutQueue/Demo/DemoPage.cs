using System.Security.Cryptography;

namespace CheckoutQueue.Demo;

/// <summary>
/// A one-file control page at <c>/</c> for trying the queue by hand. It only calls this
/// instance's own API; open a second instance in another tab to see both serve one sale.
/// </summary>
public static class DemoPage
{
    private const string NoncePlaceholder = "__CSP_NONCE__";

    private static readonly Lazy<string> Html = new(() =>
    {
        const string resource = "CheckoutQueue.Demo.index.html";
        using var stream = typeof(DemoPage).Assembly.GetManifestResourceStream(resource)
            ?? throw new InvalidOperationException($"embedded page {resource} not found");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    });

    public static IEndpointRouteBuilder MapDemoPage(this IEndpointRouteBuilder app)
    {
        app.MapGet("/", (HttpContext context) =>
        {
            // Inline script and style run only with this request's nonce, so injected markup cannot run.
            var nonce = Convert.ToBase64String(RandomNumberGenerator.GetBytes(16));
            context.Response.Headers.ContentSecurityPolicy =
                $"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; connect-src 'self'; " +
                "base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
            context.Response.Headers.XContentTypeOptions = "nosniff";
            return Results.Content(Html.Value.Replace(NoncePlaceholder, nonce), "text/html; charset=utf-8");
        });
        return app;
    }
}
