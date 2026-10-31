using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace CheckoutQueue.Tests;

[Collection(RedisCollection.Name)]
public sealed partial class DemoPageTests(RedisFixture redis)
{
    [Fact]
    public async Task DemoPage_IsServedAtRoot_WithAFreshScriptNonceEachTime()
    {
        await using var app = await TestApp.StartAsync(redis.Connection);

        var first = await app.Client.GetAsync("/");
        var second = await app.Client.GetAsync("/");

        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal("text/html", first.Content.Headers.ContentType!.MediaType);
        Assert.Equal("nosniff", first.Headers.GetValues("X-Content-Type-Options").Single());
        var csp = first.Headers.GetValues("Content-Security-Policy").Single();
        var nonce = NonceInPolicy().Match(csp).Groups[1].Value;
        Assert.NotEmpty(nonce);
        Assert.Contains("connect-src 'self'", csp);
        Assert.DoesNotContain("unsafe-inline", csp);
        var html = await first.Content.ReadAsStringAsync();
        Assert.Contains($"<script nonce=\"{nonce}\">", html);
        Assert.Contains($"<style nonce=\"{nonce}\">", html);
        Assert.DoesNotContain("__CSP_NONCE__", html);
        Assert.NotEqual(csp, second.Headers.GetValues("Content-Security-Policy").Single());
    }

    [Fact]
    public async Task AdminConfig_ShowsTheSaleSettings_ButNotWhereRedisIs()
    {
        await using var app = await TestApp.StartAsync(redis.Connection, c => c with { MaxActiveSessions = 7 });

        var response = await app.Client.GetAsync("/admin/config");
        var raw = await response.Content.ReadAsStringAsync();
        var body = JsonDocument.Parse(raw).RootElement;

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("test-instance", body.GetProperty("instance").GetString());
        Assert.StartsWith("t", body.GetProperty("saleId").GetString());
        Assert.Equal(7, body.GetProperty("maxActive").GetInt32());
        Assert.Equal(2, body.GetProperty("maxPerOrder").GetInt32());
        Assert.Equal(60_000, body.GetProperty("admissionWindowMs").GetInt32());
        Assert.Equal(120_000, body.GetProperty("paymentWindowMs").GetInt32());
        Assert.Equal(15_000, body.GetProperty("heartbeatTimeoutMs").GetInt32());
        Assert.Equal(1_000, body.GetProperty("pollIntervalMs").GetInt32());
        Assert.Equal(200, body.GetProperty("tickMs").GetInt32());
        Assert.DoesNotContain("redis", raw, StringComparison.OrdinalIgnoreCase);
    }

    [GeneratedRegex("script-src 'nonce-([A-Za-z0-9+/=]+)'")]
    private static partial Regex NonceInPolicy();
}
