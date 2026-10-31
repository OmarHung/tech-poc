namespace CheckoutQueue.Redis;

public sealed record SaleScript(string Name, string Source);

/// <summary>The Lua scripts embedded in this assembly, each with common.lua in front of it.</summary>
public static class SaleScripts
{
    public static readonly SaleScript Init = Load("init");
    public static readonly SaleScript Join = Load("join");
    public static readonly SaleScript Ticket = Load("ticket");
    public static readonly SaleScript Admit = Load("admit");
    public static readonly SaleScript Checkout = Load("checkout");
    public static readonly SaleScript Order = Load("order");
    public static readonly SaleScript Pay = Load("pay");
    public static readonly SaleScript ExpireOrders = Load("expire_orders");
    public static readonly SaleScript Audit = Load("audit");
    public static readonly SaleScript Tickets = Load("tickets");
    public static readonly SaleScript Watch = Load("watch");

    private static SaleScript Load(string name) => new(name, Read("common") + "\n" + Read(name));

    private static string Read(string name)
    {
        var resource = $"CheckoutQueue.Scripts.{name}.lua";
        using var stream = typeof(SaleScripts).Assembly.GetManifestResourceStream(resource)
            ?? throw new InvalidOperationException($"embedded script {resource} not found");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
