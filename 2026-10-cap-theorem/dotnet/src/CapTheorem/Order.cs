namespace CapTheorem;

/// <summary>兩個 version vector 的先後關係。</summary>
public enum Order
{
    /// <summary>a 發生在 b 之前：b 已經看過 a</summary>
    Before,

    /// <summary>b 發生在 a 之前</summary>
    After,

    Equal,

    /// <summary>互相沒看過對方，誰都不該蓋掉誰</summary>
    Concurrent,
}

public static class OrderText
{
    public static string ToText(this Order order) => order switch
    {
        Order.Before => "之前",
        Order.After => "之後",
        Order.Equal => "相同",
        Order.Concurrent => "並行",
        _ => throw new ArgumentOutOfRangeException(nameof(order), order, null),
    };
}
