using System.Collections.Frozen;

namespace CleanCode;

/// <summary>
/// 舊版 Calc() 用字串代表地區和會員等級，重構版改用 enum，這裡是兩邊的對照表。
/// Python 版直接把舊字串當 enum 的值（Region("main")）；C# 的 enum 只能是整數，所以另外放。
/// </summary>
public static class LegacyCodes
{
    private static readonly FrozenDictionary<string, Region> Regions =
        new Dictionary<string, Region>
        {
            ["main"] = Region.MainIsland,
            ["outlying"] = Region.OutlyingIsland,
        }.ToFrozenDictionary();

    private static readonly FrozenDictionary<string, Membership> Memberships =
        new Dictionary<string, Membership>
        {
            ["normal"] = Membership.Normal,
            ["gold"] = Membership.Gold,
        }.ToFrozenDictionary();

    // 不認得的字串會丟 KeyNotFoundException，和 Python 的 Region("x") 丟 ValueError 一樣不會默默放行
    public static Region ToRegion(string code) => Regions[code];

    public static Membership ToMembership(string code) => Memberships[code];

    public static string ToCode(Region region) => Regions.Single(pair => pair.Value == region).Key;

    public static string ToCode(Membership membership) => Memberships.Single(pair => pair.Value == membership).Key;
}
