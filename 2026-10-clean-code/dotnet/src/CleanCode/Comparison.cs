using System.Collections.Immutable;

namespace CleanCode;

/// <param name="Invalid">兩版都判定為無效輸入的組數</param>
public sealed record Comparison(int Total, int Invalid, ImmutableArray<Case> Mismatches);
