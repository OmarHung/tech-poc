namespace CleanCode;

/// <summary>舊版 Calc() 的一組輸入，順序和型別都照舊版的參數（地區、會員等級是字串）。</summary>
public readonly record struct Case(int Weight, string Region, string Membership, int Amount, bool Cold);
