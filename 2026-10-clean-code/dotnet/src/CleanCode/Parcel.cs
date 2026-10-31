namespace CleanCode;

public sealed record Parcel(
    int WeightGrams,
    Region Region,
    Membership Membership,
    int OrderAmount,
    bool NeedsColdChain);
