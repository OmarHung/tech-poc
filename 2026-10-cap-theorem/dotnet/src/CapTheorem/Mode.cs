namespace CapTheorem;

public enum Mode
{
    /// <summary>少數派那側拒絕服務，換取一致性</summary>
    CP,

    /// <summary>每個節點都照常服務，換取可用性</summary>
    AP,
}
