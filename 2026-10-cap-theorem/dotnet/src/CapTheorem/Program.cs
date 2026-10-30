// 同一個情境用 CP、AP 各跑一次，把每一步的結果印出來對照。
//
// 情境：3 台節點 A、B、C 都有 x=1。網路斷成 {A, B} | {C}，
// client1 連 A 寫 x=2，接著 client2 連 C 寫 x=3，最後網路恢復。

using CapTheorem;

RunThreeNodes(Mode.CP);
RunThreeNodes(Mode.AP);
RunTwoNodes();

static Cluster WriteAndShow(Cluster cluster, string node, string value, string stage)
{
    var (next, result) = cluster.Write(node, "x", value);
    Console.WriteLine($"  [{stage}] {node} 寫 x={value} → {(result.Ok ? "成功" : "拒絕：" + result.Reason)}");
    return next;
}

static void ShowRead(Cluster cluster, string node, string stage)
{
    var result = cluster.Read(node, "x");
    Console.WriteLine($"  [{stage}] {node} 讀 x → {(result.Ok ? result.Value ?? "（無）" : "拒絕：" + result.Reason)}");
}

static void RunThreeNodes(Mode mode)
{
    Console.WriteLine($"\n=== {mode} 模式（3 台）===");
    var cluster = WriteAndShow(Cluster.Create(mode), "A", "1", "正常");

    cluster = cluster.Partition(["A", "B"], ["C"]);
    Console.WriteLine("  --- 網路分區：{A, B} | {C} ---");
    cluster = WriteAndShow(cluster, "A", "2", "分區");
    cluster = WriteAndShow(cluster, "C", "3", "分區");
    foreach (var node in new[] { "A", "C" })
    {
        ShowRead(cluster, node, "分區");
    }

    var (healed, lost) = cluster.Heal();
    Console.WriteLine("  --- 網路恢復 ---");
    foreach (var node in healed.Nodes)
    {
        ShowRead(healed, node, "恢復");
    }
    var lostText = lost.IsEmpty ? "無" : string.Join("、", lost.Select(v => $"x={v.Value}（在 {v.Origin} 寫入）"));
    Console.WriteLine($"  已確認卻被丟掉的寫入：{lostText}");
}

static void RunTwoNodes()
{
    Console.WriteLine("\n=== 延伸：只有 2 台的 CP（對應雙機備援）===");
    var cluster = Cluster.Create(Mode.CP, "A", "B").Partition(["A"], ["B"]);
    Console.WriteLine("  --- 網路分區：{A} | {B} ---");
    cluster = WriteAndShow(cluster, "A", "1", "分區");
    WriteAndShow(cluster, "B", "1", "分區");
    Console.WriteLine("  兩邊都湊不到過半，整個服務停擺 → 需要第三方仲裁（witness）才能撐過分區");
}
