// 同一個情境用 CP、AP 各跑一次，把每一步的結果印出來對照。
//
// 情境：3 台節點 A、B、C 都有 x=1。網路斷成 {A, B} | {C}，
// client1 連 A 寫 x=2，接著 client2 連 C 寫 x=3，最後網路恢復。
// 最後示範 AP 不丟資料的兩種合併方式：version vector 和 CRDT 計數器。

using CapTheorem;

RunThreeNodes(Mode.CP);
RunThreeNodes(Mode.AP);
RunTwoNodes();
RunLosslessMerges();

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

static string FormatClock(IReadOnlyDictionary<string, int> clock) =>
    "{" + string.Join(", ", clock.OrderBy(entry => entry.Key, StringComparer.Ordinal).Select(entry => $"{entry.Key}:{entry.Value}")) + "}";

static void RunVersionVectors()
{
    Console.WriteLine("  --- 方案一：version vector，並行寫入兩個都留下 ---");
    var x1 = VersionVector.NextVersion([], "A", "1");
    Console.WriteLine($"  [正常] A 寫 x=1 → 時鐘 {FormatClock(x1.Clock)}，複製到 B、C");
    var x2 = VersionVector.NextVersion([x1], "A", "2");
    Console.WriteLine($"  [分區] A 寫 x=2 → 時鐘 {FormatClock(x2.Clock)}");
    var x3 = VersionVector.NextVersion([x1], "C", "3");
    Console.WriteLine($"  [分區] C 寫 x=3 → 時鐘 {FormatClock(x3.Clock)}");
    var order = VersionVector.Compare(x2.Clock, x3.Clock).ToText();
    Console.WriteLine($"  [恢復] 比較 {FormatClock(x2.Clock)} 和 {FormatClock(x3.Clock)} → {order}");
    var siblings = VersionVector.Reconcile([x2, x2, x3]); // A、B、C 三台手上的版本
    Console.WriteLine($"  [恢復] 讀 x → {string.Join("、", siblings.Select(sibling => sibling.Value))}（兄弟版本都留著，交給應用層決定）");
    var resolved = VersionVector.NextVersion(siblings, "A", "5");
    Console.WriteLine($"  [解決] 應用層合併成 x=5 寫回 A → 時鐘 {FormatClock(resolved.Clock)}，取代兄弟版本");
}

// 把計數器的總數當一般值，用 Cluster 的 AP 模式跑一次：A 那側 +2、C 那側 +3。
static string? LastWriteWinsTotal()
{
    var (cluster, _) = Cluster.Create(Mode.AP).Write("A", "x", "0");
    cluster = cluster.Partition(["A", "B"], ["C"]);
    (cluster, _) = cluster.Write("A", "x", "2");
    (cluster, _) = cluster.Write("C", "x", "3");
    (cluster, _) = cluster.Heal();
    return cluster.Read("A", "x").Value;
}

static void RunCounter()
{
    Console.WriteLine("  --- 方案二：CRDT 計數器（G-Counter），自動合併 ---");
    Console.WriteLine("  [分區] A 那側 +2、C 那側 +3");
    Console.WriteLine($"  [恢復] 把總數當一般值用 last-write-wins → {LastWriteWinsTotal()}（少了 A 的 +2）");
    var merged = GCounter.Merge(GCounter.Add(GCounter.Empty, "A", 2), GCounter.Add(GCounter.Empty, "C", 3));
    Console.WriteLine($"  [恢復] G-Counter 每格取最大再相加 → {FormatClock(merged)}，總數 {GCounter.Total(merged)}");
}

static void RunLosslessMerges()
{
    Console.WriteLine("\n=== 延伸：AP 不丟資料的兩種合併方式 ===");
    RunVersionVectors();
    RunCounter();
}
