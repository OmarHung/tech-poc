//! 同一個情境用 CP、AP 各跑一次，把每一步的結果印出來對照。
//!
//! 情境：3 台節點 A、B、C 都有 x=1。網路斷成 {A, B} | {C}，
//! client1 連 A 寫 x=2，接著 client2 連 C 寫 x=3，最後網路恢復。
//! 最後加上延伸：AP 不丟資料的兩種合併方式（merge 模組）。

use std::error::Error;

use cap_theorem::merge::{self, GCounter, VectorClock};
use cap_theorem::{CapError, Cluster, Mode};

fn write_and_show(cluster: Cluster, node: &str, value: &str, stage: &str) -> Cluster {
    let label = format!("[{stage}] {node} 寫 x={value}");
    match cluster.write(node, "x", value) {
        Ok(next) => {
            println!("  {label} → 成功");
            next
        }
        Err(error) => {
            println!("  {label} → 拒絕：{error}");
            cluster
        }
    }
}

fn show_read(cluster: &Cluster, node: &str, stage: &str) {
    let outcome = match cluster.read(node, "x") {
        Ok(value) => value.unwrap_or("（無）").to_owned(),
        Err(error) => format!("拒絕：{error}"),
    };
    println!("  [{stage}] {node} 讀 x → {outcome}");
}

fn run_three_nodes(mode: Mode) -> Result<(), CapError> {
    println!("\n=== {mode} 模式（3 台）===");
    let cluster = write_and_show(Cluster::new(mode, &["A", "B", "C"]), "A", "1", "正常");

    let cluster = cluster.partition(&[&["A", "B"], &["C"]])?;
    println!("  --- 網路分區：{{A, B}} | {{C}} ---");
    let cluster = write_and_show(cluster, "A", "2", "分區");
    let cluster = write_and_show(cluster, "C", "3", "分區");
    for node in ["A", "C"] {
        show_read(&cluster, node, "分區");
    }

    let (cluster, lost) = cluster.heal();
    println!("  --- 網路恢復 ---");
    for node in cluster.nodes() {
        show_read(&cluster, node, "恢復");
    }
    let lost_text = if lost.is_empty() {
        "無".to_owned()
    } else {
        lost.iter()
            .map(|version| format!("x={}（在 {} 寫入）", version.value, version.origin))
            .collect::<Vec<_>>()
            .join("、")
    };
    println!("  已確認卻被丟掉的寫入：{lost_text}");
    Ok(())
}

fn run_two_nodes() -> Result<(), CapError> {
    println!("\n=== 延伸：只有 2 台的 CP（對應雙機備援）===");
    let cluster = Cluster::new(Mode::Cp, &["A", "B"]).partition(&[&["A"], &["B"]])?;
    println!("  --- 網路分區：{{A}} | {{B}} ---");
    let cluster = write_and_show(cluster, "A", "1", "分區");
    write_and_show(cluster, "B", "1", "分區");
    println!("  兩邊都湊不到過半，整個服務停擺 → 需要第三方仲裁（witness）才能撐過分區");
    Ok(())
}

fn format_clock(clock: &VectorClock) -> String {
    let cells: Vec<String> = clock
        .iter()
        .map(|(node, count)| format!("{node}:{count}"))
        .collect();
    format!("{{{}}}", cells.join(", "))
}

fn run_version_vectors() {
    println!("  --- 方案一：version vector，並行寫入兩個都留下 ---");
    let x1 = merge::next_version(&[], "A", "1");
    println!(
        "  [正常] A 寫 x=1 → 時鐘 {}，複製到 B、C",
        format_clock(&x1.clock)
    );
    let x2 = merge::next_version(std::slice::from_ref(&x1), "A", "2");
    println!("  [分區] A 寫 x=2 → 時鐘 {}", format_clock(&x2.clock));
    let x3 = merge::next_version(std::slice::from_ref(&x1), "C", "3");
    println!("  [分區] C 寫 x=3 → 時鐘 {}", format_clock(&x3.clock));
    let pair = format!("{} 和 {}", format_clock(&x2.clock), format_clock(&x3.clock));
    println!(
        "  [恢復] 比較 {pair} → {}",
        merge::compare(&x2.clock, &x3.clock)
    );
    let siblings = merge::reconcile(&[x2.clone(), x2, x3]); // A、B、C 三台手上的版本
    let values: Vec<&str> = siblings.iter().map(|s| s.value.as_str()).collect();
    println!(
        "  [恢復] 讀 x → {}（兄弟版本都留著，交給應用層決定）",
        values.join("、")
    );
    let resolved = merge::next_version(&siblings, "A", "5");
    println!(
        "  [解決] 應用層合併成 x=5 寫回 A → 時鐘 {}，取代兄弟版本",
        format_clock(&resolved.clock)
    );
}

/// 把計數器的總數當一般值，用 Cluster 的 AP 模式跑一次：A 那側 +2、C 那側 +3。
fn last_write_wins_total() -> Result<Option<String>, CapError> {
    let cluster = Cluster::new(Mode::Ap, &["A", "B", "C"])
        .write("A", "x", "0")?
        .partition(&[&["A", "B"], &["C"]])?
        .write("A", "x", "2")?
        .write("C", "x", "3")?;
    let (cluster, _) = cluster.heal();
    Ok(cluster.read("A", "x")?.map(str::to_owned))
}

fn run_counter() -> Result<(), Box<dyn Error>> {
    println!("  --- 方案二：CRDT 計數器（G-Counter），自動合併 ---");
    println!("  [分區] A 那側 +2、C 那側 +3");
    let lww = last_write_wins_total()?;
    println!(
        "  [恢復] 把總數當一般值用 last-write-wins → {}（少了 A 的 +2）",
        lww.as_deref().unwrap_or("（無）")
    );
    let on_a = merge::add(&GCounter::new(), "A", 2)?;
    let on_c = merge::add(&GCounter::new(), "C", 3)?;
    let merged = merge::merge_counters(&[&on_a, &on_c]);
    println!(
        "  [恢復] G-Counter 每格取最大再相加 → {}，總數 {}",
        format_clock(&merged),
        merge::total(&merged)
    );
    Ok(())
}

fn run_lossless_merges() -> Result<(), Box<dyn Error>> {
    println!("\n=== 延伸：AP 不丟資料的兩種合併方式 ===");
    run_version_vectors();
    run_counter()
}

fn main() -> Result<(), Box<dyn Error>> {
    run_three_nodes(Mode::Cp)?;
    run_three_nodes(Mode::Ap)?;
    run_two_nodes()?;
    run_lossless_merges()
}
