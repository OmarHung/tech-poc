//! 同一個情境用 CP、AP 各跑一次，把每一步的結果印出來對照。
//!
//! 情境：3 台節點 A、B、C 都有 x=1。網路斷成 {A, B} | {C}，
//! client1 連 A 寫 x=2，接著 client2 連 C 寫 x=3，最後網路恢復。

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

fn main() -> Result<(), CapError> {
    run_three_nodes(Mode::Cp)?;
    run_three_nodes(Mode::Ap)?;
    run_two_nodes()
}
