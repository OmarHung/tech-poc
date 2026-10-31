#!/usr/bin/env python3
"""用真實的 etcd（CP）和 Cassandra（AP）重跑 demo 的同一個情境。

3 台節點都有 x=1 之後，把第 3 台的網路拔掉（docker network disconnect），
兩側各寫一次、各讀一次，再用同一個 IP 接回網路，看最後每台讀到什麼。
所有操作都透過 docker exec 在容器裡執行 etcdctl、cqlsh、nodetool；只用標準函式庫。
叢集由 run.sh 用 docker compose 啟動和關閉。

etcd 可以指定被隔離的 etcd3 在分區前是 leader 還是 follower，兩種情況的結果不一樣：
follower 轉給 leader 的寫入，可能在網路恢復後才送達並生效。

Usage:
    scenario.py etcd [follower|leader]     # 預設 follower
    scenario.py cassandra
"""

import json
import re
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"
POLL_INTERVAL_S = 2


@dataclass(frozen=True)
class Outcome:
    ok: bool
    output: str


@dataclass(frozen=True)
class Step:
    stage: str  # 正常／分區／恢復
    label: str  # 例如「etcd3 寫 x=3」
    ok: bool
    shown: str  # 印出來的結果：讀到的值、「成功」、拒絕或逾時的原因
    written: str | None  # 寫入的值；讀取時為 None
    raw: str  # 原始輸出，存進 results 當證據
    timed_out: bool = False  # 逾時：沒有回覆成功，但也不保證沒生效


@dataclass(frozen=True)
class Report:
    name: str  # results 檔名
    steps: list[Step]
    history: tuple[str, ...] | None = None  # 恢復後 x 依序出現過的值；系統沒有修改紀錄時為 None
    notes: dict[str, str] = field(default_factory=dict)


# --- 共用 ---


def run(command: list[str], timeout_s: int = 60) -> Outcome:
    try:
        done = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout_s, check=False
        )
    except subprocess.TimeoutExpired:
        return Outcome(ok=False, output=f"指令超過 {timeout_s} 秒沒有回應")
    return Outcome(ok=done.returncode == 0, output=(done.stdout + done.stderr).strip())


def must(outcome: Outcome, what: str) -> Outcome:
    if not outcome.ok:
        raise RuntimeError(f"{what}失敗：{outcome.output}")
    return outcome


def wait_until(check: Callable[[], bool], what: str, timeout_s: int) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(POLL_INTERVAL_S)
    raise TimeoutError(f"等了 {timeout_s} 秒，還沒有{what}")


def disconnect(network: str, container: str) -> None:
    must(run(["docker", "network", "disconnect", network, container]), "拔掉網路")


def reconnect(network: str, container: str, ip: str) -> None:
    command = ["docker", "network", "connect", "--ip", ip, network, container]
    must(run(command), "接回網路")


def failure(message: str, timed_out: bool, writing: bool) -> str:
    """寫入逾時不代表沒生效，所以標成「結果未知」；讀取逾時只是沒讀到。"""
    if not timed_out:
        return f"拒絕：{message}"
    return f"逾時，結果未知：{message}" if writing else f"逾時：{message}"


def show(step: Step) -> Step:
    print(f"  [{step.stage}] {step.label} → {step.shown}")
    return step


def final_value(steps: list[Step]) -> str:
    final = {step.shown for step in steps if step.stage == "恢復" and step.ok}
    if len(final) != 1:
        raise RuntimeError(f"恢復後各節點讀到的值不一致：{sorted(final)}")
    return final.pop()


def lost_writes(report: Report) -> list[Step]:
    """分區期間回覆成功，恢復後卻找不到的寫入。

    有修改紀錄（etcd）時，出現在紀錄裡的寫入只是被之後的寫入依序蓋過，不算遺失。
    """
    kept = {final_value(report.steps), *(report.history or ())}
    return [
        s for s in report.steps if s.stage == "分區" and s.ok and s.written not in (None, *kept)
    ]


def late_writes(report: Report) -> list[Step]:
    """分區期間逾時，恢復後卻出現在修改紀錄裡的寫入。"""
    applied = set(report.history or ())
    return [s for s in report.steps if s.timed_out and s.written in applied]


def describe(steps: list[Step]) -> str:
    return "、".join(f"x={s.written}（在 {s.label.split()[0]} 寫入）" for s in steps) or "無"


def save(report: Report, image: str) -> None:
    lost, late = lost_writes(report), late_writes(report)
    if report.history is not None:
        print(f"  x 的修改紀錄（依 revision）：{' → '.join(report.history)}")
    print(f"  已確認卻被丟掉的寫入：{describe(lost)}")
    if report.history is not None or any(s.timed_out for s in report.steps):
        print(f"  逾時、恢復後卻生效的寫入：{describe(late)}")
    RESULTS_DIR.mkdir(exist_ok=True)
    result = {
        "system": report.name,
        "image": image,
        "ran_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "notes": report.notes,
        "history": report.history,
        "lost": [s.label for s in lost],
        "timed_out_but_applied": [s.label for s in late],
        "steps": [asdict(step) for step in report.steps],
    }
    path = RESULTS_DIR / f"{report.name}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(f"  結果存在 {path.relative_to(RESULTS_DIR.parent.parent)}")


# --- etcd ---

ETCD_IMAGE = "quay.io/coreos/etcd:v3.7.3"
ETCD_NETWORK = "cap-etcd_cap"
ETCD_IPS = {"etcd1": "172.30.10.11", "etcd2": "172.30.10.12", "etcd3": "172.30.10.13"}
ETCD_TIMEOUTS = re.compile(r"deadline exceeded|timed out", re.I)


def etcdctl(node: str, *args: str) -> Outcome:
    return run(
        [
            "docker", "exec", f"cap-{node}", "/usr/local/bin/etcdctl",
            "--endpoints=http://127.0.0.1:2379", "--command-timeout=3s", *args,
        ]
    )  # fmt: skip


def etcd_error(output: str) -> str:
    match = re.search(r"^Error: (.+)$", output, re.MULTILINE)
    return match.group(1) if match else output.splitlines()[-1][:120]


def etcd_status(node: str) -> dict:
    outcome = etcdctl(node, "endpoint", "status", "-w", "json")
    return json.loads(outcome.output)[0]["Status"] if outcome.ok else {}


def etcd_members() -> dict[str, int]:
    """成員名稱 -> member ID。"""
    outcome = must(etcdctl("etcd1", "member", "list", "-w", "json"), "列出成員")
    return {member["name"]: member["ID"] for member in json.loads(outcome.output)["members"]}


def etcd_place_leader(isolate: str, members: dict[str, int]) -> str:
    """讓 etcd3 在分區前是 leader（isolate=leader）或 follower，回傳 leader 的名稱。"""
    names = {member_id: name for name, member_id in members.items()}
    leader = etcd_status("etcd1").get("leader")
    target = "etcd3" if isolate == "leader" else "etcd1"
    if (leader == members["etcd3"]) != (isolate == "leader"):
        command = ["move-leader", format(members[target], "x")]
        must(etcdctl(names[leader], *command), f"把 leader 移到 {target}")
        wait_until(
            lambda: etcd_status("etcd1").get("leader") == members[target],
            f"讓 {target} 成為 leader",
            timeout_s=30,
        )
    return names[etcd_status("etcd1")["leader"]]


def etcd_history() -> tuple[str, ...]:
    """x 從建立到現在每個 revision 的值。叢集裡只有 x 這個 key，所以 revision 是連續的。"""
    outcome = must(etcdctl("etcd1", "get", "x", "-w", "json"), "讀 x 的 revision")
    kv = json.loads(outcome.output)["kvs"][0]
    revisions = range(kv["create_revision"], kv["mod_revision"] + 1)
    return tuple(
        must(etcdctl("etcd1", "get", "x", f"--rev={rev}", "--print-value-only"), "讀舊版本").output
        for rev in revisions
    )


def etcd_put(stage: str, node: str, value: str) -> Step:
    outcome = etcdctl(node, "put", "x", value)
    timed_out = not outcome.ok and ETCD_TIMEOUTS.search(outcome.output) is not None
    shown = "成功" if outcome.ok else failure(etcd_error(outcome.output), timed_out, True)
    label = f"{node} 寫 x={value}"
    return show(Step(stage, label, outcome.ok, shown, value, outcome.output, timed_out))


def etcd_get(stage: str, node: str, serializable: bool = False) -> Step:
    extra = ["--consistency=s"] if serializable else []
    outcome = etcdctl(node, "get", "x", "--print-value-only", *extra)
    timed_out = not outcome.ok and ETCD_TIMEOUTS.search(outcome.output) is not None
    shown = outcome.output if outcome.ok else failure(etcd_error(outcome.output), timed_out, False)
    label = f"{node} 讀 x" + ("（serializable，允許舊資料）" if serializable else "")
    return show(Step(stage, label, outcome.ok, shown, None, outcome.output, timed_out))


def scenario_etcd(isolate: str) -> Report:
    print(f"=== etcd 3.7.3（CP：Raft 多數決）・被隔離的 etcd3 是 {isolate} ===")
    steps = [etcd_put("正常", "etcd1", "1"), etcd_get("正常", "etcd3")]

    members = etcd_members()
    leader = etcd_place_leader(isolate, members)
    print(f"  （分區前的 leader：{leader}）")
    disconnect(ETCD_NETWORK, "cap-etcd3")
    print("  --- 網路分區：{etcd1, etcd2} | {etcd3} ---")
    wait_until(
        lambda: etcd_status("etcd1").get("leader") not in (None, 0, members["etcd3"]),
        "在 etcd1、etcd2 這側選出 leader",
        timeout_s=30,
    )
    steps += [
        etcd_put("分區", "etcd1", "2"),
        etcd_put("分區", "etcd3", "3"),
        etcd_get("分區", "etcd1"),
        etcd_get("分區", "etcd3"),
        etcd_get("分區", "etcd3", serializable=True),
    ]

    reconnect(ETCD_NETWORK, "cap-etcd3", ETCD_IPS["etcd3"])
    print("  --- 網路恢復 ---")
    wait_until(lambda: etcdctl("etcd3", "endpoint", "health").ok, "讓 etcd3 回到叢集", 60)
    steps += [etcd_get("恢復", node) for node in ETCD_IPS]
    notes = {"isolated_role": isolate, "leader_before_partition": leader}
    return Report(f"etcd-{isolate}", steps, etcd_history(), notes)


# --- Cassandra ---

CASSANDRA_IMAGE = "cassandra:5.0.9"
CASSANDRA_NETWORK = "cap-cassandra_cap"
CASSANDRA_IPS = {"cass1": "172.30.20.11", "cass2": "172.30.20.12", "cass3": "172.30.20.13"}
CASSANDRA_ERRORS = re.compile(r"Unavailable|NoHostAvailable|timed out|Timeout|Error", re.I)
# Unavailable（湊不到足夠的活節點）是在寫之前就拒絕；逾時則可能已經寫進部分複本
CASSANDRA_TIMEOUTS = re.compile(r"timed out|Timeout", re.I)


def cqlsh(node: str, statement: str, consistency: str) -> Outcome:
    script = f"CONSISTENCY {consistency}; {statement}"
    outcome = run(["docker", "exec", f"cap-{node}", "cqlsh", "--no-color", "-e", script])
    failed = not outcome.ok or CASSANDRA_ERRORS.search(outcome.output) is not None
    return Outcome(ok=not failed, output=outcome.output)


def cassandra_error(output: str) -> str:
    match = re.search(r'message="([^"]+)"', output)
    return match.group(1) if match else output.splitlines()[-1][:120]


def cassandra_timed_out(outcome: Outcome) -> bool:
    if outcome.ok or "Cannot achieve consistency level" in outcome.output:
        return False
    return CASSANDRA_TIMEOUTS.search(outcome.output) is not None


def selected_value(output: str) -> str:
    """從 cqlsh 的表格輸出取出唯一一格的值；沒有資料時回「（無）」。"""
    lines = [line.strip() for line in output.splitlines()]
    divider = next((i for i, line in enumerate(lines) if re.fullmatch(r"-+", line)), None)
    if divider is None or divider + 1 >= len(lines) or lines[divider + 1].startswith("("):
        return "（無）"
    return lines[divider + 1]


def cassandra_view(node: str) -> dict[str, str]:
    """這台節點眼中每台的狀態：IP -> U（活著）或 D（掛了）。"""
    outcome = run(["docker", "exec", f"cap-{node}", "nodetool", "status"])
    pattern = re.compile(r"^([UD])[NLJM]\s+(\S+)", re.MULTILINE)
    return {ip: state for state, ip in pattern.findall(outcome.output)} if outcome.ok else {}


def no_pending_hints(node: str) -> bool:
    outcome = run(["docker", "exec", f"cap-{node}", "nodetool", "listpendinghints"])
    return outcome.ok and "does not have any pending hints" in outcome.output


def cassandra_put(stage: str, node: str, value: str, consistency: str) -> Step:
    statement = f"INSERT INTO cap.kv (k, v) VALUES ('x', '{value}');"
    outcome = cqlsh(node, statement, consistency)
    timed_out = cassandra_timed_out(outcome)
    shown = "成功" if outcome.ok else failure(cassandra_error(outcome.output), timed_out, True)
    label = f"{node} 寫 x={value}（{consistency}）"
    return show(Step(stage, label, outcome.ok, shown, value, outcome.output, timed_out))


def cassandra_get(stage: str, node: str, consistency: str) -> Step:
    outcome = cqlsh(node, "SELECT v FROM cap.kv WHERE k = 'x';", consistency)
    timed_out = cassandra_timed_out(outcome)
    value = selected_value(outcome.output)
    shown = value if outcome.ok else failure(cassandra_error(outcome.output), timed_out, False)
    label = f"{node} 讀 x（{consistency}）"
    return show(Step(stage, label, outcome.ok, shown, None, outcome.output, timed_out))


def scenario_cassandra() -> Report:
    print("=== Cassandra 5.0.9（AP：可調一致性，每筆資料 3 份複本）===")
    schema = (
        "CREATE KEYSPACE IF NOT EXISTS cap WITH replication = "
        "{'class': 'SimpleStrategy', 'replication_factor': 3}; "
        "CREATE TABLE IF NOT EXISTS cap.kv (k text PRIMARY KEY, v text);"
    )
    must(cqlsh("cass1", schema, "ALL"), "建立 keyspace 和資料表")
    steps = [cassandra_put("正常", "cass1", "1", "ALL"), cassandra_get("正常", "cass3", "ONE")]

    disconnect(CASSANDRA_NETWORK, "cap-cass3")
    print("  --- 網路分區：{cass1, cass2} | {cass3} ---")
    isolated, others = CASSANDRA_IPS["cass3"], [CASSANDRA_IPS["cass1"], CASSANDRA_IPS["cass2"]]
    wait_until(
        lambda: (
            cassandra_view("cass1").get(isolated) == "D"
            and all(cassandra_view("cass3").get(ip) == "D" for ip in others)
        ),
        "讓兩側都判定對方離線",
        timeout_s=120,
    )
    steps += [
        cassandra_put("分區", "cass1", "2", "ONE"),
        cassandra_put("分區", "cass3", "3", "QUORUM"),
        cassandra_put("分區", "cass3", "3", "ONE"),
        cassandra_get("分區", "cass1", "ONE"),
        cassandra_get("分區", "cass3", "ONE"),
        cassandra_get("分區", "cass3", "QUORUM"),
    ]

    reconnect(CASSANDRA_NETWORK, "cap-cass3", isolated)
    print("  --- 網路恢復 ---")
    wait_until(
        lambda: all(
            set(cassandra_view(node).values()) == {"U"} and len(cassandra_view(node)) == 3
            for node in CASSANDRA_IPS
        ),
        "讓三台都看到彼此上線",
        timeout_s=180,
    )
    wait_until(
        lambda: all(no_pending_hints(node) for node in CASSANDRA_IPS),
        "把分區期間暫存的 hint 送完",
        timeout_s=120,
    )
    steps += [cassandra_get("恢復", node, "ONE") for node in CASSANDRA_IPS]
    return Report("cassandra", steps)


def main(argv: list[str]) -> int:
    usage = f"usage: {argv[0]} etcd [follower|leader] | cassandra"
    match argv[1:]:
        case ["etcd"]:
            save(scenario_etcd("follower"), ETCD_IMAGE)
        case ["etcd", ("follower" | "leader") as isolate]:
            save(scenario_etcd(isolate), ETCD_IMAGE)
        case ["cassandra"]:
            save(scenario_cassandra(), CASSANDRA_IMAGE)
        case _:
            print(usage, file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
