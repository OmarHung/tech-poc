# CAP 定理範例

- 狀態：已完成
- 開始日期：2026-10-30
- 結論：有條件可行：選 CP 要至少 3 票，而且寫入逾時要當成「結果未知」；選 AP 要不丟資料，就不能用 last-write-wins，要改用 version vector 或 CRDT

## 背景與目標

雙機備援和微服務拆分都會碰到同一個問題：機器之間的網路斷掉（網路分區）時，系統要選「資料一致」還是「繼續服務」。CAP 定理說，發生網路分區時，一致性（Consistency）和可用性（Availability）只能二選一。

這個 PoC 先用一個最小的模擬，把 CP 和 AP 兩種設計在分區時的行為實際跑出來，再用真實的 etcd（CP）和 Cassandra（AP）重跑同一個情境對照。之後做[雙機備援](../2026-10-dual-node-failover/)（要不要加仲裁）、[微服務拆分](../2026-10-modular-monolith-to-microservices/)（能不能接受最終一致）時，可以拿這個結果當討論基礎。

## 要驗證的假設

網路分區時，CP 設計會讓少數派那側停止服務，但不會丟掉已確認的寫入；AP 設計兩側都能繼續服務，但兩側會讀到不同的資料，分區恢復後用 last-write-wins 合併，會默默丟掉已確認的寫入。

延伸驗證兩件事：AP 改用 version vector 或 CRDT 合併，就不會默默丟資料；換成真實的 etcd 和 Cassandra，行為跟模擬一致。

## 待釐清問題

- [x] CP 少數派那側要拒絕寫入、讀取，還是兩者都拒絕？→ 這個範例兩者都拒絕，避免讀到舊資料
- [x] 只有 2 台機器能做 CP 嗎？→ 不行。分區時兩邊都湊不到過半，整個服務停擺
- [x] AP 有沒有不丟資料的合併方式？→ 有兩種。一是 version vector：偵測出並行寫入，兩個都留下來交給應用層決定；二是把資料設計成 CRDT（例如 G-Counter），合併時自動算出正確結果。前者衝突還在，只是不會默默消失；後者只適用於能設計成 CRDT 的資料型別
- [x] 換成真實系統（CP 用 etcd，AP 用 Cassandra），結果跟模擬一不一致？→ 一致：etcd 沒有丟掉任何已確認的寫入；Cassandra 用 ONE 一致性時兩側都能寫，恢復後 last-write-wins 丟掉一筆。另外發現兩件模擬沒涵蓋的事，見實驗紀錄
- [x] （實驗中發現）CP 系統裡，寫入逾時算失敗嗎？→ 不算，結果是未知的。etcd 被隔離的節點是 follower 時，逾時的寫入 5 次都在網路恢復後生效

## 成功標準

| 指標 | 目標值 | 實測值 |
|------|--------|--------|
| CP：分區時少數派（C）的寫入與讀取被拒絕 | 是 | 是 |
| CP：分區時多數派（A、B）照常服務 | 是 | 是 |
| CP：恢復後遺失的已確認寫入 | 0 筆 | 0 筆 |
| AP：分區時兩側寫入都成功 | 是 | 是 |
| AP：分區時兩側讀到不同的值 | 是 | 是（A 讀到 2，C 讀到 3） |
| AP：恢復後遺失的已確認寫入（LWW 合併） | 至少 1 筆（證明代價真的存在） | 1 筆（A 寫入的 x=2） |
| 2 台 CP：分區時任一側還能服務 | 否 | 否 |
| AP 改用 version vector：恢復後保留兩個並行寫入 | 是 | 是（讀到 2、3 兩個兄弟版本） |
| AP 改用 G-Counter：兩側各 +2、+3，恢復後的總數 | 5 | 5（用 last-write-wins 是 3） |
| etcd：被隔離的節點讀寫成功 | 否 | 否（逾時） |
| etcd：恢復後遺失的已確認寫入 | 0 筆 | 0 筆（跑了 10 次都是） |
| Cassandra（ONE）：分區時兩側寫入都成功 | 是 | 是 |
| Cassandra（ONE）：恢復後遺失的已確認寫入 | 至少 1 筆 | 1 筆（cass1 寫入的 x=2） |
| Cassandra（QUORUM）：少數派那側讀寫成功 | 否 | 否（`Cannot achieve consistency level QUORUM`） |

## 範圍外

- 跨機房、真實硬體的網路：真實系統對照是用 Docker 在同一台電腦上跑，分區用 `docker network disconnect` 模擬
- 長時間、隨機注入故障的測試（例如 Jepsen）：這次每個情境只重複 1～5 次
- 共識演算法（Raft、Paxos）的選主與日誌複製細節
- PACELC 的延遲取捨（沒有分區時，一致性和延遲之間的取捨）
- 時鐘誤差：模擬用全域邏輯時鐘；Docker 的容器共用主機的時鐘，所以 Cassandra 也沒有時鐘誤差。真實環境各台機器的時間不準時，last-write-wins「誰被丟掉」會更難預測
- G-Counter 以外的 CRDT（PN-Counter、OR-Set 等）

## 技術選型

| 項目 | 選擇 | 理由 |
|------|------|------|
| 語言 | Python、.NET、Rust 各寫一份 | 同一個模型用三種語言實作，輸出逐字相同，方便對照 |
| 外部套件 | 範例本身都不用（只有測試工具需要） | 不用裝東西就能跑，重點放在模型本身 |
| 網路分區 | 在程序內用「節點分組」模擬 | 每次結果都一樣，不會受真實網路抖動影響 |
| CP 的判斷 | 多數派（quorum）：連得到過半節點才能讀寫 | etcd、ZooKeeper 這類 CP 系統的做法 |
| AP 的合併 | last-write-wins（LWW） | 最常見也最簡單，剛好看得出丟資料的代價 |
| AP 不丟資料的合併 | version vector（保留兄弟版本）、CRDT（G-Counter） | 分別代表「把衝突交給應用層」和「資料型別本身就能自動合併」兩條路 |
| 真實系統 | etcd 3.7.3（CP）、Cassandra 5.0.9（AP），各 3 台 | 各自是 CP、AP 最常被拿來舉例的系統；Cassandra 還能每筆請求選一致性等級 |
| 真實系統的環境 | Docker Compose，固定 IP；分區用 `docker network disconnect`，恢復用 `docker network connect --ip` 接回原本的 IP | 一台電腦就能跑；兩個系統都靠 IP 認節點，所以要接回同一個 IP |
| 驗證腳本 | Python 標準函式庫，透過 `docker exec` 在容器裡執行 `etcdctl`、`cqlsh`、`nodetool` | 不用裝任何 client 套件，也不用對外開 port |
| 測試 | Python：pytest + pytest-cov；.NET：xUnit + coverlet；Rust：`cargo test` | 依各語言的測試規範，跟其他 PoC 一致 |

## 做法

三種語言用同一個模型：3 台節點 A、B、C 都存在記憶體裡，「網路分區」用節點分組表示，同組可以互相連線、跨組連不到。CP 和 AP 的差別只有一個判斷：CP 模式下，連得到的節點沒過半就拒絕讀寫。網路恢復時，用 last-write-wins 合併各節點的資料。另外有一個 `merge` 模組，示範 AP 改用 version vector 或 G-Counter 合併時，怎麼做到不丟資料。

`verify/` 用真實的 etcd 和 Cassandra 各起 3 台，跑同一個情境：3 台都寫入 x=1 之後拔掉第 3 台的網路，兩側各寫一次、各讀一次，再接回網路，看最後每台讀到什麼。

每個實作資料夾的運作說明頁（有可以切換情境的互動流程圖，並追到各檔案和函式）：

- [Python 版](./python/how-it-works.html)
- [.NET 版](./dotnet/how-it-works.html)
- [Rust 版](./rust/how-it-works.html)

### 怎麼跑

需要：Python 3.12 以上加 uv、.NET 10 SDK、Rust 1.85 以上；跑真實系統對照要有 Docker。

```bash
python/run.sh demo   # 印出四段情境：CP、AP、2 台 CP、AP 不丟資料的兩種合併（dotnet/、rust/ 用法相同）
python/run.sh test   # 跑單元測試

# 確認三種語言的輸出完全相同
diff <(python/run.sh demo) <(dotnet/run.sh demo) && diff <(python/run.sh demo) <(rust/run.sh demo)

# 用真實系統重跑同一個情境（跑完會自動關掉叢集）
verify/run.sh etcd follower   # 被隔離的 etcd3 是 follower，約 30 秒
verify/run.sh etcd leader     # 被隔離的 etcd3 是 leader
verify/run.sh cassandra       # 約 2 分鐘，3 台要一台一台加入叢集
```

| 內容 | Python | .NET | Rust |
|------|--------|------|------|
| 模擬本體：叢集、分區、讀寫、恢復合併 | `python/cap.py` | `dotnet/src/CapTheorem/Cluster.cs` 等 | `rust/src/lib.rs` |
| AP 不丟資料的合併：version vector、G-Counter | `python/merge.py` | `dotnet/src/CapTheorem/VersionVector.cs`、`GCounter.cs` 等 | `rust/src/merge.rs` |
| 依序跑四段情境並印出結果 | `python/demo.py` | `dotnet/src/CapTheorem/Program.cs` | `rust/src/main.rs` |
| 對應每一條成功標準的測試 | `python/test_cap.py`、`test_merge.py` | `dotnet/tests/CapTheorem.Tests/` | `rust/tests/cap.rs`、`merge.rs` |

| 真實系統對照 | 內容 |
|------|------|
| `verify/run.sh` | 用 Docker Compose 起叢集、跑情境、關掉叢集 |
| `verify/etcd/compose.yaml`、`verify/cassandra/compose.yaml` | 3 台叢集的設定，固定 IP、不對外開 port |
| `verify/scenario.py` | 情境本體：拔網路、讀寫、接回網路、比對結果 |
| `verify/results/*.json` | 最近一次執行的完整紀錄，包含每一步的原始輸出 |

## 實驗紀錄

### 2026-10-30：程序內模擬

情境：3 台節點 A、B、C 都有 `x=1`。網路斷成 `{A, B} | {C}`，A 寫 `x=2`，接著 C 寫 `x=3`，最後網路恢復。

| 步驟 | CP | AP |
|------|----|----|
| 分區時 A 寫 x=2 | 成功 | 成功 |
| 分區時 C 寫 x=3 | 拒絕（只連得到 1 台，需要 2 台才過半） | 成功 |
| 分區時 A 讀 x | 2 | 2 |
| 分區時 C 讀 x | 拒絕 | 3 |
| 恢復後三台讀 x | 都是 2 | 都是 3 |
| 已確認卻被丟掉的寫入 | 無 | x=2（A 的寫入） |

- CP 的代價：C 那側在分區期間完全不能用。但資料沒有分歧，恢復後 C 只要追上進度。
- AP 的代價：分區期間兩側看到不同的資料。A 已經回覆 client「寫入成功」的 x=2，恢復後被 C 較晚寫入的 x=3 蓋掉，client 不會收到任何通知。
- 只有 2 台時，CP 在分區時兩側都湊不到過半（2 台需要 2 票），整個服務停擺。雙機備援如果要選 CP，就需要第三方仲裁（witness）。

### 2026-10-31：補上 .NET 和 Rust 版本

用同一個模型改寫成 .NET 和 Rust，三種語言跑出來的輸出逐字相同（用 `diff` 比對過）。

| 語言 | 測試 | 覆蓋率 | 格式與靜態檢查 |
|------|------|--------|----------------|
| Python | 14 個全部通過 | `cap.py` 100% | ruff 通過 |
| .NET | 14 個全部通過 | `Cluster`、`Version`、`Result` 100% | `dotnet format` 通過，警告視為錯誤 |
| Rust | 12 個全部通過 | 沒量（本機沒有 cargo-llvm-cov） | `cargo fmt`、`clippy -D warnings` 通過 |

Rust 的測試比較少，是因為「分組不合法」的三種情況在 Python 和 .NET 各拆成 3 個參數化測試，Rust 則用一個測試跑迴圈。三種語言測的情境其實一樣。

### 2026-10-31：AP 不丟資料的兩種合併方式

同樣的分區情境，改用兩種方式合併（三種語言輸出相同）：

```
=== 延伸：AP 不丟資料的兩種合併方式 ===
  --- 方案一：version vector，並行寫入兩個都留下 ---
  [正常] A 寫 x=1 → 時鐘 {A:1}，複製到 B、C
  [分區] A 寫 x=2 → 時鐘 {A:2}
  [分區] C 寫 x=3 → 時鐘 {A:1, C:1}
  [恢復] 比較 {A:2} 和 {A:1, C:1} → 並行
  [恢復] 讀 x → 2、3（兄弟版本都留著，交給應用層決定）
  [解決] 應用層合併成 x=5 寫回 A → 時鐘 {A:3, C:1}，取代兄弟版本
  --- 方案二：CRDT 計數器（G-Counter），自動合併 ---
  [分區] A 那側 +2、C 那側 +3
  [恢復] 把總數當一般值用 last-write-wins → 3（少了 A 的 +2）
  [恢復] G-Counter 每格取最大再相加 → {A:2, C:3}，總數 5
```

- version vector 分得出「落後」和「並行」：x=2 的時鐘 `{A:2}` 和 x=3 的 `{A:1, C:1}` 互相沒看過對方，所以兩個都留下。代價是應用層要自己決定怎麼合併。
- G-Counter 讓每台只累加自己那一格，合併時每格取最大值，順序、次數都不影響結果。但只適用於計數器、集合這類能設計成 CRDT 的資料，「把 x 設成某個值」這種覆寫做不到。

| 語言 | 測試 | 覆蓋率 | 格式與靜態檢查 |
|------|------|--------|----------------|
| Python | 28 個全部通過 | `cap.py`、`merge.py` 100% | ruff 通過 |
| .NET | 32 個全部通過 | `Program.cs` 以外 99.2%（只差 enum `switch` 必須有的預設分支） | `dotnet format` 通過，警告視為錯誤 |
| Rust | 22 個全部通過 | 沒量 | `cargo fmt`、`clippy -D warnings` 通過 |

.NET 多出來的 4 個測試是確認 `Order` 轉成「之前／之後／相同／並行」的文字；Python 的 enum 值本身就是文字，不需要另外測。

### 2026-10-31：真實系統對照（etcd 3.7.3、Cassandra 5.0.9）

情境跟模擬相同：3 台都寫入 x=1，拔掉第 3 台的網路，兩側各寫一次、各讀一次，再接回網路。

| 步驟 | 模擬 CP | etcd | 模擬 AP | Cassandra（ONE） |
|------|---------|------|---------|------------------|
| 分區時多數派那側寫 x=2 | 成功 | 成功 | 成功 | 成功 |
| 分區時被隔離的那台寫 x=3 | 拒絕 | 逾時，結果未知 | 成功 | 成功；改用 QUORUM 被拒絕 |
| 分區時被隔離的那台讀 x | 拒絕 | 逾時；改用 serializable 讀到舊值 1 | 3 | 3；改用 QUORUM 被拒絕 |
| 恢復後三台讀 x | 都是 2 | 都是 2 或都是 3（見下表） | 都是 3 | 都是 3 |
| 已確認卻被丟掉的寫入 | 無 | 無 | x=2 | x=2（在 cass1 寫入） |

大方向跟模擬一致：etcd 沒有丟掉任何已確認的寫入；Cassandra 用 ONE 一致性時，cass1 已經回覆成功的 x=2，恢復後被 cass3 較晚寫入的 x=3 蓋掉。另外發現兩件模擬沒涵蓋的事：

**一、CP 系統裡，寫入逾時不等於被拒絕。** etcd 在被隔離的節點上寫入，client 收到的是逾時（`context deadline exceeded`），但這筆寫入可能在網路恢復後才生效。用 `verify/run.sh etcd follower`、`verify/run.sh etcd leader` 各跑 5 次：

| 分區前被隔離的 etcd3 是 | 逾時的 x=3 | x 的修改紀錄（依 revision） | 已確認卻被丟掉的寫入 |
|------|------|------|------|
| follower | 5 次都在恢復後生效 | 1 → 2 → 3 | 0 |
| leader | 5 次都被丟棄 | 1 → 2 | 0 |

推測的原因：follower 會把寫入轉給 leader，這個請求留在還沒斷開的 TCP 連線裡，網路恢復後才送到 leader 並 commit；leader 自己收到的寫入只進了自己的日誌、沒有 commit，恢復後被新 leader 的日誌覆蓋。不論哪一種，x=2 都還在修改紀錄裡，只是被之後的寫入依序蓋過，所以不算遺失。

這代表 client 收到逾時時，要把結果當成「未知」：重試要搭配冪等鍵，或先讀回來確認。這跟[微服務拆分](../2026-10-modular-monolith-to-microservices/)遇到的「扣庫存成功、回應卻逾時」是同一類問題。

**二、一致性可以每筆請求選。** etcd 的讀取預設要過半，但改用 serializable 讀取，被隔離的節點也能回答（回的是舊值 1），等於這一筆選了 A。Cassandra 反過來：預設可以只寫一台，但改用 QUORUM，被隔離的那台就拒絕讀寫，等於這一筆選了 C。CP、AP 比較像系統的預設值，不是整個系統固定不變的屬性。

## 結論

- 結果：有條件可行。假設在模擬和真實系統上都成立：CP 少數派停擺但不丟已確認的寫入，AP 兩側可用但 last-write-wins 會默默丟掉已確認的寫入
- 條件或限制：
  - 選 CP：分區時少數派那側會停擺，而且至少要 3 票才撐得過分區（雙機要加 witness）。寫入逾時要當成結果未知，重試要搭配冪等鍵或先讀回確認
  - 選 AP：預設的 last-write-wins 會默默丟資料。要不丟，就用 version vector 把衝突交給應用層，或把資料設計成 CRDT
  - 驗證只在一台電腦上用 Docker 做，沒有跨機房網路、時鐘誤差和長時間隨機故障
- 如果要正式導入，下一步是：雙機備援 PoC 依這裡的結論決定要不要加 witness；需要 AP 的資料，先檢查能不能設計成 CRDT，不行的話規劃應用層怎麼處理兄弟版本
