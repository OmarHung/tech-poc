# tech-poc

技術可行性驗證（Proof of Concept）的集合。每個 PoC 一個子資料夾，只回答一個問題：「這個做法在技術上可不可行？」

## PoC 清單

| PoC | 主題 | 狀態 | 結論 |
|-----|------|------|------|
| [2026-10-modular-monolith-to-microservices](./2026-10-modular-monolith-to-microservices/) | 模組化單體轉微服務（.NET / Python / Rust） | 已完成 | 有條件可行：起點要是模組化單體，並補上跨服務一致性與資料搬遷 |
| [2026-10-checkout-queue](./2026-10-checkout-queue/) | 結帳排隊機制（.NET / Python / Rust） | 已完成 | 有條件可行：虛擬等候室＋Redis Lua 能做到不超賣、不重複扣款，三種語言結果一致；一場活動受單一 Redis 執行緒限制（約每秒 9 萬次腳本），前面要有負載平衡器接住大量新連線，狀態推送用 SSE |
| [2026-10-dual-node-failover](./2026-10-dual-node-failover/) | 雙機備援機制：主備同步（.NET / Python / Rust）、熱熱備援與上游指派（.NET） | 進行中 | 有條件可行：主備要有 witness 並同步複寫；熱熱備援要可重播的上游和決定性處理；上游指派要冪等的下游 |
| [2026-10-cap-theorem](./2026-10-cap-theorem/) | CAP 定理範例（.NET / Python / Rust），並用 etcd、Cassandra 對照 | 已完成 | 有條件可行：選 CP 要至少 3 票，寫入逾時要當成結果未知；選 AP 要不丟資料，就不能用 last-write-wins，要改用 version vector 或 CRDT |
| [2026-10-solid-principles](./2026-10-solid-principles/) | SOLID 原則範例（.NET / Python / Rust） | 已完成 | 有條件可行：五個原則防的問題都跑得出來，但每個都要多付型別；只在那個方向真的會變時才划算 |
| [2026-10-clean-code](./2026-10-clean-code/) | Clean Code 重構範例（.NET / Python / Rust） | 已完成 | 有條件可行：先用 characterization test 守住行為再重構，複雜度降到門檻以下；但總行數變多，且只證明得了測過的輸入 |

## 命名與結構慣例

- 資料夾名稱：`YYYY-MM-主題`，主題用英文 kebab-case
- 每個 PoC 都要有 `README.md`，格式照 [TEMPLATE.md](./TEMPLATE.md)
- 每個 PoC 的每個實作資料夾（例如 `rust/`、`dotnet/`）都要有一份 `how-it-works.html`，用瀏覽器打開就能看懂程式怎麼運作，規範照 [HOW-IT-WORKS-GUIDE.md](./HOW-IT-WORKS-GUIDE.md)。狀態改成 `已完成` 之前一定要寫好
- 狀態只用這四種：`規劃中`、`進行中`、`已完成`、`已放棄`
- 結論只用這三種：`可行`、`不可行`、`有條件可行`（後面寫條件）
- PoC 完成後，記得回來更新上面的清單
