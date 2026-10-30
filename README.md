# tech-poc

技術可行性驗證（Proof of Concept）的集合。每個 PoC 一個子資料夾，只回答一個問題：「這個做法在技術上可不可行？」

## PoC 清單

| PoC | 主題 | 狀態 | 結論 |
|-----|------|------|------|
| [2026-10-modular-monolith-to-microservices](./2026-10-modular-monolith-to-microservices/) | 模組化單體轉微服務（.NET / Python / Rust） | 已完成 | 有條件可行：起點要是模組化單體，並補上跨服務一致性與資料搬遷 |
| [2026-10-checkout-queue](./2026-10-checkout-queue/) | 結帳排隊機制 | 進行中 | — |
| [2026-10-dual-node-failover](./2026-10-dual-node-failover/) | 雙機備援機制（.NET 版完成） | 進行中 | 有條件可行：要有第三方仲裁（witness），而且要同步複寫才能不丟資料 |
| [2026-10-cap-theorem](./2026-10-cap-theorem/) | CAP 定理範例（.NET / Python / Rust） | 進行中 | — |

## 命名與結構慣例

- 資料夾名稱：`YYYY-MM-主題`，主題用英文 kebab-case
- 每個 PoC 都要有 `README.md`，格式照 [TEMPLATE.md](./TEMPLATE.md)
- 狀態只用這四種：`規劃中`、`進行中`、`已完成`、`已放棄`
- 結論只用這三種：`可行`、`不可行`、`有條件可行`（後面寫條件）
- PoC 完成後，記得回來更新上面的清單
