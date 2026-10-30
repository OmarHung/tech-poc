# catalog 模組（⚠ 已轉微服務）

這個模組已經拆成獨立服務 **catalog-service**，現役程式碼在 [`catalog-service/src/catalog/`](../../../../catalog-service/src/catalog/)。

這份保留在單體裡原本的位置，有兩個用途：

1. **對照**：可以直接比較拆分前後。除了 `mod.rs` 的說明和拆分時新增的 `remote.rs`，兩份檔案一模一樣。
2. **回滾**：monolith 用 `CATALOG_MODE=inprocess` 啟動，就會改回使用這份原本的實作。

| 檔案 | 拆分後的狀態 |
|------|-------------|
| `port.rs`（`CatalogPort`） | 仍在使用：orders 模組依賴的介面 |
| `remote.rs`（`HttpCatalogClient`） | 拆分時新增：透過 HTTP 呼叫 catalog-service |
| `routes.rs` | 仍在使用：`/products` 請求改成轉給 catalog-service |
| `product.rs`、`error.rs`、`validation.rs` | 仍在使用：資料格式、錯誤、輸入驗證，orders 和 HTTP client 都會用到 |
| `store.rs`（`InMemoryCatalog`）與種子資料 | 只在 `inprocess` 模式（回滾）使用；確定不回滾後即可刪除 |
