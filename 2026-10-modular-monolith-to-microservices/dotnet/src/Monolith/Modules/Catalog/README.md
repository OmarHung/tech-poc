# Catalog 模組（⚠ 已轉微服務）

這個模組已經拆成獨立服務 **CatalogService**，現役程式碼在 [`src/CatalogService/Catalog/`](../../../CatalogService/Catalog/)。

這份保留在單體裡原本的位置，有兩個用途：

1. **對照**：可以直接比較拆分前後。除了 namespace 和拆分時新增的 `HttpCatalogClient.cs`，兩份檔案一模一樣。
2. **回滾**：monolith 用 `CATALOG_MODE=inprocess` 啟動，就會改回使用這份原本的實作。

| 檔案 | 拆分後的狀態 |
|------|-------------|
| `ICatalog.cs` | 仍在使用：Orders 模組依賴的介面 |
| `HttpCatalogClient.cs` | 拆分時新增：透過 HTTP 呼叫 CatalogService |
| `CatalogEndpoints.cs`、`HostingExtensions.cs` | 仍在使用：`/products` 請求改成轉給 CatalogService |
| `Product.cs`、`ApiException.cs`、`CatalogRules.cs` | 仍在使用：資料格式、錯誤、輸入驗證，Orders 和 HTTP client 都會用到 |
| `InMemoryCatalog.cs` 與 `CatalogSeed` 種子資料 | 只在 `inprocess` 模式（回滾）使用；確定不回滾後即可刪除 |
