# catalog 模組（⚠ 已轉微服務）

這個模組已經拆成獨立服務 **catalog-service**，現役程式碼在 [`src/catalog_service/catalog/`](../../../catalog_service/catalog/)。

這份保留在單體裡原本的位置，有兩個用途：

1. **對照**：可以直接比較拆分前後。除了 `__init__.py` 的說明和拆分時新增的 `remote.py`，兩份檔案一模一樣。`tests/test_catalog.py` 會對兩份各跑一次同樣的測試，防止它們慢慢走樣。
2. **回滾**：monolith 用 `CATALOG_MODE=inprocess` 啟動，就會改回使用這份原本的實作。

| 檔案 | 拆分後的狀態 |
|------|-------------|
| `port.py`（`CatalogPort`） | 仍在使用：orders 模組依賴的介面 |
| `remote.py`（`HttpCatalogClient`） | 拆分時新增：透過 HTTP 呼叫 catalog-service |
| `api.py` | 仍在使用：`/products` 請求改成轉給 catalog-service |
| `models.py`、`errors.py`、`validation.py` | 仍在使用：資料格式、錯誤、輸入驗證，orders 和 HTTP client 都會用到 |
| `store.py`（`InMemoryCatalog`）與種子資料 | 只在 `inprocess` 模式（回滾）使用；確定不回滾後即可刪除 |
