# 模組化單體轉微服務

- 狀態：已完成
- 開始日期：2026-10-30
- 結論：有條件可行（條件見最後一節）

## 背景與目標

驗證能不能把模組化單體（modular monolith）裡的某個模組拆成獨立服務，而且拆的過程不影響既有功能。同樣的設計用 .NET、Python、Rust 各實作一次，用同一套驗證腳本比較三者。

## 要驗證的假設

可以用漸進的方式（Strangler Fig 模式）把單一模組拆成獨立服務，過程中不用停機，拆完之後的效能和資料一致性都在可接受範圍。

## 待釐清問題

- [x] **拆分邊界**：拆 catalog（商品＋庫存）。邊界就是 `CatalogPort` 介面，只有三個操作：列出、查詢、扣庫存。因為起點是模組化單體，orders 本來就只透過這個介面呼叫 catalog，所以拆分只需要新增「HTTP 版的介面實作」和「裝 catalog 模組的服務」，orders 的程式一行都不用改。
- [x] **服務間通訊**：這次用同步 REST，並量出每多一跳的代價（見實驗紀錄）。非同步 message queue 沒有驗證。
- [ ] **資料庫拆分**：只驗證了一半。
  - 已驗證：拆分後 monolith 不再持有 catalog 資料，所有 catalog 相關請求都轉給 catalog-service，資料只有一份。實測 monolith 和 catalog-service 看到的庫存一致。
  - 未驗證：資料都在記憶體，「資料搬遷」是用 catalog-service 重新載入同一份種子資料來模擬。跨服務寫入的一致性（Saga / Outbox）也沒有做。下單時如果「扣庫存成功，但回應在路上逾時」，會出現庫存被扣、訂單卻沒建立的情況。
- [x] **漸進遷移**：gateway 可以在執行中切換 `/products` 的去向。負載下來回切 15 次，三種語言都是 0 個失敗請求。要切回舊版，再呼叫一次切換 API 就好。
- [x] **代價**：延遲增加、多一個故障點、要維運的程序從 1 個變 3 個（見實驗紀錄和結論）。

## 成功標準

| 指標 | 目標值 | 實測值 |
|------|--------|--------|
| 遷移過程服務中斷時間 | 0 秒 | gateway 切換路由：0 失敗（三種語言）。但 monolith 從 inprocess 改成 remote 需要重啟，正式環境要靠滾動部署或藍綠部署，這段本次未驗證 |
| 拆分後 API p95 延遲增加 | 待定 | 下單 p95：Rust +0.05ms、.NET +0.12ms、Python +1.15ms（本機 loopback） |
| 切回舊版所需時間 | 待定 | 一次 API 呼叫，立即生效 |

## 範圍外

- 一次拆完整個單體（這次只拆 catalog 一個模組）
- 正式環境部署
- 真正的資料庫與資料搬遷（資料都在記憶體）
- 跨服務交易一致性（Saga / Outbox）
- gateway 管理 API 的驗證與授權（PoC 只綁 `127.0.0.1`，正式環境一定要加）

## 技術選型

| 項目 | 選擇 | 理由 |
|------|------|------|
| 遷移模式 | Strangler Fig + Branch by Abstraction | 前面放 gateway，逐步把流量切走；單體內用介面隔開，換實作不動呼叫端 |
| 服務間通訊 | 同步 REST + JSON | 最簡單，三種語言都有成熟套件，先量出最基本的代價 |
| Rust | axum 0.8、reqwest 0.13、tokio | |
| Python | FastAPI 0.162、httpx 0.28、uvicorn（含 uvloop），uv 管理 | |
| .NET | ASP.NET Core 10 Minimal API、YARP 2.3（直接轉發模式） | YARP 是微軟官方的反向代理元件 |
| 驗證 | Python 標準函式庫寫的黑箱腳本 | 不依賴任何一種實作，三種語言跑同一套情境，數字才能比較 |

## 做法

### 資料夾結構

三種語言的結構相同。以 Rust 為例：

```
rust/
├── monolith/                  模組化單體（原本的系統，拆分後仍在運作）
│   └── src/modules/
│       ├── orders.rs          orders 模組：留在單體
│       └── catalog/           catalog 模組：⚠ 已轉微服務（保留在原位，供對照與回滾）
├── catalog-service/           從 monolith 的 catalog 模組拆出來的微服務
│   └── src/catalog/           搬過來的 catalog 模組（現役）
└── gateway/                   Strangler Fig 門面
```

| 語言 | 模組化單體 | 已轉微服務的模組（原位） | 微服務 | gateway |
|------|-----------|------------------------|--------|---------|
| Rust | `rust/monolith/` | `rust/monolith/src/modules/catalog/` | `rust/catalog-service/` | `rust/gateway/` |
| Python | `python/src/monolith/` | `python/src/monolith/modules/catalog/` | `python/src/catalog_service/` | `python/src/gateway/` |
| .NET | `dotnet/src/Monolith/` | `dotnet/src/Monolith/Modules/Catalog/` | `dotnet/src/CatalogService/` | `dotnet/src/Gateway/` |

- **每個語言資料夾都有一份 `how-it-works.html`**，直接用瀏覽器打開就能看：互動流程圖（四個階段的請求路徑）、一個下單請求經過哪些檔案和函式、啟動流程、關鍵設計。[Rust](./rust/how-it-works.html)、[Python](./python/how-it-works.html)、[.NET](./dotnet/how-it-works.html)
- **catalog 模組的程式碼有兩份**，這是刻意的，真實的遷移也是這樣：模組搬到新服務之後，單體裡的舊模組要等確定不回滾了才刪。兩份檔案除了說明文字、單體多出來的 HTTP client 之外完全相同（.NET 另外只差 namespace）。三種語言都會拿同一套 catalog 測試跑兩份，防止它們慢慢走樣。
- 每個「已轉微服務」的資料夾裡都有一份 `README.md`，列出哪些檔案拆分後仍在使用、哪些只剩回滾用途、可以刪掉。
- gateway 的規則：`/products` 開頭的請求依目前設定送給 monolith 或 catalog-service，其他一律送 monolith。

### 請求怎麼走

```
                     ┌───────────────────── gateway ─────────────────────┐
  client ──────────▶ │ /products/*  → monolith 或 catalog-service（可切換）│
                     │ 其他路徑     → monolith                            │
                     └───────────────────────────────────────────────────┘

  monolith（模組化單體）                                    catalog-service
  ┌───────────────────────────────────────────┐          ┌──────────────────┐
  │ orders 模組 ──▶ CatalogPort（介面）         │          │ catalog 模組      │
  │                  ├ 拆分前：同程序內的 catalog 模組      │（從單體搬過來，現役）│
  │                  │         （⚠ 已轉微服務，留作回滾）    │                  │
  │                  └ 拆分後：HTTP client ───── HTTP ──▶ │                  │
  └───────────────────────────────────────────┘          └──────────────────┘
```

### 遷移階段

`verify/run.sh` 依序跑三個階段，每個階段結束都跑一次黑箱驗證：

| 階段 | gateway 的 `/products` | monolith 的 catalog | 驗證什麼 |
|------|-----------------------|---------------------|----------|
| 1. 單體 | → monolith | 同程序內（`CATALOG_MODE=inprocess`） | 功能正確，量基準延遲 |
| 2. 拆出 | → monolith，負載下來回切到 service | 透過 HTTP 呼叫 catalog-service（`remote`） | 資料只有一份、切換零失敗、延遲代價 |
| 3. 故障 | — | `remote`，但 catalog-service 停掉 | 多出來的網路依賴會造成什麼影響 |

### HTTP 介面（三種語言完全相同）

| 方法與路徑 | 說明 |
|-----------|------|
| `GET /products`、`GET /products/{id}` | 查商品 |
| `POST /products/{id}/reserve` `{"quantity": n}` | 扣庫存（原子操作，不會超賣） |
| `POST /orders` `{"productId": 1, "quantity": 2}` | 下單：驗證輸入 → 扣庫存 → 建單，回 201 |
| `GET /orders/{id}` | 查訂單 |
| `GET /admin/routes`、`PUT /admin/routes` `{"catalog": "monolith"\|"service"}` | gateway 專用：查看或切換路由 |
| `GET /health` | 健康檢查，monolith 會回報目前的 `catalogMode` |

- 錯誤一律回 `{"error": "..."}`：400 輸入錯誤、404 找不到、409 庫存不足、502 下游服務不可用
- 每個回應都帶 `X-Served-By` 標頭，驗證腳本靠它判斷是誰回應的
- 連不到下游時只回「catalog unavailable」，內部網址等細節只寫進伺服器 log

### 怎麼跑

需要：Rust 1.85 以上、Python 3.12 以上加 uv、.NET 10 SDK。會用到 port 8080–8082。

```bash
./verify/run.sh rust                  # 或 python、dotnet：建置 → 三個階段 → 各跑一次驗證
python3 verify/scenario.py summary    # 把 verify/results/ 裡所有結果整理成表格

cd rust && ./run.sh test              # 各語言的單元測試（python/、dotnet/ 用法相同）
```

## 實驗紀錄

### 2026-10-30

**功能驗證：三種語言全部通過**

| 語言 | 黑箱檢查（3 個階段） | 單元測試 |
|------|---------------------|----------|
| Rust | 70 / 70 | 35 個，clippy 無警告 |
| Python | 70 / 70 | 79 個，覆蓋率 97% |
| .NET | 70 / 70 | 81 個，建置 0 警告 |

單元測試數包含 catalog 測試對兩份程式碼各跑一次。

**延遲（p50，單位 ms）**

| 情境 | 請求路徑 | Rust | .NET | Python |
|------|----------|------|------|--------|
| 拆分前：查商品 | gateway → monolith | 0.091 | 0.150 | 0.659 |
| 拆分前：下單 | gateway → monolith | 0.104 | 0.156 | 0.653 |
| 拆分後：查商品 | gateway → monolith → catalog-service | 0.126 | 0.254 | 1.397 |
| 拆分後：下單 | gateway → monolith → catalog-service | 0.141 | 0.240 | 1.463 |
| 路由切到 service 後：查商品 | gateway → catalog-service | 0.093 | 0.141 | 0.927 |
| **每多一跳的成本**（下單 p50 差） | | **+0.04** | **+0.08** | **+0.81** |

p95、p99 在 `verify/results/<語言>-<階段>.json`。這組數字來自整理資料夾結構之後的那一輪。前一輪是同一套程式、在比較空閒的時候量的：Rust 和 .NET 幾乎一樣，Python 每一跳的成本是 +0.57ms，可見 Python 對機器負載比較敏感。

**負載下切換路由**（8 個並行 client，3 秒內來回切 15 次）

| 語言 | 切換期間請求數 | 失敗 |
|------|---------------|------|
| Rust | 42,796 | 0 |
| .NET | 34,633 | 0 |
| Python | 2,626 | 0 |

**catalog-service 停掉時**（三種語言結果相同）

- 下單回 502，查商品也回 502（不管路由指向哪邊）
- 既有訂單仍然查得到，因為訂單建立時已經存了商品名稱和價格的快照，不需要再問 catalog

**量測過程遇到的問題**

1. 一開始每個操作只熱身 100 次，.NET 出現「越後面量越快」的現象，同一條路徑前後差到 1.5 倍。原因是 JIT 分層編譯還沒完成。改成每個操作先熱身 3 秒再量，數字就穩定了。
2. Python「路由切到 service 後」的查商品比拆分前慢約 0.27ms。單獨重測時，gateway → monolith 和 gateway → catalog-service 都是約 0.66–0.70ms，沒有結構上的差異，是執行順序造成的波動。同一次實驗也看出 Python gateway 本身每轉一手要約 0.55ms（直接打服務只要 0.116ms），這是 Python 版延遲的主要來源。
3. ASP.NET 預設會把 JSON 裡的字串 `"2"` 當成數字 2 接受，跟另外兩種語言的行為不一致，所以改成嚴格模式。

4. 有一輪是在機器很忙的時候跑的（系統負載超過 CPU 核心數）。功能檢查照樣全過，但延遲雜訊很大：Rust 的同一條路徑變慢一倍、.NET 的 p99 跳到 6ms。那輪的數字已經丟掉，等機器比較空閒時才重量。

**量測環境與限制**：所有程式跑在同一台開發機上，量測時機器大致空閒，全部走 loopback，每個情境循序送 1000 個請求。數字只適合比較相對大小，不代表正式環境。跨機器時，每一跳還要再加上真實的網路延遲。

## 結論

- 結果：**有條件可行**
- 條件或限制：
  1. **起點必須是模組化單體**。模組之間只透過明確的介面互相呼叫，是這次拆分成本這麼低的關鍵：orders 完全不用改。如果現有單體的模組之間會直接存取彼此的資料，要先花力氣整理成模組化單體，才適合拆。
  2. **每多一跳都要付網路成本**。光是本機就要 +0.04ms（Rust）到 +0.6～0.8ms（Python），跨機器還要再加網路延遲。呼叫頻繁、粒度很細的跨模組操作不適合拆出去。
  3. **多了一個會壞的地方**。catalog-service 停掉時，下單會直接失敗。正式導入前要加上逾時重試、熔斷，或改成非同步。
  4. **跨服務寫入不再有交易保證**。要補上冪等鍵和補償機制（Saga / Outbox）才能上線。
  5. **資料搬遷還沒驗證**，這次的資料都在記憶體裡。
- 如果要正式導入，下一步是：
  1. 兩邊各用一個真正的資料庫，驗證資料搬遷（雙寫或 CDC）
  2. 下單流程加上冪等鍵和 Outbox，實測「扣庫存成功、但建單失敗」的情境
  3. 在真實網路上（兩台機器或 Kubernetes）重新量延遲
  4. 語言選擇上：Rust 和 .NET 每多一跳都在 0.1ms 以內；Python 每跳約 0.6～0.8ms，而且對機器負載比較敏感，如果服務之間的呼叫鏈很長要特別注意
