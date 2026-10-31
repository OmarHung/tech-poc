# SOLID 原則範例

- 狀態：已完成
- 開始日期：2026-10-31
- 結論：有條件可行：五個原則防的問題都能用最小程式跑出來，但每個都要多付型別；只在那個方向真的會變時才划算

## 背景與目標

SOLID 常被當成口號背，code review 時說「這違反 SRP」，對方不一定知道會出什麼事。這個 PoC 把五個原則各自防止的問題，放進同一個電商結帳情境，用可以執行的程式跑出「違反版出錯、遵守版不出錯」，當作設計討論時的共同依據。

這也是其他 PoC 背後的原理：[模組化單體轉微服務](../2026-10-modular-monolith-to-microservices/)能做到「訂單模組不改、把目錄模組換成 HTTP 客戶端」，靠的就是 DIP（訂單只依賴目錄的介面）和 LSP（兩種實作守住同一份契約）。

## 要驗證的假設

SOLID 五個原則各自防止的問題，都能在同一個結帳情境裡用最小程式重現：違反版會出錯，遵守版不會；代價是型別數量變多。

## 待釐清問題

- [x] Python 沒有編譯期，ISP 說的「執行前就擋下」要怎麼證明？→ 在測試裡用 `mypy.api.run()` 實際檢查，斷言 mypy 回報型別錯誤；.NET 靠編譯錯誤 CS1503，Rust 靠 `compile_fail` doctest
- [x] 介面寫好了，子型別就一定能替換嗎？→ 不一定。LSP 的契約（折扣不能超過小計）不在型別系統裡，要靠契約測試；而且檢查的是實例，不是類別（見實驗紀錄）
- [ ] 在真實專案裡（例如模組化單體 PoC），套用前後加一個需求要改的檔案數差多少？

## 成功標準

| 指標 | 目標值 | 實測值 |
|------|--------|--------|
| S：財務改成含稅後，未稅 960 元訂單的免運判斷 | 違反版被連帶改成「是」，遵守版維持「否」 | 違反版「是」、遵守版「否」 |
| O：新增「滿千折百」要修改的既有函式數 | 違反版至少 1 個，遵守版 0 個 | 1 個／0 個（新規則寫在示範程式和測試檔，ocp 模組沒改） |
| L：150 元訂單套「折 200 元」的總額 | 違反版是負數而且被契約檢查抓到，遵守版 0 元 | −50 元（抓到小計 0、150）／0 元 |
| I：貨到付款被迫實作、卻只能丟錯誤的方法數 | 違反版至少 1 個，遵守版 0 個 | 2 個／0 個 |
| I：對貨到付款呼叫退款，什麼時候發現 | 遵守版在執行前 | Python：mypy（測試裡驗證）；.NET：編譯錯誤 CS1503；Rust：`compile_fail` doctest |
| D：不連 SMTP 伺服器能不能測結帳流程 | 違反版不能，遵守版能 | 違反版一定失敗，成功路徑永遠測不到；遵守版注入 `RecordingNotifier` 就能測 |
| 三種語言的示範輸出 | 逐字相同 | 相同（`diff` 比對） |

## 範圍外

- 用真實專案量化（修改檔案數、變更頻率），這次只用最小範例
- DI 容器和框架，只用建構子注入
- 其他設計原則（組合優於繼承、Law of Demeter 等）
- 命名、函式長度這類程式碼層級的整潔，見 [Clean Code 重構範例](../2026-10-clean-code/)

## 技術選型

| 項目 | 選擇 | 理由 |
|------|------|------|
| 語言 | Python、.NET、Rust 各寫一份 | 同一套情境、輸出逐字相同；三種型別系統（結構型別、名目型別、trait）對 ISP、LSP 的影響不同，剛好可以對照 |
| 情境 | 電商結帳，金額用整數新台幣元 | 跟其他 PoC 同一個領域；整數不會有浮點誤差，三種語言算出來一樣 |
| 違反版和遵守版 | 放在同一個模組，名稱撞到時違反版加 `Legacy` 前綴 | 打開一個檔案就能對照 |
| 介面 | Python `Protocol`、C# `interface`、Rust `trait` | 各語言的慣用寫法 |
| 「執行前擋下」的驗證 | Python：mypy 2.4（在測試裡跑）；.NET：編譯器；Rust：`compile_fail` doctest | ISP 的好處要能證明，不能只是口頭說 |
| 外部套件 | 範例本身都不用（只有測試工具需要） | 不用裝東西就能跑 |
| 測試 | Python：pytest + pytest-cov + mypy；.NET：xUnit + coverlet；Rust：`cargo test` | 跟其他 PoC 一致 |

## 做法

五個原則各一個模組，每個模組上半是違反版、下半是遵守版。示範程式依序跑五個情境，把兩個版本在同一個需求下的結果並排印出來。輸出只印型別名稱（三種語言都是 PascalCase），不印方法名稱和介面名稱（各語言命名慣例不同），所以可以用 `diff` 逐字比對。

| 原則 | 情境 | 違反版 | 遵守版 |
|------|------|--------|--------|
| S | 財務要求發票含 5% 稅，物流規則沒變 | `LegacyOrder` 的發票和免運共用同一個金額函式 | `InvoiceCalculator`（財務）、`ShippingPolicy`（物流） |
| O | 行銷部新增「滿 1000 折 100」 | 一串判斷折扣種類字串的分支 | `DiscountRule` 介面，新折扣就是新類別 |
| L | 150 元訂單套「折 200 元」 | `UncappedAmountOff`：永遠折滿額 | `FixedAmountOff`：最多折到 0 元，加上契約檢查 |
| I | 貨到付款不能線上退款 | 肥介面逼 `LegacyCashOnDelivery` 實作退款和記住付款資訊 | 收款、退款、記住付款資訊拆成三個介面 |
| D | 結帳後寄通知，環境沒有 SMTP | `LegacyCheckoutService` 自己建立 `SmtpMailer` | `CheckoutService` 只依賴通知介面，由外面注入 |

每個實作資料夾的運作說明頁（有可以切換原則的互動對照圖，並追到各檔案和函式）：

- [Python 版](./python/how-it-works.html)
- [.NET 版](./dotnet/how-it-works.html)
- [Rust 版](./rust/how-it-works.html)

### 怎麼跑

需要：Python 3.12 以上加 uv、.NET 10 SDK、Rust 1.85 以上。

```bash
python/run.sh demo   # 印出五個原則的對照（dotnet/、rust/ 用法相同）
python/run.sh test   # 跑單元測試

# 確認三種語言的輸出完全相同
diff <(python/run.sh demo) <(dotnet/run.sh demo) && diff <(python/run.sh demo) <(rust/run.sh demo)
```

| 內容 | Python | .NET | Rust |
|------|--------|------|------|
| 五個原則 | `python/srp.py` ～ `dip.py` | `dotnet/src/SolidPrinciples/Srp.cs` ～ `Dip.cs` | `rust/src/srp.rs` ～ `dip.rs` |
| 依序跑五個情境並印出結果 | `python/demo.py` | `dotnet/src/SolidPrinciples/Program.cs` | `rust/src/main.rs` |
| 對應每一條成功標準的測試 | `python/test_*.py` | `dotnet/tests/SolidPrinciples.Tests/` | `rust/tests/*.rs`、`rust/src/isp.rs` 的 doctest |

## 實驗紀錄

### 2026-10-31：三種語言的最小範例

情境依序跑完，三種語言的輸出逐字相同。

| 原則 | 違反版 | 遵守版 |
|------|--------|--------|
| S | 發票 1008 元、免運：是（物流沒改需求卻被連帶改到） | 發票 1008 元、免運：否 |
| O | 套「滿千折百」→ 錯誤：不支援的折扣：threshold | 新類別 `SpendThresholdOff` 寫在示範程式裡，1200 元算出 1100 元 |
| L | 總額 −50 元；違反契約的小計：0、150 | 總額 0 元；違反契約的小計：無 |
| I | 退款、記住付款資訊都在執行時才丟錯誤 | 貨到付款沒有實作退款介面，退款流程在執行前就收不了它 |
| D | 結帳 → 錯誤：連不到 smtp.example.com:25 | 注入 `RecordingNotifier` 就能完成結帳並檢查通知內容 |

| 語言 | 測試 | 覆蓋率 | 格式與靜態檢查 |
|------|------|--------|----------------|
| Python | 43 個全部通過（含 2 個用 mypy 檢查型別） | 99%：唯一沒蓋到的是 `LegacyCheckoutService` 的 `return receipt` | ruff 通過 |
| .NET | 41 個全部通過 | 扣掉示範程式 96.9%：沒蓋到的同樣是 `LegacyCheckoutService` 的 `return receipt` | `dotnet format` 通過，警告視為錯誤 |
| Rust | 26 個全部通過（22 個測試 ＋ 4 個 doctest，其中 3 個 `compile_fail`） | 沒量（本機沒有 cargo-llvm-cov） | `cargo fmt`、`clippy -D warnings` 通過 |

測試數量不同，是因為參數化的寫法不同（Rust 用迴圈收成一個測試），以及 Python 多了 2 個 mypy 測試。三種語言測的情境一樣。

移植和交叉檢查時發現的事：

- **SRP 最難被發現。** 其他原則被違反時，多少會在執行時丟出錯誤；SRP 被違反時程式完全正常，只是免運判斷悄悄變了。只有寫了「物流規則沒變，結果也不該變」這種測試才抓得到。
- **LSP 的契約檢查只對實例有效。** 示範用的 `SpendThresholdOff(1000, 100)` 守得住契約，但 `SpendThresholdOff(50, 100)` 遇到小計 60 會折 100，一樣違反。要一勞永逸，就得像 `FixedAmountOff` 一樣在折扣裡設上限，或在建構時擋下 `amount > threshold`。
- **「型別檢查會擋下」要真的驗證。** Python 版一開始只在註解和輸出裡說「mypy 會擋下」，但沒有任何東西真的在跑 mypy。後來把 mypy 放進測試，並加一個「換成 `CreditCard` 就通過」的正向對照，證明失敗確實是型別造成的。Rust 的 `compile_fail` 也有同樣的問題：它只要編譯失敗就算過，stable 版不檢查錯誤碼，所以同樣加了一個能編譯的正向對照。
- **C# 編譯器比想像中更早擋下。** `CashOnDelivery` 是 `sealed`，寫 `cash is IRefundable` 會被編譯器判定「永遠是 false」（CS0184），在警告視為錯誤的設定下直接編譯失敗，示範程式只好把變數宣告成 `IChargeable`。
- **DIP 的違反版連覆蓋率都看得出來。** 三種語言都有同一行測不到：`LegacyCheckoutService` 寄信之後的 `return receipt`。沒有 SMTP 伺服器，這條路徑永遠跑不到。
- **輸出要語言中立。** 示範輸出原本寫「只能回去改 if/elif」，但 C# 用 switch、Rust 用 match，後來改成「只能回去改這串判斷」。

## 結論

- 結果：有條件可行
- 條件或限制：五個原則的好處都跑得出來，但每個都要多付型別（例如 ISP 從 1 個抽象類別變成 3 個介面）。只在「那個方向真的會變」時才划算：有兩種以上的角色會要求同一段程式改（S）、同一類規則會一直增加（O）、實作者真的做不到某些方法（I）、依賴的東西要在測試或部署時替換（D）。LSP 沒有不划算的時候，違反它就是 bug，但型別系統保證不了，要有契約測試
- 如果要正式導入，下一步是：拿[模組化單體 PoC](../2026-10-modular-monolith-to-microservices/) 實際量一次「加一個需求要改幾個檔案」；code review 時遇到 SOLID 爭議，直接指到這裡對應的情境
