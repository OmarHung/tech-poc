//! SOLID 五個原則各用一個電商結帳的小情境，把「違反版」和「遵守版」的差別印出來。
//!
//! 輸出只用型別名稱（三種語言都一樣），不印函式名稱（各語言的命名慣例不同），
//! 所以和 Python、.NET 版本的輸出可以逐字相同。

use std::fmt::Display;

use solid_principles::dip::{
    CheckoutService, LegacyCheckoutService, RecordingNotifier, SmtpMailer,
};
use solid_principles::isp::{
    CashOnDelivery, Chargeable, LegacyCashOnDelivery, LegacyPaymentMethod,
};
use solid_principles::lsp::{
    FixedAmountOff, SAMPLE_SUBTOTALS, UncappedAmountOff, contract_violations,
};
use solid_principles::ocp::{
    DiscountRule, NoDiscount, PercentOff, legacy_total_after_discount, total_after_discount,
};
use solid_principles::srp::{InvoiceCalculator, LegacyOrder, LineItem, ShippingPolicy};

const CUSTOMER_EMAIL: &str = "alice@example.com";

/// 新需求：滿 threshold 折 amount。寫在示範程式裡，ocp 模組一行都沒改。
struct SpendThresholdOff {
    threshold: i64,
    amount: i64,
}

impl DiscountRule for SpendThresholdOff {
    fn discount(&self, subtotal: i64) -> i64 {
        if subtotal >= self.threshold {
            self.amount
        } else {
            0
        }
    }
}

/// 成功印結果，失敗印「錯誤：訊息」
fn outcome<T: Display, E: Display>(result: Result<T, E>) -> String {
    match result {
        Ok(value) => value.to_string(),
        Err(error) => format!("錯誤：{error}"),
    }
}

fn yes_no(flag: bool) -> &'static str {
    if flag { "是" } else { "否" }
}

fn show_srp() {
    println!("\n=== S：單一職責原則（SRP）===");
    println!("  情境：訂單未稅 960 元。財務要求發票改成含 5% 稅；物流規則沒變：未稅滿 1000 元免運");
    let items = vec![LineItem::new("滑鼠", 480, 2)];
    let legacy = LegacyOrder::new(items.clone());
    println!(
        "  [違反] LegacyOrder：發票 {} 元、免運：{}  ← 財務的改動連帶改到物流",
        legacy.invoice_amount(),
        yes_no(legacy.ships_free()),
    );
    println!(
        "  [遵守] InvoiceCalculator：發票 {} 元；ShippingPolicy：免運：{}",
        InvoiceCalculator.amount(&items),
        yes_no(ShippingPolicy.ships_free(&items)),
    );
}

fn show_ocp() {
    println!("\n=== O：開放封閉原則（OCP）===");
    println!("  情境：小計 1200 元。原本有 VIP 九折、員工八折，行銷部新增「滿 1000 折 100」");
    let vip = outcome(legacy_total_after_discount(1200, "vip"));
    let threshold = outcome(legacy_total_after_discount(1200, "threshold"));
    println!("  [違反] 套 VIP → {vip} 元");
    println!("  [違反] 套滿千折百 → {threshold}  ← 只能回去改這串判斷");
    let percent = total_after_discount(1200, &PercentOff(10));
    let spend = total_after_discount(
        1200,
        &SpendThresholdOff {
            threshold: 1000,
            amount: 100,
        },
    );
    println!("  [遵守] 套 PercentOff(10) → {percent} 元");
    println!(
        "  [遵守] 套 SpendThresholdOff(1000, 100) → {spend} 元  ← 新類別寫在示範程式裡，ocp 模組沒改"
    );
}

fn show_lsp() {
    println!("\n=== L：里氏替換原則（LSP）===");
    println!("  情境：小計 150 元的訂單，套用「折 200 元」");
    let cases: [(&str, &dyn DiscountRule); 2] = [
        ("[違反] UncappedAmountOff(200)", &UncappedAmountOff(200)),
        ("[遵守] FixedAmountOff(200)", &FixedAmountOff(200)),
    ];
    for (label, rule) in cases {
        let violations = join_or_none(&contract_violations(rule, &SAMPLE_SUBTOTALS));
        let total = total_after_discount(150, rule);
        println!("  {label} → 總額 {total} 元；違反契約的小計：{violations}");
    }

    let rules: [(&str, &dyn DiscountRule); 5] = [
        ("NoDiscount", &NoDiscount),
        ("PercentOff(10)", &PercentOff(10)),
        ("PercentOff(20)", &PercentOff(20)),
        (
            "SpendThresholdOff(1000, 100)",
            &SpendThresholdOff {
                threshold: 1000,
                amount: 100,
            },
        ),
        ("FixedAmountOff(200)", &FixedAmountOff(200)),
    ];
    let passed = rules
        .iter()
        .all(|(_, rule)| contract_violations(*rule, &SAMPLE_SUBTOTALS).is_empty());
    let names = rules.map(|(name, _)| name).join("、");
    let verdict = if passed {
        "全部通過"
    } else {
        "有規則違反契約"
    };
    println!("  契約檢查 {names}：{verdict}");
}

fn join_or_none(subtotals: &[i64]) -> String {
    if subtotals.is_empty() {
        return "無".to_owned();
    }
    subtotals
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join("、")
}

fn show_isp() {
    println!("\n=== I：介面隔離原則（ISP）===");
    println!("  情境：貨到付款能收款，但不能線上退款，也沒有付款資訊可以記住");
    // 呼叫端只看到肥 trait：型別上 refund、remember 都在，執行了才知道不行
    let legacy: &dyn LegacyPaymentMethod = &LegacyCashOnDelivery;
    println!("  [違反] LegacyCashOnDelivery 被迫實作肥介面的 3 個方法");
    println!("    收款 300 元 → {}", legacy.charge(300));
    println!("    退款 300 元 → {}", outcome(legacy.refund(300)));
    println!("    記住付款資訊 → {}", outcome(legacy.remember()));

    let cash = CashOnDelivery;
    println!("  [遵守] CashOnDelivery 只實作收款介面");
    println!("    收款 300 元 → {}", cash.charge(300));
    // Rust 沒辦法在執行期查「有沒有實作某個 trait」，所以這裡直接印「否」。
    // 這句話由 src/isp.rs 的 compile_fail doctest 保證：refund_order(&CashOnDelivery, 300) 編譯不過。
    println!("    能退款嗎 → 否：沒有實作退款介面，退款流程在型別檢查時就收不了它");
}

fn show_dip() {
    println!("\n=== D：依賴反轉原則（DIP）===");
    println!("  情境：結帳後寄通知給 {CUSTOMER_EMAIL}。這個環境沒有 SMTP 伺服器");
    let legacy = LegacyCheckoutService::new();
    println!("  [違反] LegacyCheckoutService 自己建立 SmtpMailer");
    println!(
        "    結帳 1080 元 → {}",
        outcome(legacy.checkout(CUSTOMER_EMAIL, 1080))
    );

    println!("  [遵守] CheckoutService 只依賴通知介面，由外面注入");
    let recorder = RecordingNotifier::default();
    let receipt = outcome(CheckoutService::new(&recorder).checkout(CUSTOMER_EMAIL, 1080));
    let sent = recorder.sent();
    println!(
        "    注入 RecordingNotifier：結帳 1080 元 → {receipt}；記下 {} 則通知給 {}",
        sent.len(),
        sent[0].0,
    );
    let smtp = outcome(CheckoutService::new(&SmtpMailer).checkout(CUSTOMER_EMAIL, 1080));
    println!("    注入 SmtpMailer：結帳 1080 元 → {smtp}");
}

fn main() {
    show_srp();
    show_ocp();
    show_lsp();
    show_isp();
    show_dip();
}
