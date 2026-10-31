//! L：里氏替換原則（Liskov Substitution Principle）。
//!
//! 子型別要能替換父型別，呼叫端不用知道拿到的是哪一種。
//! DiscountRule 的契約是 0 <= discount(subtotal) <= subtotal，
//! total_after_discount() 靠這個契約保證總額不會是負數。
//!
//! 情境：新增「折 200 元」。小計只有 150 元的訂單套用後，總額是多少？

use crate::ocp::DiscountRule;

pub const SAMPLE_SUBTOTALS: [i64; 5] = [0, 150, 999, 1000, 5000];

// --- 違反：不管小計多少都折這個金額，小計不夠折時總額變負數 ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UncappedAmountOff(pub i64);

impl DiscountRule for UncappedAmountOff {
    fn discount(&self, _subtotal: i64) -> i64 {
        self.0
    }
}

// --- 遵守：最多折到 0 元 ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedAmountOff(pub i64);

impl DiscountRule for FixedAmountOff {
    fn discount(&self, subtotal: i64) -> i64 {
        self.0.min(subtotal)
    }
}

/// 回傳讓 rule 違反契約的小計。空的代表這組樣本都守住了契約。
///
/// Rust 沒有預設參數，一般用法是傳 `&SAMPLE_SUBTOTALS`。
pub fn contract_violations(rule: &dyn DiscountRule, subtotals: &[i64]) -> Vec<i64> {
    subtotals
        .iter()
        .copied()
        .filter(|&subtotal| !(0..=subtotal).contains(&rule.discount(subtotal)))
        .collect()
}
