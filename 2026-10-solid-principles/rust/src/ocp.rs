//! O：開放封閉原則（Open-Closed Principle）。
//!
//! 對擴充開放、對修改封閉：加新功能時是寫新的程式碼，不是回去改已經能用的程式碼。
//!
//! 情境：原本有「VIP 九折」「員工八折」，行銷部新增「滿 1000 折 100」。

use std::fmt;

/// 舊版折扣函式不認得的折扣種類
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedDiscount {
    pub kind: String,
}

impl fmt::Display for UnsupportedDiscount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "不支援的折扣：{}", self.kind)
    }
}

impl std::error::Error for UnsupportedDiscount {}

// --- 違反：每多一種折扣，就要回來改這個 match ---

pub fn legacy_total_after_discount(subtotal: i64, kind: &str) -> Result<i64, UnsupportedDiscount> {
    match kind {
        "none" => Ok(subtotal),
        "vip" => Ok(subtotal - subtotal / 10),
        "staff" => Ok(subtotal - subtotal / 5),
        _ => Err(UnsupportedDiscount {
            kind: kind.to_owned(),
        }),
    }
}

// --- 遵守：折扣是一個 trait，新折扣就是新型別，total_after_discount() 不用改 ---

/// 契約：0 <= discount(subtotal) <= subtotal。lsp 模組會檢查這件事。
pub trait DiscountRule {
    fn discount(&self, subtotal: i64) -> i64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoDiscount;

impl DiscountRule for NoDiscount {
    fn discount(&self, _subtotal: i64) -> i64 {
        0
    }
}

/// 打折，欄位是折掉的百分比：PercentOff(10) 就是九折
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PercentOff(pub i64);

impl DiscountRule for PercentOff {
    fn discount(&self, subtotal: i64) -> i64 {
        subtotal * self.0 / 100
    }
}

pub fn total_after_discount(subtotal: i64, rule: &dyn DiscountRule) -> i64 {
    subtotal - rule.discount(subtotal)
}
