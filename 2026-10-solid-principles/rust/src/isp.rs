//! I：介面隔離原則（Interface Segregation Principle）。
//!
//! 不要逼實作者依賴它用不到的方法。介面太肥，實作者只好回傳「不支援」交差，
//! 呼叫端從型別上看不出哪些方法其實不能用，等到執行才知道。
//!
//! 情境：貨到付款能收款，但不能線上退款，也沒有付款資訊可以記住給下次用。

use std::fmt;

/// 付款方式被迫實作、其實做不到的操作
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotSupported(pub &'static str);

impl fmt::Display for NotSupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for NotSupported {}

// --- 違反：一個肥 trait，每種付款方式都得實作三個方法 ---

/// refund／remember 回傳 Result，因為總有付款方式做不到；
/// 拿到 `&dyn LegacyPaymentMethod` 的呼叫端看不出是哪一種，只能執行了再說。
pub trait LegacyPaymentMethod {
    fn charge(&self, amount: i64) -> String;

    fn refund(&self, amount: i64) -> Result<String, NotSupported>;

    /// 記住付款資訊，下次一鍵付款。
    fn remember(&self) -> Result<String, NotSupported>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LegacyCreditCard;

impl LegacyPaymentMethod for LegacyCreditCard {
    fn charge(&self, amount: i64) -> String {
        format!("信用卡扣款 {amount} 元")
    }

    fn refund(&self, amount: i64) -> Result<String, NotSupported> {
        Ok(format!("信用卡退款 {amount} 元"))
    }

    fn remember(&self) -> Result<String, NotSupported> {
        Ok("已記住這張信用卡".to_owned())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LegacyCashOnDelivery;

impl LegacyPaymentMethod for LegacyCashOnDelivery {
    fn charge(&self, amount: i64) -> String {
        format!("送達時收現 {amount} 元")
    }

    fn refund(&self, _amount: i64) -> Result<String, NotSupported> {
        Err(NotSupported("貨到付款不能線上退款"))
    }

    fn remember(&self) -> Result<String, NotSupported> {
        Err(NotSupported("貨到付款沒有付款資訊可以記住"))
    }
}

// --- 遵守：一種能力一個 trait，付款方式做得到哪些就實作哪些 ---

pub trait Chargeable {
    fn charge(&self, amount: i64) -> String;
}

pub trait Refundable {
    fn refund(&self, amount: i64) -> String;
}

pub trait Rememberable {
    fn remember(&self) -> String;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CreditCard;

impl Chargeable for CreditCard {
    fn charge(&self, amount: i64) -> String {
        format!("信用卡扣款 {amount} 元")
    }
}

impl Refundable for CreditCard {
    fn refund(&self, amount: i64) -> String {
        format!("信用卡退款 {amount} 元")
    }
}

impl Rememberable for CreditCard {
    fn remember(&self) -> String {
        "已記住這張信用卡".to_owned()
    }
}

/// 只實作 Chargeable。沒有退款、記住付款資訊的方法可以呼叫，寫了就編譯不過：
///
/// ```compile_fail
/// use solid_principles::isp::{CashOnDelivery, Refundable};
///
/// CashOnDelivery.refund(300);
/// ```
///
/// ```compile_fail
/// use solid_principles::isp::{CashOnDelivery, Rememberable};
///
/// CashOnDelivery.remember();
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct CashOnDelivery;

impl Chargeable for CashOnDelivery {
    fn charge(&self, amount: i64) -> String {
        format!("送達時收現 {amount} 元")
    }
}

/// 退款流程只收 Refundable：
///
/// ```
/// use solid_principles::isp::{CreditCard, refund_order};
///
/// assert_eq!(refund_order(&CreditCard, 300), "信用卡退款 300 元");
/// ```
///
/// 傳 CashOnDelivery 進來，編譯器就擋下（上面那段能編譯，證明擋下的原因只有 trait 不符）：
///
/// ```compile_fail
/// use solid_principles::isp::{CashOnDelivery, refund_order};
///
/// refund_order(&CashOnDelivery, 300);
/// ```
pub fn refund_order(method: &impl Refundable, amount: i64) -> String {
    method.refund(amount)
}
