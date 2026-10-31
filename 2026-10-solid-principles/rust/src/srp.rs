//! S：單一職責原則（Single Responsibility Principle）。
//!
//! 一個模組只該為一種角色（actor）改變。這裡有兩種角色：
//! 財務在意「發票金額」，物流在意「免不免運」。
//!
//! 情境：財務要求發票金額改成含 5% 營業稅。物流的規則沒變：未稅小計滿 1000 元免運。

pub const TAX_RATE_PERCENT: i64 = 5;
pub const FREE_SHIPPING_THRESHOLD: i64 = 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineItem {
    pub name: String,
    pub unit_price: i64,
    pub quantity: i64,
}

impl LineItem {
    pub fn new(name: impl Into<String>, unit_price: i64, quantity: i64) -> Self {
        Self {
            name: name.into(),
            unit_price,
            quantity,
        }
    }
}

// --- 違反：一個型別同時服務財務和物流，兩邊共用 amount() ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyOrder {
    pub items: Vec<LineItem>,
}

impl LegacyOrder {
    pub fn new(items: Vec<LineItem>) -> Self {
        Self { items }
    }

    // 財務
    pub fn invoice_amount(&self) -> i64 {
        self.amount()
    }

    // 物流
    pub fn ships_free(&self) -> bool {
        self.amount() >= FREE_SHIPPING_THRESHOLD
    }

    fn amount(&self) -> i64 {
        // 為了財務的需求在這裡加上稅，用到這個函式的免運判斷也跟著變了
        let base: i64 = self
            .items
            .iter()
            .map(|item| item.unit_price * item.quantity)
            .sum();
        base + base * TAX_RATE_PERCENT / 100
    }
}

// --- 遵守：財務和物流各有自己的型別，改一邊不會動到另一邊 ---

/// 未稅小計。兩種角色都認同的事實，不帶任何一方的規則，所以可以共用。
pub fn subtotal(items: &[LineItem]) -> i64 {
    items
        .iter()
        .map(|item| item.unit_price * item.quantity)
        .sum()
}

/// 財務
#[derive(Debug, Clone, Copy, Default)]
pub struct InvoiceCalculator;

impl InvoiceCalculator {
    pub fn amount(&self, items: &[LineItem]) -> i64 {
        let base = subtotal(items);
        base + base * TAX_RATE_PERCENT / 100
    }
}

/// 物流
#[derive(Debug, Clone, Copy, Default)]
pub struct ShippingPolicy;

impl ShippingPolicy {
    pub fn ships_free(&self, items: &[LineItem]) -> bool {
        subtotal(items) >= FREE_SHIPPING_THRESHOLD
    }
}
