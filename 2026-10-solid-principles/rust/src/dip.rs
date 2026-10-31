//! D：依賴反轉原則（Dependency Inversion Principle）。
//!
//! 高層的業務流程不該依賴低層的實作細節，兩者都該依賴抽象。
//!
//! 情境：結帳後要通知客人。這個 PoC 沒有 SMTP 伺服器，真的寄信一定失敗。

use std::cell::RefCell;
use std::fmt;

/// 連不到寄信用的伺服器
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionError {
    /// host:port
    pub address: String,
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "連不到 {}", self.address)
    }
}

impl std::error::Error for ConnectionError {}

/// 低層細節：用 SMTP 寄信。模擬的環境沒有 SMTP 伺服器，所以一定失敗。
#[derive(Debug, Clone, Copy, Default)]
pub struct SmtpMailer;

impl SmtpMailer {
    pub const HOST: &'static str = "smtp.example.com";
    pub const PORT: u16 = 25;
}

// --- 違反：結帳流程自己建立 SmtpMailer，外面換不掉 ---

#[derive(Debug)]
pub struct LegacyCheckoutService {
    mailer: SmtpMailer,
}

impl LegacyCheckoutService {
    pub fn new() -> Self {
        Self { mailer: SmtpMailer }
    }

    /// 寄信一定失敗，所以 Ok 那條路永遠走不到。這是刻意的：換不掉 SmtpMailer 就測不了成功的情況。
    pub fn checkout(&self, email: &str, amount: i64) -> Result<String, ConnectionError> {
        let receipt = format!("已收款 {amount} 元");
        self.mailer.send(email, &receipt)?;
        Ok(receipt)
    }
}

impl Default for LegacyCheckoutService {
    fn default() -> Self {
        Self::new()
    }
}

// --- 遵守：結帳流程只依賴 Notifier trait，接哪一種由外面決定 ---

pub trait Notifier {
    fn send(&self, to: &str, message: &str) -> Result<(), ConnectionError>;
}

/// 低層細節也依賴同一個抽象：SmtpMailer 實作 Notifier，才能被注入 CheckoutService
impl Notifier for SmtpMailer {
    fn send(&self, _to: &str, _message: &str) -> Result<(), ConnectionError> {
        Err(ConnectionError {
            address: format!("{}:{}", Self::HOST, Self::PORT),
        })
    }
}

/// 把通知記在記憶體裡，給測試和示範用。
///
/// send 只拿到 `&self`（寄信本來就不該改動寄信的人），所以記錄用 RefCell 在內部改。
#[derive(Debug, Default)]
pub struct RecordingNotifier {
    sent: RefCell<Vec<(String, String)>>,
}

impl RecordingNotifier {
    /// 目前為止記下的通知（收件人, 內容），回傳複本
    pub fn sent(&self) -> Vec<(String, String)> {
        self.sent.borrow().clone()
    }
}

impl Notifier for RecordingNotifier {
    fn send(&self, to: &str, message: &str) -> Result<(), ConnectionError> {
        self.sent
            .borrow_mut()
            .push((to.to_owned(), message.to_owned()));
        Ok(())
    }
}

pub struct CheckoutService<'a> {
    notifier: &'a dyn Notifier,
}

impl<'a> CheckoutService<'a> {
    pub fn new(notifier: &'a dyn Notifier) -> Self {
        Self { notifier }
    }

    pub fn checkout(&self, email: &str, amount: i64) -> Result<String, ConnectionError> {
        let receipt = format!("已收款 {amount} 元");
        self.notifier.send(email, &receipt)?;
        Ok(receipt)
    }
}
