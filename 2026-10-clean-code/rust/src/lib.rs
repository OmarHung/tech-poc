//! 把故意寫髒的運費計算 `legacy::calc()` 照 Clean Code 原則重構成 `shipping::shipping_fee()`，
//! 再用 `characterization` 窮舉輸入，證明兩版的行為一模一樣。

pub mod characterization;
pub mod legacy;
pub mod shipping;
