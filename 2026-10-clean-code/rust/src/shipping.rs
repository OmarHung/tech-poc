//! 運費計算，legacy.rs 的 calc() 照 Clean Code 原則重構後的版本。
//!
//! 規則：
//! - 重量 1000 公克以內 60 元，5000 公克以內 100 元，超過的部分每 1000 公克（不足也算）加 20 元
//! - 寄離島加 80 元
//! - 本島訂單滿門檻免運：一般會員 1000 元、金卡會員 500 元。離島不提供免運
//! - 冷藏另外收費：本島 50 元、離島 100 元，免運也照收

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Region {
    MainIsland,
    OutlyingIsland,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Membership {
    Normal,
    Gold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parcel {
    pub weight_grams: i64,
    pub region: Region,
    pub membership: Membership,
    pub order_amount: i64,
    pub needs_cold_chain: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidParcel {
    NonPositiveWeight(i64),
    NegativeOrderAmount(i64),
}

impl fmt::Display for InvalidParcel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonPositiveWeight(grams) => write!(f, "重量必須大於 0 公克，收到 {grams}"),
            Self::NegativeOrderAmount(amount) => write!(f, "訂單金額不能是負數，收到 {amount}"),
        }
    }
}

impl std::error::Error for InvalidParcel {}

const LIGHT_PARCEL_MAX_GRAMS: i64 = 1_000;
const LIGHT_PARCEL_FEE: i64 = 60;
const STANDARD_PARCEL_MAX_GRAMS: i64 = 5_000;
const STANDARD_PARCEL_FEE: i64 = 100;
const GRAMS_PER_KILOGRAM: i64 = 1_000;
const FEE_PER_EXTRA_KILOGRAM: i64 = 20;
const OUTLYING_ISLAND_SURCHARGE: i64 = 80;
const FREE_SHIPPING_THRESHOLD_NORMAL: i64 = 1_000;
const FREE_SHIPPING_THRESHOLD_GOLD: i64 = 500;
const COLD_CHAIN_FEE_MAIN_ISLAND: i64 = 50;
const COLD_CHAIN_FEE_OUTLYING_ISLAND: i64 = 100;

pub fn shipping_fee(parcel: &Parcel) -> Result<i64, InvalidParcel> {
    reject_invalid(parcel)?;
    Ok(delivery_fee(parcel) + cold_chain_fee(parcel))
}

fn reject_invalid(parcel: &Parcel) -> Result<(), InvalidParcel> {
    if parcel.weight_grams <= 0 {
        return Err(InvalidParcel::NonPositiveWeight(parcel.weight_grams));
    }
    if parcel.order_amount < 0 {
        return Err(InvalidParcel::NegativeOrderAmount(parcel.order_amount));
    }
    Ok(())
}

fn delivery_fee(parcel: &Parcel) -> i64 {
    if ships_free(parcel) {
        return 0;
    }
    weight_fee(parcel.weight_grams) + region_surcharge(parcel.region)
}

fn ships_free(parcel: &Parcel) -> bool {
    // 離島的物流成本高，行銷部決定不開放免運
    if parcel.region == Region::OutlyingIsland {
        return false;
    }
    let threshold = match parcel.membership {
        Membership::Normal => FREE_SHIPPING_THRESHOLD_NORMAL,
        Membership::Gold => FREE_SHIPPING_THRESHOLD_GOLD,
    };
    parcel.order_amount >= threshold
}

fn weight_fee(weight_grams: i64) -> i64 {
    if weight_grams <= LIGHT_PARCEL_MAX_GRAMS {
        return LIGHT_PARCEL_FEE;
    }
    if weight_grams <= STANDARD_PARCEL_MAX_GRAMS {
        return STANDARD_PARCEL_FEE;
    }
    let extra_kilograms = ceil_div(weight_grams - STANDARD_PARCEL_MAX_GRAMS, GRAMS_PER_KILOGRAM);
    STANDARD_PARCEL_FEE + extra_kilograms * FEE_PER_EXTRA_KILOGRAM
}

fn region_surcharge(region: Region) -> i64 {
    // 用 match 列出每個地區，之後多一個地區時編譯器會逼你決定它要不要加價
    match region {
        Region::MainIsland => 0,
        Region::OutlyingIsland => OUTLYING_ISLAND_SURCHARGE,
    }
}

fn cold_chain_fee(parcel: &Parcel) -> i64 {
    // 冷藏車是另外跟物流商計費的，所以免運也要收
    if !parcel.needs_cold_chain {
        return 0;
    }
    match parcel.region {
        Region::MainIsland => COLD_CHAIN_FEE_MAIN_ISLAND,
        Region::OutlyingIsland => COLD_CHAIN_FEE_OUTLYING_ISLAND,
    }
}

fn ceil_div(dividend: i64, divisor: i64) -> i64 {
    // i64::div_ceil 還沒穩定，只好自己寫。Rust 的 / 會往 0 截斷，
    // div_euclid 在除數為正時才跟 Python 的 // 一樣往下取整
    -(-dividend).div_euclid(divisor)
}
