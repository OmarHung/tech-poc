//! Characterization test（又叫 golden master）：不管規格寫什麼，只比對舊版和重構版
//! 在同一批輸入下的結果是不是一模一樣。
//!
//! 舊版用 -1 表示輸入無效，重構版改成回傳 `Err(InvalidParcel)`。比對時把這兩種視為同一個結果。

use crate::legacy::calc;
use crate::shipping::{Membership, Parcel, Region, shipping_fee};

// 每個邊界都取「剛好、少 1、多 1」
pub const WEIGHTS: [i64; 13] = [
    -1, 0, 1, 999, 1000, 1001, 4999, 5000, 5001, 5999, 6000, 6001, 12345,
];
pub const REGIONS: [&str; 2] = ["main", "outlying"];
pub const MEMBERSHIPS: [&str; 2] = ["normal", "gold"];
pub const ORDER_AMOUNTS: [i64; 7] = [-1, 0, 499, 500, 999, 1000, 5000];
pub const COLD_CHAIN: [bool; 2] = [false, true];

const LEGACY_ERROR_CODE: i64 = -1;

/// 舊版 calc() 的 5 個位置參數：重量、地區、會員等級、訂單金額、是否冷藏
pub type Case = (i64, &'static str, &'static str, i64, bool);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Fee(i64),
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Comparison {
    pub total: usize,
    /// 兩版都判定為無效輸入的組數
    pub invalid: usize,
    pub mismatches: Vec<Case>,
}

/// 窮舉所有維度的組合。跟 Python 的 itertools.product 一樣，前面的維度變得最慢
pub fn input_grid() -> impl Iterator<Item = Case> {
    WEIGHTS
        .into_iter()
        .flat_map(|weight| REGIONS.map(|region| (weight, region)))
        .flat_map(|(weight, region)| MEMBERSHIPS.map(|membership| (weight, region, membership)))
        .flat_map(|(weight, region, membership)| {
            ORDER_AMOUNTS.map(|amount| (weight, region, membership, amount))
        })
        .flat_map(|(weight, region, membership, amount)| {
            COLD_CHAIN.map(|cold| (weight, region, membership, amount, cold))
        })
}

/// 把舊版的參數轉成重構版的 Parcel。
/// 舊版的字串只認 "main" 和 "gold"，其他值一律當離島、一般會員；
/// 這裡只接受網格裡的四個字串，出現別的值代表網格寫錯了，直接 panic。
pub fn parcel_from_case(case: Case) -> Parcel {
    let (weight_grams, region, membership, order_amount, needs_cold_chain) = case;
    Parcel {
        weight_grams,
        region: match region {
            "main" => Region::MainIsland,
            "outlying" => Region::OutlyingIsland,
            other => panic!("網格裡不該出現的地區：{other}"),
        },
        membership: match membership {
            "normal" => Membership::Normal,
            "gold" => Membership::Gold,
            other => panic!("網格裡不該出現的會員等級：{other}"),
        },
        order_amount,
        needs_cold_chain,
    }
}

/// 反方向：把 Parcel 轉回舊版 calc() 吃的參數，demo 用它餵同一筆訂單給舊版
pub fn case_from_parcel(parcel: &Parcel) -> Case {
    let region = match parcel.region {
        Region::MainIsland => "main",
        Region::OutlyingIsland => "outlying",
    };
    let membership = match parcel.membership {
        Membership::Normal => "normal",
        Membership::Gold => "gold",
    };
    (
        parcel.weight_grams,
        region,
        membership,
        parcel.order_amount,
        parcel.needs_cold_chain,
    )
}

pub fn legacy_outcome(case: Case) -> Outcome {
    let (weight, region, membership, amount, cold) = case;
    match calc(weight, region, membership, amount, cold) {
        LEGACY_ERROR_CODE => Outcome::Invalid,
        fee => Outcome::Fee(fee),
    }
}

pub fn refactored_outcome(case: Case) -> Outcome {
    shipping_fee(&parcel_from_case(case)).map_or(Outcome::Invalid, Outcome::Fee)
}

/// 比對重構版和舊版
pub fn compare() -> Comparison {
    compare_with(refactored_outcome)
}

/// 拿 candidate 的結果去比對舊版。測試會傳入故意改壞的版本，確認比對真的抓得到差異。
pub fn compare_with(candidate: impl Fn(Case) -> Outcome) -> Comparison {
    let cases: Vec<Case> = input_grid().collect();
    let outcomes: Vec<(Outcome, Outcome)> = cases
        .iter()
        .map(|&case| (legacy_outcome(case), candidate(case)))
        .collect();
    Comparison {
        total: cases.len(),
        invalid: outcomes
            .iter()
            .filter(|outcome| **outcome == (Outcome::Invalid, Outcome::Invalid))
            .count(),
        mismatches: cases
            .iter()
            .zip(&outcomes)
            .filter(|(_, (old, new))| old != new)
            .map(|(&case, _)| case)
            .collect(),
    }
}
