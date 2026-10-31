//! 先拿幾筆訂單讓舊版 calc() 和重構版 shipping_fee() 各算一次，
//! 再窮舉 728 組輸入，確認重構沒有改變行為。

use clean_code::characterization::{case_from_parcel, compare};
use clean_code::legacy::calc;
use clean_code::shipping::{Membership, Parcel, Region, shipping_fee};

const SAMPLES: [Parcel; 5] = [
    Parcel {
        weight_grams: 2500,
        region: Region::MainIsland,
        membership: Membership::Normal,
        order_amount: 800,
        needs_cold_chain: false,
    },
    Parcel {
        weight_grams: 800,
        region: Region::MainIsland,
        membership: Membership::Normal,
        order_amount: 1000,
        needs_cold_chain: false,
    },
    Parcel {
        weight_grams: 2500,
        region: Region::MainIsland,
        membership: Membership::Gold,
        order_amount: 600,
        needs_cold_chain: true,
    },
    Parcel {
        weight_grams: 7200,
        region: Region::OutlyingIsland,
        membership: Membership::Gold,
        order_amount: 3000,
        needs_cold_chain: true,
    },
    Parcel {
        weight_grams: 0,
        region: Region::MainIsland,
        membership: Membership::Normal,
        order_amount: 800,
        needs_cold_chain: false,
    },
];

fn region_label(region: Region) -> &'static str {
    match region {
        Region::MainIsland => "本島",
        Region::OutlyingIsland => "離島",
    }
}

fn membership_label(membership: Membership) -> &'static str {
    match membership {
        Membership::Normal => "一般會員",
        Membership::Gold => "金卡會員",
    }
}

fn describe(parcel: &Parcel) -> String {
    let temperature = if parcel.needs_cold_chain {
        "冷藏"
    } else {
        "常溫"
    };
    format!(
        "{}、{}、訂單 {} 元、{} 公克、{temperature}",
        region_label(parcel.region),
        membership_label(parcel.membership),
        parcel.order_amount,
        parcel.weight_grams,
    )
}

fn refactored_result(parcel: &Parcel) -> String {
    match shipping_fee(parcel) {
        Ok(fee) => fee.to_string(),
        Err(error) => format!("錯誤：{error}"),
    }
}

fn show_samples() {
    println!("\n=== 同一筆訂單，舊版和重構版各算一次運費 ===");
    for parcel in &SAMPLES {
        let (weight, region, membership, amount, cold) = case_from_parcel(parcel);
        let old = calc(weight, region, membership, amount, cold);
        println!(
            "  {} → 舊版 {old}｜重構版 {}",
            describe(parcel),
            refactored_result(parcel)
        );
    }
}

fn show_characterization() {
    let result = compare();
    println!(
        "\n=== Characterization test：窮舉 {} 組輸入 ===",
        result.total
    );
    let agreed_fees = result.total - result.invalid - result.mismatches.len();
    println!("  有效輸入 {agreed_fees} 組：兩版運費相同");
    println!(
        "  無效輸入 {} 組：舊版回 -1，重構版回報明確的錯誤",
        result.invalid
    );
    println!("  結果不同：{} 組", result.mismatches.len());
}

fn main() {
    show_samples();
    show_characterization();
}
