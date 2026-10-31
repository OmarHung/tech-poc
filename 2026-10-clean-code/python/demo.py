"""先拿幾筆訂單讓舊版 calc() 和重構版 shipping_fee() 各算一次，
再窮舉 728 組輸入，確認重構沒有改變行為。
"""

from characterization import compare
from legacy import calc
from shipping import InvalidParcelError, Membership, Parcel, Region, shipping_fee

REGION_LABELS = {Region.MAIN_ISLAND: "本島", Region.OUTLYING_ISLAND: "離島"}
MEMBERSHIP_LABELS = {Membership.NORMAL: "一般會員", Membership.GOLD: "金卡會員"}

SAMPLES = (
    Parcel(2500, Region.MAIN_ISLAND, Membership.NORMAL, 800, False),
    Parcel(800, Region.MAIN_ISLAND, Membership.NORMAL, 1000, False),
    Parcel(2500, Region.MAIN_ISLAND, Membership.GOLD, 600, True),
    Parcel(7200, Region.OUTLYING_ISLAND, Membership.GOLD, 3000, True),
    Parcel(0, Region.MAIN_ISLAND, Membership.NORMAL, 800, False),
)


def describe(parcel: Parcel) -> str:
    temperature = "冷藏" if parcel.needs_cold_chain else "常溫"
    return (
        f"{REGION_LABELS[parcel.region]}、{MEMBERSHIP_LABELS[parcel.membership]}、"
        f"訂單 {parcel.order_amount} 元、{parcel.weight_grams} 公克、{temperature}"
    )


def refactored_result(parcel: Parcel) -> str:
    try:
        return str(shipping_fee(parcel))
    except InvalidParcelError as error:
        return f"錯誤：{error}"


def show_samples() -> None:
    print("\n=== 同一筆訂單，舊版和重構版各算一次運費 ===")
    for parcel in SAMPLES:
        old = calc(
            parcel.weight_grams,
            parcel.region.value,
            parcel.membership.value,
            parcel.order_amount,
            parcel.needs_cold_chain,
        )
        print(f"  {describe(parcel)} → 舊版 {old}｜重構版 {refactored_result(parcel)}")


def show_characterization() -> None:
    result = compare()
    print(f"\n=== Characterization test：窮舉 {result.total} 組輸入 ===")
    agreed_fees = result.total - result.invalid - len(result.mismatches)
    print(f"  有效輸入 {agreed_fees} 組：兩版運費相同")
    print(f"  無效輸入 {result.invalid} 組：舊版回 -1，重構版回報明確的錯誤")
    print(f"  結果不同：{len(result.mismatches)} 組")


if __name__ == "__main__":
    show_samples()
    show_characterization()
