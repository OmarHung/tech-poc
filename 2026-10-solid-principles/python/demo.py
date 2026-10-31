"""SOLID 五個原則各用一個電商結帳的小情境，把「違反版」和「遵守版」的差別印出來。

輸出只用型別名稱（三種語言都一樣），不印函式名稱（各語言的命名慣例不同），
所以 .NET、Rust 版本的輸出可以和這裡逐字相同。
"""

from collections.abc import Callable
from dataclasses import dataclass

from dip import CheckoutService, LegacyCheckoutService, RecordingNotifier, SmtpMailer
from isp import CashOnDelivery, LegacyCashOnDelivery, Refundable
from lsp import FixedAmountOff, UncappedAmountOff, contract_violations
from ocp import (
    DiscountRule,
    NoDiscount,
    PercentOff,
    legacy_total_after_discount,
    total_after_discount,
)
from srp import InvoiceCalculator, LegacyOrder, LineItem, ShippingPolicy

CUSTOMER_EMAIL = "alice@example.com"


@dataclass(frozen=True)
class SpendThresholdOff:
    """新需求：滿 threshold 折 amount。寫在示範程式裡，ocp.py 一行都沒改。"""

    threshold: int
    amount: int

    def discount(self, subtotal: int) -> int:
        return self.amount if subtotal >= self.threshold else 0


def attempt(action: Callable[[], object]) -> str:
    try:
        return str(action())
    except (ValueError, NotImplementedError, ConnectionError) as error:
        return f"錯誤：{error}"


def yes_no(flag: bool) -> str:
    return "是" if flag else "否"


def show_srp() -> None:
    print("\n=== S：單一職責原則（SRP）===")
    print(
        "  情境：訂單未稅 960 元。財務要求發票改成含 5% 稅；物流規則沒變：未稅滿 1000 元免運"
    )
    items = (LineItem("滑鼠", 480, 2),)
    legacy = LegacyOrder(items)
    print(
        f"  [違反] LegacyOrder：發票 {legacy.invoice_amount()} 元、"
        f"免運：{yes_no(legacy.ships_free())}  ← 財務的改動連帶改到物流"
    )
    print(
        f"  [遵守] InvoiceCalculator：發票 {InvoiceCalculator().amount(items)} 元；"
        f"ShippingPolicy：免運：{yes_no(ShippingPolicy().ships_free(items))}"
    )


def show_ocp() -> None:
    print("\n=== O：開放封閉原則（OCP）===")
    print(
        "  情境：小計 1200 元。原本有 VIP 九折、員工八折，行銷部新增「滿 1000 折 100」"
    )
    vip = legacy_total_after_discount(1200, "vip")
    threshold = attempt(lambda: legacy_total_after_discount(1200, "threshold"))
    print(f"  [違反] 套 VIP → {vip} 元")
    print(f"  [違反] 套滿千折百 → {threshold}  ← 只能回去改這串判斷")
    percent = total_after_discount(1200, PercentOff(10))
    spend = total_after_discount(1200, SpendThresholdOff(1000, 100))
    print(f"  [遵守] 套 PercentOff(10) → {percent} 元")
    print(
        f"  [遵守] 套 SpendThresholdOff(1000, 100) → {spend} 元"
        "  ← 新類別寫在示範程式裡，ocp 模組沒改"
    )


def show_lsp() -> None:
    print("\n=== L：里氏替換原則（LSP）===")
    print("  情境：小計 150 元的訂單，套用「折 200 元」")
    for label, rule in (
        ("[違反] UncappedAmountOff(200)", UncappedAmountOff(200)),
        ("[遵守] FixedAmountOff(200)", FixedAmountOff(200)),
    ):
        violations = "、".join(map(str, contract_violations(rule))) or "無"
        total = total_after_discount(150, rule)
        print(f"  {label} → 總額 {total} 元；違反契約的小計：{violations}")

    rules: tuple[tuple[str, DiscountRule], ...] = (
        ("NoDiscount", NoDiscount()),
        ("PercentOff(10)", PercentOff(10)),
        ("PercentOff(20)", PercentOff(20)),
        ("SpendThresholdOff(1000, 100)", SpendThresholdOff(1000, 100)),
        ("FixedAmountOff(200)", FixedAmountOff(200)),
    )
    passed = all(not contract_violations(rule) for _, rule in rules)
    names = "、".join(name for name, _ in rules)
    print(f"  契約檢查 {names}：{'全部通過' if passed else '有規則違反契約'}")


def show_isp() -> None:
    print("\n=== I：介面隔離原則（ISP）===")
    print("  情境：貨到付款能收款，但不能線上退款，也沒有付款資訊可以記住")
    legacy = LegacyCashOnDelivery()
    print("  [違反] LegacyCashOnDelivery 被迫實作肥介面的 3 個方法")
    print(f"    收款 300 元 → {attempt(lambda: legacy.charge(300))}")
    print(f"    退款 300 元 → {attempt(lambda: legacy.refund(300))}")
    print(f"    記住付款資訊 → {attempt(legacy.remember)}")

    cash = CashOnDelivery()
    print("  [遵守] CashOnDelivery 只實作收款介面")
    print(f"    收款 300 元 → {attempt(lambda: cash.charge(300))}")
    refundable = isinstance(cash, Refundable)
    print(
        f"    能退款嗎 → {yes_no(refundable)}：沒有實作退款介面，退款流程在型別檢查時就收不了它"
    )


def show_dip() -> None:
    print("\n=== D：依賴反轉原則（DIP）===")
    print(f"  情境：結帳後寄通知給 {CUSTOMER_EMAIL}。這個環境沒有 SMTP 伺服器")
    legacy = LegacyCheckoutService()
    print("  [違反] LegacyCheckoutService 自己建立 SmtpMailer")
    print(
        f"    結帳 1080 元 → {attempt(lambda: legacy.checkout(CUSTOMER_EMAIL, 1080))}"
    )

    print("  [遵守] CheckoutService 只依賴通知介面，由外面注入")
    recorder = RecordingNotifier()
    receipt = attempt(lambda: CheckoutService(recorder).checkout(CUSTOMER_EMAIL, 1080))
    print(
        f"    注入 RecordingNotifier：結帳 1080 元 → {receipt}；"
        f"記下 {len(recorder.sent)} 則通知給 {recorder.sent[0][0]}"
    )
    smtp = attempt(lambda: CheckoutService(SmtpMailer()).checkout(CUSTOMER_EMAIL, 1080))
    print(f"    注入 SmtpMailer：結帳 1080 元 → {smtp}")


if __name__ == "__main__":
    show_srp()
    show_ocp()
    show_lsp()
    show_isp()
    show_dip()
