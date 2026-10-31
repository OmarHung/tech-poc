from pathlib import Path

import pytest
from mypy import api as mypy_api

from isp import (
    CashOnDelivery,
    Chargeable,
    CreditCard,
    LegacyCashOnDelivery,
    LegacyCreditCard,
    Refundable,
    Rememberable,
    refund_order,
)


def test_legacy_credit_card_supports_every_method():
    card = LegacyCreditCard()

    assert card.charge(300) == "信用卡扣款 300 元"
    assert card.refund(300) == "信用卡退款 300 元"
    assert card.remember() == "已記住這張信用卡"


def test_legacy_cash_on_delivery_charges():
    assert LegacyCashOnDelivery().charge(300) == "送達時收現 300 元"


def test_legacy_cash_on_delivery_is_forced_to_stub_two_methods():
    cash = LegacyCashOnDelivery()

    with pytest.raises(NotImplementedError, match="不能線上退款"):
        cash.refund(300)
    with pytest.raises(NotImplementedError, match="沒有付款資訊可以記住"):
        cash.remember()


def test_cash_on_delivery_only_implements_charging():
    cash = CashOnDelivery()

    assert isinstance(cash, Chargeable)
    assert not isinstance(cash, Refundable)
    assert not isinstance(cash, Rememberable)
    assert cash.charge(300) == "送達時收現 300 元"


def test_credit_card_implements_every_role():
    card = CreditCard()

    assert isinstance(card, Chargeable)
    assert isinstance(card, Refundable)
    assert isinstance(card, Rememberable)
    assert card.charge(300) == "信用卡扣款 300 元"
    assert card.remember() == "已記住這張信用卡"


def test_refund_order_refunds_a_refundable_method():
    assert refund_order(CreditCard(), 300) == "信用卡退款 300 元"


def _type_check(snippet: str, tmp_path: Path, monkeypatch) -> tuple[str, int]:
    """用 mypy 檢查一小段程式，回傳 (輸出, 結束碼)。模組從這個測試檔所在的資料夾找。"""
    monkeypatch.setenv("MYPYPATH", str(Path(__file__).parent))
    stdout, stderr, status = mypy_api.run(["--cache-dir", str(tmp_path), "-c", snippet])
    return stdout + stderr, status


def test_type_checker_rejects_refunding_cash_on_delivery(tmp_path, monkeypatch):
    snippet = "from isp import CashOnDelivery, refund_order\nrefund_order(CashOnDelivery(), 300)\n"

    output, status = _type_check(snippet, tmp_path, monkeypatch)

    assert status == 1, output
    assert 'incompatible type "CashOnDelivery"; expected "Refundable"' in output


def test_type_checker_accepts_refunding_credit_card(tmp_path, monkeypatch):
    snippet = (
        "from isp import CreditCard, refund_order\nrefund_order(CreditCard(), 300)\n"
    )

    output, status = _type_check(snippet, tmp_path, monkeypatch)

    assert status == 0, output
