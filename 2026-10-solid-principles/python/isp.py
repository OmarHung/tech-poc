"""I：介面隔離原則（Interface Segregation Principle）。

不要逼實作者依賴它用不到的方法。介面太肥，實作者只好用「丟例外」交差，
呼叫端從型別上看不出哪些方法其實不能用，等到執行才爆炸。

情境：貨到付款能收款，但不能線上退款，也沒有付款資訊可以記住給下次用。
"""

from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable

# --- 違反：一個肥介面，每種付款方式都得實作三個方法 ---


class LegacyPaymentMethod(ABC):
    @abstractmethod
    def charge(self, amount: int) -> str: ...

    @abstractmethod
    def refund(self, amount: int) -> str: ...

    @abstractmethod
    def remember(self) -> str:
        """記住付款資訊，下次一鍵付款。"""


class LegacyCreditCard(LegacyPaymentMethod):
    def charge(self, amount: int) -> str:
        return f"信用卡扣款 {amount} 元"

    def refund(self, amount: int) -> str:
        return f"信用卡退款 {amount} 元"

    def remember(self) -> str:
        return "已記住這張信用卡"


class LegacyCashOnDelivery(LegacyPaymentMethod):
    def charge(self, amount: int) -> str:
        return f"送達時收現 {amount} 元"

    def refund(self, amount: int) -> str:
        raise NotImplementedError("貨到付款不能線上退款")

    def remember(self) -> str:
        raise NotImplementedError("貨到付款沒有付款資訊可以記住")


# --- 遵守：一種能力一個介面，付款方式做得到哪些就實作哪些 ---


@runtime_checkable
class Chargeable(Protocol):
    def charge(self, amount: int) -> str: ...


@runtime_checkable
class Refundable(Protocol):
    def refund(self, amount: int) -> str: ...


@runtime_checkable
class Rememberable(Protocol):
    def remember(self) -> str: ...


class CreditCard:
    def charge(self, amount: int) -> str:
        return f"信用卡扣款 {amount} 元"

    def refund(self, amount: int) -> str:
        return f"信用卡退款 {amount} 元"

    def remember(self) -> str:
        return "已記住這張信用卡"


class CashOnDelivery:
    def charge(self, amount: int) -> str:
        return f"送達時收現 {amount} 元"


def refund_order(method: Refundable, amount: int) -> str:
    """退款流程只收 Refundable。傳 CashOnDelivery 進來，mypy 這類型別檢查工具會擋下。"""
    return method.refund(amount)
