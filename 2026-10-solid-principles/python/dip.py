"""D：依賴反轉原則（Dependency Inversion Principle）。

高層的業務流程不該依賴低層的實作細節，兩者都該依賴抽象。

情境：結帳後要通知客人。這個 PoC 沒有 SMTP 伺服器，真的寄信一定失敗。
"""

from dataclasses import dataclass, field
from typing import Protocol


class SmtpMailer:
    """低層細節：用 SMTP 寄信。模擬的環境沒有 SMTP 伺服器，所以一定失敗。"""

    host = "smtp.example.com"

    def send(self, to: str, message: str) -> None:
        raise ConnectionError(f"連不到 {self.host}:25")


# --- 違反：結帳流程自己建立 SmtpMailer，外面換不掉 ---


class LegacyCheckoutService:
    def __init__(self) -> None:
        self._mailer = SmtpMailer()

    def checkout(self, email: str, amount: int) -> str:
        receipt = f"已收款 {amount} 元"
        self._mailer.send(email, receipt)
        return receipt


# --- 遵守：結帳流程只依賴 Notifier 介面，接哪一種由外面決定 ---


class Notifier(Protocol):
    def send(self, to: str, message: str) -> None: ...


@dataclass
class RecordingNotifier:
    """把通知記在記憶體裡，給測試和示範用。"""

    sent: list[tuple[str, str]] = field(default_factory=list)

    def send(self, to: str, message: str) -> None:
        self.sent.append((to, message))


class CheckoutService:
    def __init__(self, notifier: Notifier) -> None:
        self._notifier = notifier

    def checkout(self, email: str, amount: int) -> str:
        receipt = f"已收款 {amount} 元"
        self._notifier.send(email, receipt)
        return receipt
