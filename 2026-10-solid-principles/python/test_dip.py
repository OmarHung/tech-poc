import pytest

from dip import CheckoutService, LegacyCheckoutService, RecordingNotifier, SmtpMailer


def test_legacy_checkout_cannot_run_without_an_smtp_server():
    with pytest.raises(ConnectionError, match="smtp.example.com:25"):
        LegacyCheckoutService().checkout("alice@example.com", 1080)


def test_checkout_with_a_fake_notifier_needs_no_smtp_server():
    notifier = RecordingNotifier()

    receipt = CheckoutService(notifier).checkout("alice@example.com", 1080)

    assert receipt == "已收款 1080 元"
    assert notifier.sent == [("alice@example.com", "已收款 1080 元")]


def test_checkout_uses_whatever_notifier_it_is_given():
    with pytest.raises(ConnectionError, match="smtp.example.com:25"):
        CheckoutService(SmtpMailer()).checkout("alice@example.com", 1080)
