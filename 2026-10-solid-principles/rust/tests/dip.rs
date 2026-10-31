use solid_principles::dip::{
    CheckoutService, LegacyCheckoutService, RecordingNotifier, SmtpMailer,
};

#[test]
fn legacy_checkout_cannot_run_without_an_smtp_server() {
    let error = LegacyCheckoutService::new()
        .checkout("alice@example.com", 1080)
        .unwrap_err();

    assert_eq!(error.to_string(), "連不到 smtp.example.com:25");
}

#[test]
fn checkout_with_a_fake_notifier_needs_no_smtp_server() {
    let notifier = RecordingNotifier::default();

    let receipt = CheckoutService::new(&notifier).checkout("alice@example.com", 1080);

    assert_eq!(receipt, Ok("已收款 1080 元".to_owned()));
    assert_eq!(
        notifier.sent(),
        vec![("alice@example.com".to_owned(), "已收款 1080 元".to_owned())]
    );
}

#[test]
fn checkout_uses_whatever_notifier_it_is_given() {
    let error = CheckoutService::new(&SmtpMailer)
        .checkout("alice@example.com", 1080)
        .unwrap_err();

    assert_eq!(error.to_string(), "連不到 smtp.example.com:25");
}
