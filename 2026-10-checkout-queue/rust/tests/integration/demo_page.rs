use axum::http::StatusCode;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use checkout_queue::config::Config;

use crate::support::TestApp;

fn nonce_in(policy: &str) -> &str {
    let start =
        policy.find("script-src 'nonce-").expect("a script nonce") + "script-src 'nonce-".len();
    let length = policy[start..].find('\'').expect("closing quote");
    &policy[start..start + length]
}

pub async fn demo_page_is_served_at_root_with_a_fresh_script_nonce_each_time() {
    let app = TestApp::start().await;

    let first = app.get("/").await;
    let second = app.get("/").await;

    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(first.header("content-type"), "text/html; charset=utf-8");
    assert_eq!(first.header("x-content-type-options"), "nosniff");
    assert_eq!(first.header("x-served-by"), "test-instance");
    let policy = first.header("content-security-policy");
    let nonce = nonce_in(policy);
    assert_eq!(STANDARD.decode(nonce).unwrap().len(), 16);
    assert!(policy.contains("connect-src 'self'"), "{policy}");
    assert!(
        policy.contains(&format!("style-src 'nonce-{nonce}'")),
        "{policy}"
    );
    assert!(policy.starts_with("default-src 'none'") && policy.contains("frame-ancestors 'none'"));
    assert!(!policy.contains("unsafe-inline"), "{policy}");
    let html = first.text();
    assert!(html.contains(&format!("<script nonce=\"{nonce}\">")));
    assert!(html.contains(&format!("<style nonce=\"{nonce}\">")));
    assert!(!html.contains("__CSP_NONCE__"));
    assert_ne!(policy, second.header("content-security-policy"));
}

pub async fn admin_config_shows_the_sale_settings_but_not_where_redis_is() {
    let app = TestApp::start_with(|c| Config { max_active: 7, ..c }).await;

    let response = app.get("/admin/config").await;
    let body = response.json();

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(body["instance"], "test-instance");
    assert!(body["saleId"].as_str().unwrap().starts_with('t'));
    assert_eq!(body["maxActive"], 7);
    assert_eq!(body["maxPerOrder"], 2);
    assert_eq!(body["admissionWindowMs"], 60_000);
    assert_eq!(body["paymentWindowMs"], 120_000);
    assert_eq!(body["heartbeatTimeoutMs"], 15_000);
    assert_eq!(body["pollIntervalMs"], 1_000);
    assert_eq!(body["tickMs"], 200);
    assert!(!response.text().to_lowercase().contains("redis"), "{body}");
}
