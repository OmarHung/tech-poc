//! A one-file control page at `/` for trying the queue by hand (`../shared/demo/index.html`).
//! It only calls this instance's own API; open a second instance in another tab to see both
//! serve one sale.

use axum::http::header;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::ids;

const PAGE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../shared/demo/index.html"
));
const NONCE_PLACEHOLDER: &str = "__CSP_NONCE__";

/// Inline script and style run only with this request's nonce, so injected markup cannot run.
pub async fn page() -> Response {
    let nonce = STANDARD.encode(ids::random_bytes::<16>());
    let policy = format!(
        "default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; \
         connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    );
    let headers = [
        (header::CONTENT_SECURITY_POLICY, policy),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_owned()),
        (header::CONTENT_TYPE, "text/html; charset=utf-8".to_owned()),
    ];
    (headers, PAGE.replace(NONCE_PLACEHOLDER, &nonce)).into_response()
}
