//! HTTP plumbing: error mapping, served-by header, strict request parsing.

mod error;
mod request;

use axum::extract::State;
use axum::http::{HeaderName, HeaderValue};
use axum::response::Response;

pub use error::{ApiError, ErrorBody};
pub use request::{JsonBody, path_id, valid_id, valid_user_id};

pub const SERVED_BY: HeaderName = HeaderName::from_static("x-served-by");

/// Tags every response with X-Served-By so callers can tell which instance answered.
pub async fn served_by(State(name): State<HeaderValue>, mut response: Response) -> Response {
    response.headers_mut().insert(SERVED_BY, name);
    response
}
