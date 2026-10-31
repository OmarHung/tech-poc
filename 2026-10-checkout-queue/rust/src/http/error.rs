use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::sale_redis::StoreError;

/// JSON body of every error response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// Safe to show the caller.
    pub error: String,
    /// What a client branches on (several different 409s need different handling).
    pub code: String,
}

/// An error that maps to an HTTP status and `{"error": "...", "code": "..."}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    pub fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    pub fn forbidden(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, code, message)
    }

    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    pub fn gone(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::GONE, code, message)
    }

    /// Redis is down or slow: tell the client to retry rather than give up.
    pub fn unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "storage unavailable, retry shortly",
        )
    }

    pub fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal error",
        )
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn code(&self) -> &str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Unavailable(error) => {
                tracing::warn!("redis unavailable while serving a request: {error}");
                Self::unavailable()
            }
            StoreError::Unexpected(bug) => {
                tracing::error!("{bug}");
                Self::internal()
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: self.message,
            code: self.code.to_owned(),
        };
        (self.status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_errors_become_503_for_outages_and_500_for_bugs() {
        let outage = StoreError::Unavailable(redis::RedisError::from(std::io::Error::from(
            std::io::ErrorKind::ConnectionRefused,
        )));
        let bug = StoreError::Unexpected("join.lua returned unexpected outcome 'maybe'".to_owned());

        let outage = ApiError::from(outage);
        let bug = ApiError::from(bug);

        assert_eq!(
            (outage.status(), outage.code(), outage.message()),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "storage unavailable, retry shortly"
            )
        );
        assert_eq!(
            (bug.status(), bug.code()),
            (StatusCode::INTERNAL_SERVER_ERROR, "internal")
        );
    }
}
