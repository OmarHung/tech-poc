use std::fmt;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

/// JSON body of every error response: `{"error": "..."}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
}

/// An error that maps to an HTTP status. The message is safe to return to callers, except for
/// `Internal`, whose detail is logged and replaced by a generic message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    InvalidInput(String),
    NotFound(String),
    Conflict(String),
    /// The request was valid but this process cannot serve it right now; retrying (maybe elsewhere) can work.
    Unavailable(String),
    Internal(String),
}

impl ApiError {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::InvalidInput(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::InvalidInput(m)
            | Self::NotFound(m)
            | Self::Conflict(m)
            | Self::Unavailable(m)
            | Self::Internal(m) => m,
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let error = match &self {
            Self::Internal(detail) => {
                tracing::error!("Request failed: {detail}");
                "internal error".to_owned()
            }
            other => other.message().to_owned(),
        };
        (self.status(), Json(ErrorBody { error })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::*;

    async fn body_of(error: ApiError) -> (StatusCode, String) {
        let response = error.into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn errors_become_json_with_their_status() {
        assert_eq!(
            body_of(ApiError::InvalidInput("bad".into())).await,
            (StatusCode::BAD_REQUEST, r#"{"error":"bad"}"#.into())
        );
        assert_eq!(
            body_of(ApiError::NotFound("x".into())).await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            body_of(ApiError::Conflict("x".into())).await.0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            body_of(ApiError::Unavailable("not primary".into())).await,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                r#"{"error":"not primary"}"#.into()
            )
        );
    }

    #[tokio::test]
    async fn internal_details_are_not_returned() {
        let (status, body) = body_of(ApiError::Internal("disk full at /secret".into())).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body, r#"{"error":"internal error"}"#);
        assert_eq!(ApiError::Internal("d".into()).to_string(), "d");
    }
}
