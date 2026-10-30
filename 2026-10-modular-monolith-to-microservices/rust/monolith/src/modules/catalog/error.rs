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

/// Errors carry their message as text so they survive an HTTP hop unchanged:
/// the remote client turns a status code plus message back into the same variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    InvalidInput(String),
    NotFound(String),
    InsufficientStock(String),
    Unavailable(String),
}

impl CatalogError {
    pub fn product_not_found(id: u64) -> Self {
        Self::NotFound(format!("product {id} not found"))
    }

    pub fn insufficient_stock(id: u64, requested: u64, available: u64) -> Self {
        Self::InsufficientStock(format!(
            "insufficient stock for product {id}: requested {requested}, available {available}"
        ))
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::InvalidInput(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::InsufficientStock(_) => StatusCode::CONFLICT,
            Self::Unavailable(_) => StatusCode::BAD_GATEWAY,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::InvalidInput(m)
            | Self::NotFound(m)
            | Self::InsufficientStock(m)
            | Self::Unavailable(m) => m,
        }
    }
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for CatalogError {}

impl IntoResponse for CatalogError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: self.message().to_owned(),
        };
        (self.status(), Json(body)).into_response()
    }
}
