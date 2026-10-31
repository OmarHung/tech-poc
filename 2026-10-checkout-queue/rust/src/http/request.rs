//! Strict request parsing: every way a request can be malformed is a 400 with the error envelope,
//! never axum's default plain-text 415/422 rejections.

use axum::body::Bytes;
use axum::extract::rejection::PathRejection;
use axum::extract::{FromRequest, Path, Request};
use axum::http::{HeaderMap, header};
use serde_json::{Map, Value};

use super::ApiError;
use crate::ids;

/// A JSON object body. Only `application/json` (or `*/*+json`) is accepted; values are never
/// coerced, so `"quantity": "1"` is a 400. Field names match case-insensitively, like ASP.NET's
/// web defaults that the .NET version uses.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonBody(Map<String, Value>);

impl<S: Send + Sync> FromRequest<S> for JsonBody {
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        if !has_json_content_type(request.headers()) {
            return Err(ApiError::bad_request("request body must be JSON"));
        }
        let body = Bytes::from_request(request, state)
            .await
            .map_err(|rejection| ApiError::bad_request(rejection.body_text()))?;
        Self::parse(&body)
    }
}

impl JsonBody {
    pub fn parse(body: &[u8]) -> Result<Self, ApiError> {
        match serde_json::from_slice(body) {
            Ok(Value::Object(fields)) => Ok(Self(fields)),
            Ok(Value::Null) => Err(ApiError::bad_request("request body is required")),
            Ok(_) => Err(ApiError::bad_request("request body must be a JSON object")),
            Err(error) => Err(ApiError::bad_request(format!("invalid JSON body: {error}"))),
        }
    }

    /// A string field; missing or `null` is `None`, any other type is a 400.
    pub fn string(&self, name: &str) -> Result<Option<&str>, ApiError> {
        match self.field(name) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value)),
            Some(_) => Err(wrong_type(name, "a string")),
        }
    }

    /// An integer field; missing or `null` is `None`, fractions and other types are a 400.
    pub fn integer(&self, name: &str) -> Result<Option<i64>, ApiError> {
        match self.field(name) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Number(number)) => number
                .as_i64()
                .map(Some)
                .ok_or_else(|| wrong_type(name, "an integer")),
            Some(_) => Err(wrong_type(name, "an integer")),
        }
    }

    fn field(&self, name: &str) -> Option<&Value> {
        self.0.get(name).or_else(|| {
            self.0
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value)
        })
    }
}

fn wrong_type(name: &str, expected: &str) -> ApiError {
    ApiError::bad_request(format!(
        "invalid JSON body at $.{name}: expected {expected}"
    ))
}

/// `application/json`, any `+json` media type, with or without parameters such as charset.
fn has_json_content_type(headers: &HeaderMap) -> bool {
    let Some(value) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let media_type = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json"
        || media_type
            .split_once('/')
            .is_some_and(|(kind, subtype)| !kind.is_empty() && subtype.ends_with("+json"))
}

/// Ticket and order ids are 32 lowercase hex characters; anything else is a 400, not a lookup.
pub fn valid_id(value: Option<&str>, name: &str) -> Result<String, ApiError> {
    match value {
        Some(id) if ids::is_valid(id) => Ok(id.to_owned()),
        _ => Err(ApiError::bad_request(format!(
            "{name} must be 32 lowercase hex characters"
        ))),
    }
}

/// The `{id}` segment of a path, validated like [`valid_id`].
pub fn path_id(path: Result<Path<String>, PathRejection>, name: &str) -> Result<String, ApiError> {
    let id = path.ok().map(|Path(id)| id);
    valid_id(id.as_deref(), name)
}

/// Stands in for the authenticated user: `^[A-Za-z0-9._@-]{1,64}$`, characters that cannot
/// reshape a Redis key.
pub fn valid_user_id(value: Option<&str>) -> Result<String, ApiError> {
    match value {
        Some(user_id)
            if (1..=64).contains(&user_id.len())
                && user_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._@-".contains(&b)) =>
        {
            Ok(user_id.to_owned())
        }
        _ => Err(ApiError::bad_request(
            "userId must be 1-64 letters, digits, '.', '_', '-' or '@'",
        )),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn headers(content_type: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(content_type).unwrap(),
        );
        headers
    }

    #[test]
    fn content_type_must_be_json() {
        for accepted in [
            "application/json",
            "application/json; charset=utf-8",
            "Application/JSON",
            "application/problem+json",
        ] {
            assert!(has_json_content_type(&headers(accepted)), "{accepted}");
        }
        for rejected in ["text/plain", "application/jsonp", "json", "/+json", ""] {
            assert!(!has_json_content_type(&headers(rejected)), "{rejected}");
        }
        assert!(!has_json_content_type(&HeaderMap::new()));
    }

    #[test]
    fn body_must_be_a_json_object() {
        for body in [
            "",
            "{not json",
            "null",
            "[]",
            "\"text\"",
            "42",
            "{} trailing",
        ] {
            assert!(JsonBody::parse(body.as_bytes()).is_err(), "{body:?}");
        }
        assert!(JsonBody::parse(b"{}").is_ok());
    }

    #[test]
    fn fields_are_strictly_typed() {
        let body =
            JsonBody::parse(br#"{"userId":"alice","quantity":2,"q2":"1","q3":1.5,"n":null}"#)
                .unwrap();

        assert_eq!(body.string("userId").unwrap(), Some("alice"));
        assert_eq!(body.string("missing").unwrap(), None);
        assert_eq!(body.string("n").unwrap(), None);
        assert!(body.string("quantity").is_err());
        assert_eq!(body.integer("quantity").unwrap(), Some(2));
        assert!(body.integer("q2").is_err());
        assert!(body.integer("q3").is_err());
        assert!(body.integer("userId").is_err());
    }

    #[test]
    fn field_names_match_case_insensitively_preferring_the_exact_name() {
        let loose = JsonBody::parse(br#"{"UserID":"alice"}"#).unwrap();
        let both = JsonBody::parse(br#"{"USERID":"other","userId":"exact"}"#).unwrap();

        assert_eq!(loose.string("userId").unwrap(), Some("alice"));
        assert_eq!(both.string("userId").unwrap(), Some("exact"));
    }

    #[test]
    fn ids_and_user_ids_are_validated() {
        let id = "0123456789abcdef0123456789abcdef";
        assert_eq!(valid_id(Some(id), "ticketId").unwrap(), id);
        assert!(valid_id(Some("ABC"), "ticketId").is_err());
        assert!(valid_id(None, "ticketId").is_err());

        for ok in ["alice", "a.b_c-d@e", &"a".repeat(64)] {
            assert!(valid_user_id(Some(ok)).is_ok(), "{ok}");
        }
        for bad in ["", "has space", "brace{", "ünï", &"a".repeat(65)] {
            assert!(valid_user_id(Some(bad)).is_err(), "{bad}");
        }
        assert!(valid_user_id(None).is_err());
    }
}
