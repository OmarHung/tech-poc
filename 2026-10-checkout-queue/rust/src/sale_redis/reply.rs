//! A script's reply: an outcome word, then name/value pairs (`[outcome, name1, value1, ...]`).

use std::collections::HashMap;

use redis::Value;

use super::StoreError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptReply {
    pub script: &'static str,
    pub outcome: String,
    fields: HashMap<String, String>,
}

impl ScriptReply {
    pub fn parse(script: &'static str, value: &Value) -> Result<Self, StoreError> {
        let Value::Array(items) = value else {
            return Err(StoreError::Unexpected(format!(
                "{script}.lua replied {value:?}, not an array"
            )));
        };
        let texts: Vec<String> = items
            .iter()
            .map(|item| text(script, item))
            .collect::<Result<_, _>>()?;
        let Some((outcome, pairs)) = texts.split_first() else {
            return Err(StoreError::Unexpected(format!(
                "{script}.lua replied an empty array"
            )));
        };
        let fields = pairs
            .as_chunks::<2>()
            .0
            .iter()
            .map(|[name, value]| (name.clone(), value.clone()))
            .collect();
        Ok(Self {
            script,
            outcome: outcome.clone(),
            fields,
        })
    }

    /// For scripts that return a list of replies, each shaped like a single script reply.
    pub fn parse_rows(script: &'static str, value: &Value) -> Result<Vec<Self>, StoreError> {
        match value {
            Value::Array(rows) => rows.iter().map(|row| Self::parse(script, row)).collect(),
            other => Err(StoreError::Unexpected(format!(
                "{script}.lua replied {other:?}, not a list of rows"
            ))),
        }
    }

    pub fn find(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }

    pub fn get(&self, name: &str) -> Result<&str, StoreError> {
        self.find(name).ok_or_else(|| {
            StoreError::Unexpected(format!(
                "{}.lua reply '{}' is missing '{name}'",
                self.script, self.outcome
            ))
        })
    }

    pub fn int(&self, name: &str) -> Result<i64, StoreError> {
        self.number(name, self.get(name)?)
    }

    pub fn optional_int(&self, name: &str) -> Result<Option<i64>, StoreError> {
        self.find(name)
            .map(|value| self.number(name, value))
            .transpose()
    }

    /// For outcomes a caller does not handle: these mean the script and the Rust side disagree.
    pub fn unexpected(&self) -> StoreError {
        StoreError::Unexpected(format!(
            "{}.lua returned unexpected outcome '{}'",
            self.script, self.outcome
        ))
    }

    fn number(&self, name: &str, value: &str) -> Result<i64, StoreError> {
        value.parse().map_err(|_| {
            StoreError::Unexpected(format!(
                "{}.lua field '{name}' is not an integer: '{value}'",
                self.script
            ))
        })
    }
}

/// Lua strings arrive as bulk strings, Lua numbers as integers.
fn text(script: &str, value: &Value) -> Result<String, StoreError> {
    match value {
        Value::BulkString(bytes) => String::from_utf8(bytes.clone())
            .map_err(|_| StoreError::Unexpected(format!("{script}.lua replied non-UTF-8 text"))),
        Value::SimpleString(text) => Ok(text.clone()),
        Value::Int(number) => Ok(number.to_string()),
        other => Err(StoreError::Unexpected(format!(
            "{script}.lua replied {other:?} inside its reply"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(text: &str) -> Value {
        Value::BulkString(text.as_bytes().to_vec())
    }

    #[test]
    fn parses_outcome_and_fields_of_either_type() {
        let value = Value::Array(vec![
            bulk("ok"),
            bulk("seq"),
            Value::Int(7),
            bulk("status"),
            Value::SimpleString("waiting".to_owned()),
        ]);

        let reply = ScriptReply::parse("ticket", &value).unwrap();

        assert_eq!(reply.outcome, "ok");
        assert_eq!(reply.int("seq").unwrap(), 7);
        assert_eq!(reply.get("status").unwrap(), "waiting");
        assert_eq!(reply.find("position"), None);
        assert_eq!(reply.optional_int("position").unwrap(), None);
    }

    #[test]
    fn parses_rows() {
        let value = Value::Array(vec![
            Value::Array(vec![bulk("ticket"), bulk("ticketId"), bulk("a")]),
            Value::Array(vec![bulk("order"), bulk("orderId"), bulk("b")]),
        ]);

        let rows = ScriptReply::parse_rows("tickets", &value).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(
            (rows[1].outcome.as_str(), rows[1].find("orderId")),
            ("order", Some("b"))
        );
        assert!(ScriptReply::parse_rows("tickets", &Value::Int(1)).is_err());
    }

    #[test]
    fn shapes_the_scripts_never_send_are_bugs() {
        let cases = [
            Value::Nil,
            Value::Array(vec![]),
            Value::Array(vec![bulk("ok"), Value::Nil]),
            Value::Array(vec![Value::BulkString(vec![0xff])]),
        ];
        for value in cases {
            let error = ScriptReply::parse("ticket", &value).unwrap_err();

            assert!(matches!(error, StoreError::Unexpected(_)), "{value:?}");
        }
    }

    #[test]
    fn missing_or_malformed_fields_are_bugs() {
        let reply = ScriptReply::parse(
            "order",
            &Value::Array(vec![bulk("ok"), bulk("quantity"), bulk("two")]),
        )
        .unwrap();

        assert!(
            reply
                .get("orderId")
                .unwrap_err()
                .to_string()
                .contains("missing 'orderId'")
        );
        assert!(
            reply
                .int("quantity")
                .unwrap_err()
                .to_string()
                .contains("not an integer")
        );
        assert!(
            reply
                .unexpected()
                .to_string()
                .contains("unexpected outcome 'ok'")
        );
    }
}
