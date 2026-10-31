//! Settings from environment-style lookups. Every error names the variable to fix.

use std::fmt;
use std::time::Duration;

use url::Url;

/// A setting that cannot be used. The message names the variable and the value it got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

/// Reads settings through `lookup`, so tests can pass a map instead of the real environment.
pub struct EnvReader<F> {
    lookup: F,
}

impl<F: Fn(&str) -> Option<String>> EnvReader<F> {
    pub fn new(lookup: F) -> Self {
        Self { lookup }
    }

    /// The value, or `fallback` when the variable is unset or empty.
    pub fn string(&self, name: &str, fallback: &str) -> String {
        self.raw(name).unwrap_or_else(|| fallback.to_owned())
    }

    pub fn port(&self, name: &str, fallback: u16) -> Result<u16, ConfigError> {
        let value = self.int(name, i64::from(fallback), 1, 65535)?;
        u16::try_from(value).map_err(|_| out_of_range(name, 1, 65535, &value.to_string()))
    }

    /// A plain decimal integer in `[min, max]`: no sign, no spaces, no separators.
    pub fn int(&self, name: &str, fallback: i64, min: i64, max: i64) -> Result<i64, ConfigError> {
        let Some(raw) = self.raw(name) else {
            return Ok(fallback);
        };
        raw.bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| raw.parse::<i64>().ok())
            .flatten()
            .filter(|value| (min..=max).contains(value))
            .ok_or_else(|| out_of_range(name, min, max, &raw))
    }

    pub fn millis(
        &self,
        name: &str,
        fallback: Duration,
        min_ms: i64,
        max_ms: i64,
    ) -> Result<Duration, ConfigError> {
        let fallback_ms = i64::try_from(fallback.as_millis()).unwrap_or(i64::MAX);
        let ms = self.int(name, fallback_ms, min_ms, max_ms)?;
        Ok(Duration::from_millis(ms.unsigned_abs()))
    }

    pub fn url(&self, name: &str, fallback: &Url) -> Result<Url, ConfigError> {
        match self.raw(name) {
            Some(raw) => parse_url(name, &raw),
            None => Ok(fallback.clone()),
        }
    }

    /// One of a fixed set of words, e.g. `ARBITER=witness|none`.
    pub fn choice<T: Copy>(
        &self,
        name: &str,
        fallback: T,
        options: &[(&str, T)],
    ) -> Result<T, ConfigError> {
        let Some(raw) = self.raw(name) else {
            return Ok(fallback);
        };
        options
            .iter()
            .find(|(word, _)| *word == raw)
            .map(|(_, value)| *value)
            .ok_or_else(|| {
                let words: Vec<&str> = options.iter().map(|(word, _)| *word).collect();
                ConfigError(format!(
                    "{name} must be one of {}, got '{raw}'",
                    words.join(", ")
                ))
            })
    }

    fn raw(&self, name: &str) -> Option<String> {
        (self.lookup)(name).filter(|value| !value.is_empty())
    }
}

/// An absolute http(s) URL.
pub fn parse_url(name: &str, raw: &str) -> Result<Url, ConfigError> {
    Url::parse(raw)
        .ok()
        .filter(|url| matches!(url.scheme(), "http" | "https") && url.has_host())
        .ok_or_else(|| ConfigError(format!("{name} must be an http(s) URL, got '{raw}'")))
}

fn out_of_range(name: &str, min: i64, max: i64, raw: &str) -> ConfigError {
    ConfigError(format!(
        "{name} must be an integer between {min} and {max}, got '{raw}'"
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn reader(pairs: &[(&str, &str)]) -> EnvReader<impl Fn(&str) -> Option<String>> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        EnvReader::new(move |name: &str| map.get(name).cloned())
    }

    #[test]
    fn unset_or_empty_values_fall_back() {
        let env = reader(&[("EMPTY", "")]);

        assert_eq!(env.string("EMPTY", "x"), "x");
        assert_eq!(env.string("MISSING", "y"), "y");
        assert_eq!(env.port("MISSING", 8101), Ok(8101));
        assert_eq!(
            env.millis("EMPTY", Duration::from_millis(500), 10, 1000),
            Ok(Duration::from_millis(500))
        );
    }

    #[test]
    fn integers_must_be_plain_digits_in_range() {
        let env = reader(&[
            ("OK", "070"),
            ("SIGN", "+5"),
            ("BIG", "99999999999999999999"),
            ("LOW", "0"),
        ]);

        assert_eq!(env.int("OK", 0, 1, 100), Ok(70));
        for name in ["SIGN", "BIG", "LOW"] {
            let error = env.int(name, 0, 1, 100).unwrap_err();
            assert!(error.0.starts_with(name), "{error}");
        }
        assert!(
            env.port("BIG", 1)
                .unwrap_err()
                .0
                .contains("between 1 and 65535")
        );
    }

    #[test]
    fn urls_must_be_absolute_http() {
        let env = reader(&[
            ("GOOD", "http://127.0.0.1:8102"),
            ("FTP", "ftp://host"),
            ("TEXT", "not a url"),
        ]);
        let fallback = Url::parse("http://fallback/").unwrap();

        assert_eq!(
            env.url("GOOD", &fallback).unwrap().as_str(),
            "http://127.0.0.1:8102/"
        );
        assert_eq!(env.url("MISSING", &fallback), Ok(fallback.clone()));
        assert!(
            env.url("FTP", &fallback)
                .unwrap_err()
                .0
                .contains("FTP must be an http(s) URL")
        );
        assert!(env.url("TEXT", &fallback).is_err());
    }

    #[test]
    fn choices_list_the_options_on_error() {
        let env = reader(&[("MODE", "semi"), ("OTHER", "b")]);
        let options = [("a", 1), ("b", 2)];

        assert_eq!(env.choice("OTHER", 1, &options), Ok(2));
        assert_eq!(env.choice("MISSING", 1, &options), Ok(1));
        assert_eq!(
            env.choice("MODE", 1, &options).unwrap_err().0,
            "MODE must be one of a, b, got 'semi'"
        );
    }
}
