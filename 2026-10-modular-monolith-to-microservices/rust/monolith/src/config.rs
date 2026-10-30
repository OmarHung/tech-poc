use std::env;
use std::str::FromStr;

const DEFAULT_PORT: &str = "8081";
const DEFAULT_CATALOG_URL: &str = "http://127.0.0.1:8082";

/// Where the monolith's catalog lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogMode {
    /// Catalog module runs inside the monolith process (before extraction).
    InProcess,
    /// Catalog lives in catalog-service and is called over HTTP (after extraction).
    Remote,
}

impl CatalogMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InProcess => "inprocess",
            Self::Remote => "remote",
        }
    }
}

impl FromStr for CatalogMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "inprocess" => Ok(Self::InProcess),
            "remote" => Ok(Self::Remote),
            other => Err(format!(
                "CATALOG_MODE must be 'inprocess' or 'remote', got '{other}'"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub catalog_mode: CatalogMode,
    pub catalog_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let port = env_or("PORT", DEFAULT_PORT);
        Ok(Self {
            port: port
                .parse()
                .map_err(|_| format!("PORT must be between 1 and 65535, got '{port}'"))?,
            catalog_mode: env_or("CATALOG_MODE", "inprocess").parse()?,
            catalog_url: env_or("CATALOG_URL", DEFAULT_CATALOG_URL),
        })
    }
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}
