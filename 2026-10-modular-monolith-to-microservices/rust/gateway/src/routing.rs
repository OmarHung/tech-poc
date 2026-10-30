use std::str::FromStr;

use serde::Serialize;

/// Which backend answers catalog paths (`/products` and below).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogRoute {
    Monolith,
    Service,
}

impl FromStr for CatalogRoute {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "monolith" => Ok(Self::Monolith),
            "service" => Ok(Self::Service),
            other => Err(format!(
                "catalog must be 'monolith' or 'service', got '{other}'"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Upstreams {
    pub monolith: String,
    pub catalog: String,
}

pub fn is_catalog_path(path: &str) -> bool {
    path == "/products" || path.starts_with("/products/")
}

/// Everything goes to the monolith except catalog paths once they are switched over.
pub fn resolve<'a>(path: &str, route: CatalogRoute, upstreams: &'a Upstreams) -> &'a str {
    match route {
        CatalogRoute::Service if is_catalog_path(path) => &upstreams.catalog,
        CatalogRoute::Service | CatalogRoute::Monolith => &upstreams.monolith,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upstreams() -> Upstreams {
        Upstreams {
            monolith: "http://mono".into(),
            catalog: "http://cat".into(),
        }
    }

    #[test]
    fn catalog_paths_follow_the_switch() {
        let upstreams = upstreams();
        for path in ["/products", "/products/1", "/products/1/reserve"] {
            assert_eq!(
                resolve(path, CatalogRoute::Monolith, &upstreams),
                "http://mono",
                "{path}"
            );
            assert_eq!(
                resolve(path, CatalogRoute::Service, &upstreams),
                "http://cat",
                "{path}"
            );
        }
    }

    #[test]
    fn other_paths_always_go_to_the_monolith() {
        let upstreams = upstreams();
        for path in ["/orders", "/orders/1", "/productsX", "/", "/health"] {
            assert_eq!(
                resolve(path, CatalogRoute::Service, &upstreams),
                "http://mono",
                "{path}"
            );
        }
    }

    #[test]
    fn parses_only_known_routes() {
        assert_eq!("service".parse(), Ok(CatalogRoute::Service));
        assert_eq!("monolith".parse(), Ok(CatalogRoute::Monolith));
        assert!("Service".parse::<CatalogRoute>().is_err());
    }
}
