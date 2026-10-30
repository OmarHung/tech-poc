//! Catalog module — **MIGRATED to `catalog-service` (已轉微服務)**.
//!
//! The data and business logic now live in `catalog-service`. This copy stays in its
//! original place so the before/after is easy to compare, and so `CATALOG_MODE=inprocess`
//! can roll back. Still in use after the migration:
//! - [`CatalogPort`]: the interface the orders module depends on
//! - [`HttpCatalogClient`]: added by the migration, calls catalog-service
//! - [`router`]: the `/products` routes, which now delegate through the HTTP client
//!
//! Everything else (`InMemoryCatalog`, seed data) only runs in `inprocess` mode and can be
//! deleted once rollback is no longer needed. See README.md in this folder.

mod error;
mod port;
mod product;
mod remote;
mod routes;
mod store;
mod validation;

pub use error::{CatalogError, ErrorBody};
pub use port::{CatalogPort, SharedCatalog};
pub use product::{Product, ReserveRequest, seed_products};
pub use remote::{HttpCatalogClient, REQUEST_TIMEOUT};
pub use routes::router;
pub use store::InMemoryCatalog;
pub use validation::{MAX_QUANTITY, parse_id, validate_id, validate_quantity};
