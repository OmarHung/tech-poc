//! Catalog module, moved here from the monolith's `modules/catalog`.
//!
//! The files are the same as the monolith's copy (which is now marked as migrated),
//! minus the HTTP client the monolith uses to call this service.

mod error;
mod port;
mod product;
mod routes;
mod store;
mod validation;

pub use error::{CatalogError, ErrorBody};
pub use port::{CatalogPort, SharedCatalog};
pub use product::{Product, ReserveRequest, seed_products};
pub use routes::router;
pub use store::InMemoryCatalog;
pub use validation::{MAX_QUANTITY, parse_id, validate_id, validate_quantity};
