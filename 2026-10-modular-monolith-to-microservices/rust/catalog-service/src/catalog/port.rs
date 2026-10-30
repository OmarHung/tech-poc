use std::sync::Arc;

use async_trait::async_trait;

use super::{CatalogError, Product};

/// Everything the rest of the system may ask of the catalog.
///
/// Orders depend on this trait only, which is the seam that lets the catalog
/// move out of process: swap the in-memory implementation for an HTTP client
/// and nothing else changes.
#[async_trait]
pub trait CatalogPort: Send + Sync {
    async fn list(&self) -> Result<Vec<Product>, CatalogError>;

    async fn get(&self, id: u64) -> Result<Product, CatalogError>;

    /// Atomically takes `quantity` units out of stock and returns the updated product.
    async fn reserve(&self, id: u64, quantity: u64) -> Result<Product, CatalogError>;
}

pub type SharedCatalog = Arc<dyn CatalogPort>;
