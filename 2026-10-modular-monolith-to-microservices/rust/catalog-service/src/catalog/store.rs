use std::collections::BTreeMap;
use std::sync::RwLock;

use async_trait::async_trait;

use super::{CatalogError, CatalogPort, Product, seed_products, validate_quantity};

const POISONED: &str = "catalog lock poisoned";

pub struct InMemoryCatalog {
    products: RwLock<BTreeMap<u64, Product>>,
}

impl InMemoryCatalog {
    pub fn new(products: Vec<Product>) -> Self {
        let by_id = products.into_iter().map(|p| (p.id, p)).collect();
        Self {
            products: RwLock::new(by_id),
        }
    }

    pub fn seeded() -> Self {
        Self::new(seed_products())
    }
}

#[async_trait]
impl CatalogPort for InMemoryCatalog {
    async fn list(&self) -> Result<Vec<Product>, CatalogError> {
        Ok(self
            .products
            .read()
            .expect(POISONED)
            .values()
            .cloned()
            .collect())
    }

    async fn get(&self, id: u64) -> Result<Product, CatalogError> {
        let products = self.products.read().expect(POISONED);
        products
            .get(&id)
            .cloned()
            .ok_or_else(|| CatalogError::product_not_found(id))
    }

    async fn reserve(&self, id: u64, quantity: u64) -> Result<Product, CatalogError> {
        validate_quantity(quantity)?;
        let mut products = self.products.write().expect(POISONED);
        let current = products
            .get(&id)
            .ok_or_else(|| CatalogError::product_not_found(id))?;
        if current.stock < quantity {
            return Err(CatalogError::insufficient_stock(
                id,
                quantity,
                current.stock,
            ));
        }
        let updated = Product {
            stock: current.stock - quantity,
            ..current.clone()
        };
        products.insert(id, updated.clone());
        Ok(updated)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn catalog_with_stock(stock: u64) -> InMemoryCatalog {
        InMemoryCatalog::new(vec![Product {
            id: 1,
            name: "Keyboard".into(),
            price: 100,
            stock,
        }])
    }

    #[tokio::test]
    async fn list_returns_products_sorted_by_id() {
        let ids: Vec<u64> = InMemoryCatalog::seeded()
            .list()
            .await
            .unwrap()
            .iter()
            .map(|p| p.id)
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn get_unknown_product_is_not_found() {
        let error = catalog_with_stock(5).get(99).await.unwrap_err();
        assert_eq!(error, CatalogError::NotFound("product 99 not found".into()));
    }

    #[tokio::test]
    async fn reserve_decrements_stock() {
        let catalog = catalog_with_stock(5);
        let updated = catalog.reserve(1, 2).await.unwrap();
        assert_eq!(updated.stock, 3);
        assert_eq!(catalog.get(1).await.unwrap().stock, 3);
    }

    #[tokio::test]
    async fn reserve_more_than_stock_is_rejected_without_change() {
        let catalog = catalog_with_stock(1);
        let error = catalog.reserve(1, 2).await.unwrap_err();
        assert!(matches!(error, CatalogError::InsufficientStock(_)));
        assert_eq!(catalog.get(1).await.unwrap().stock, 1);
    }

    #[tokio::test]
    async fn reserve_rejects_invalid_quantity() {
        let error = catalog_with_stock(5).reserve(1, 0).await.unwrap_err();
        assert!(matches!(error, CatalogError::InvalidInput(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reservations_never_oversell() {
        let catalog = Arc::new(catalog_with_stock(10));
        let attempts = (0..50).map(|_| {
            let catalog = Arc::clone(&catalog);
            tokio::spawn(async move { catalog.reserve(1, 1).await.is_ok() })
        });
        let mut succeeded = 0;
        for attempt in attempts {
            succeeded += usize::from(attempt.await.unwrap());
        }
        assert_eq!(succeeded, 10);
        assert_eq!(catalog.get(1).await.unwrap().stock, 0);
    }
}
