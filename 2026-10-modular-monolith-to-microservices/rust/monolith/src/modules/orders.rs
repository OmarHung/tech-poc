//! Orders module. Stays in the monolith; depends on the catalog only through `CatalogPort`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::modules::catalog::{
    CatalogError, ErrorBody, SharedCatalog, parse_id, validate_id, validate_quantity,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

const POISONED: &str = "orders lock poisoned";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    pub id: u64,
    pub product_id: u64,
    /// Name and price are copied at order time, so later catalog changes
    /// (or the catalog being unreachable) never alter an existing order.
    pub product_name: String,
    pub unit_price: u64,
    pub quantity: u64,
    pub total: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderRequest {
    pub product_id: u64,
    pub quantity: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderError {
    Catalog(CatalogError),
    NotFound(u64),
}

impl From<CatalogError> for OrderError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl IntoResponse for OrderError {
    fn into_response(self) -> Response {
        match self {
            Self::Catalog(error) => error.into_response(),
            Self::NotFound(id) => {
                let body = ErrorBody {
                    error: format!("order {id} not found"),
                };
                (StatusCode::NOT_FOUND, Json(body)).into_response()
            }
        }
    }
}

pub struct OrderService {
    catalog: SharedCatalog,
    orders: RwLock<BTreeMap<u64, Order>>,
    next_id: AtomicU64,
}

impl OrderService {
    pub fn new(catalog: SharedCatalog) -> Self {
        Self {
            catalog,
            orders: RwLock::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    pub async fn place(&self, product_id: u64, quantity: u64) -> Result<Order, OrderError> {
        validate_id(product_id, "productId")?;
        validate_quantity(quantity)?;
        let product = self.catalog.reserve(product_id, quantity).await?;
        let order = Order {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            product_id,
            product_name: product.name,
            unit_price: product.price,
            quantity,
            total: product.price * quantity,
        };
        self.orders
            .write()
            .expect(POISONED)
            .insert(order.id, order.clone());
        Ok(order)
    }

    pub fn find(&self, id: u64) -> Result<Order, OrderError> {
        self.orders
            .read()
            .expect(POISONED)
            .get(&id)
            .cloned()
            .ok_or(OrderError::NotFound(id))
    }
}

pub fn router(service: Arc<OrderService>) -> Router {
    Router::new()
        .route("/orders", post(create_order))
        .route("/orders/{id}", get(get_order))
        .with_state(service)
}

async fn create_order(
    State(service): State<Arc<OrderService>>,
    payload: Result<Json<CreateOrderRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<Order>), OrderError> {
    let Json(request) =
        payload.map_err(|rejection| CatalogError::InvalidInput(rejection.body_text()))?;
    let order = service.place(request.product_id, request.quantity).await?;
    Ok((StatusCode::CREATED, Json(order)))
}

async fn get_order(
    State(service): State<Arc<OrderService>>,
    Path(raw_id): Path<String>,
) -> Result<Json<Order>, OrderError> {
    let id = parse_id(&raw_id, "order id")?;
    service.find(id).map(Json)
}

#[cfg(test)]
mod tests {
    use crate::modules::catalog::{InMemoryCatalog, Product};

    use super::*;

    fn service() -> OrderService {
        let product = Product {
            id: 1,
            name: "Keyboard".into(),
            price: 300,
            stock: 5,
        };
        OrderService::new(Arc::new(InMemoryCatalog::new(vec![product])))
    }

    #[tokio::test]
    async fn place_order_snapshots_product_and_computes_total() {
        let order = service().place(1, 2).await.unwrap();
        assert_eq!(order.product_name, "Keyboard");
        assert_eq!(order.unit_price, 300);
        assert_eq!(order.total, 600);
    }

    #[tokio::test]
    async fn placed_order_can_be_found() {
        let service = service();
        let order = service.place(1, 1).await.unwrap();
        assert_eq!(service.find(order.id), Ok(order));
    }

    #[tokio::test]
    async fn order_ids_are_sequential() {
        let service = service();
        let first = service.place(1, 1).await.unwrap();
        let second = service.place(1, 1).await.unwrap();
        assert_eq!(second.id, first.id + 1);
    }

    #[tokio::test]
    async fn catalog_errors_pass_through() {
        let error = service().place(1, 6).await.unwrap_err();
        assert!(matches!(
            error,
            OrderError::Catalog(CatalogError::InsufficientStock(_))
        ));
        let error = service().place(9, 1).await.unwrap_err();
        assert!(matches!(
            error,
            OrderError::Catalog(CatalogError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn invalid_input_is_rejected_before_calling_catalog() {
        let error = service().place(0, 1).await.unwrap_err();
        assert!(matches!(
            error,
            OrderError::Catalog(CatalogError::InvalidInput(_))
        ));
        let error = service().place(1, 0).await.unwrap_err();
        assert!(matches!(
            error,
            OrderError::Catalog(CatalogError::InvalidInput(_))
        ));
    }

    #[test]
    fn unknown_order_is_not_found() {
        assert_eq!(service().find(42), Err(OrderError::NotFound(42)));
    }
}
