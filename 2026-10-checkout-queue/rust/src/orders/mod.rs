//! Checkout (reserve stock), simulated payment, and the release of unpaid orders.

mod routes;
mod store;
mod view;

pub use routes::routes;
pub use store::{CheckoutResult, OrderStore, PayResult};
pub use view::{OrderStatus, OrderView};
