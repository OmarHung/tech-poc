use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Product {
    pub id: u64,
    pub name: String,
    pub price: u64,
    pub stock: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReserveRequest {
    pub quantity: u64,
}

/// Same seed data in every language, so the verify scenario sees the same catalog.
/// Product 3 has a huge stock so load tests never run out.
pub fn seed_products() -> Vec<Product> {
    vec![
        product(1, "Mechanical Keyboard", 3200, 50),
        product(2, "Wireless Mouse", 1500, 100),
        product(3, "USB-C Cable", 290, 1_000_000),
    ]
}

fn product(id: u64, name: &str, price: u64, stock: u64) -> Product {
    Product {
        id,
        name: name.to_owned(),
        price,
        stock,
    }
}
