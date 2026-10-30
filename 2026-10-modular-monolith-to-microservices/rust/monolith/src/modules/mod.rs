//! The monolith's modules. They only talk to each other through explicit interfaces,
//! which is what made extracting `catalog` cheap.

pub mod catalog;
pub mod orders;
