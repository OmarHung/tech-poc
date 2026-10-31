//! One of the two data nodes. Which one is primary is decided at runtime (see `cluster::election`).

pub mod cluster;
pub mod config;
pub mod hosting;
pub mod kv;
pub mod storage;
