//! One of two identical workers that never talk to each other: architecture B (hot-hot, `hothot/`)
//! or C (dispatched, `dispatch/`), chosen by `MODE`.

pub mod client;
pub mod config;
pub mod dispatch;
pub mod hosting;
pub mod hothot;
pub mod status;
