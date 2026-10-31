//! Flash-sale checkout queue (a virtual waiting room) backed by Redis. All domain logic lives in
//! the shared Lua scripts (`../shared/lua/`); this crate validates requests, runs the scripts,
//! shapes their replies into JSON, and pushes ticket updates over server-sent events.

pub mod admin;
pub mod app;
pub mod clock;
pub mod config;
pub mod demo;
pub mod http;
pub mod ids;
pub mod limits;
pub mod maintenance;
pub mod orders;
pub mod queue;
pub mod sale_redis;
