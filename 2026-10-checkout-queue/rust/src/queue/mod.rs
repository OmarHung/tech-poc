//! The virtual waiting room: join, poll (or stream), get admitted.

mod events;
pub mod push;
mod routes;
mod store;
mod streams;
mod view;

pub use push::{TicketPushWorker, Work};
pub use routes::routes;
pub use store::{AdmitResult, JoinResult, QueueStore};
pub use streams::{TicketStream, TicketStreams};
pub use view::{TicketStatus, TicketView};
