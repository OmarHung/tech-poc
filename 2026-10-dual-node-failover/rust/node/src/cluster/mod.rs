//! Who is primary, and how the log gets from the primary to the standby.

pub mod contracts;
pub mod election;
pub mod node_state;
pub mod primary_term;
pub mod replication;
pub mod rpc;
pub mod witness_client;

/// Most entries sent in one replication request. Also how far behind a solo standby may be when
/// writes start waiting for it again.
pub const MAX_BATCH: usize = 256;
