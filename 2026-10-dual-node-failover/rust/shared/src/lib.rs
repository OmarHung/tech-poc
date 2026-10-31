//! Plumbing shared by every process: the node, the witness and the gateway (architecture A), and the
//! upstream, the workers and the sink (architectures B and C).

pub mod clock;
pub mod env;
pub mod error;
pub mod http;
pub mod logging;
pub mod state_file;
pub mod supervise;
pub mod wire;
pub mod work;
