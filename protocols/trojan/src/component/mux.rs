//! Explicit real-pool ownership; ordinary resource admission remains gated.
mod options;
mod role;
pub use options::{PreparedMux, StreamOptions};
pub use role::StreamFactory;
pub use zero_transport::pooled_driver::PoolFactory;
