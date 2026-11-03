//! Explicit real-pool ownership; ordinary resource admission remains gated.
mod options;
mod role;
pub use options::{Options, PreparedMux};
pub use role::Factory;
pub use zero_transport::pooled_driver::PoolFactory;
