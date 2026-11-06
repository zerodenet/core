//! Explicit real-pool ownership; ordinary resource admission remains gated.
mod options;
mod pool;
pub use pool::{OwnedPoolFactory, OwnedPoolOptions};
mod role;
pub use options::{Options, PreparedMux};
pub use role::Factory;
pub use zero_transport::pooled_driver::PoolFactory;
