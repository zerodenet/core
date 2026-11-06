//! Explicit real-pool ownership; ordinary resource admission remains gated.
mod options;
mod pool;
mod role;
pub use options::{Options, PreparedMux};
pub use pool::{OwnedPoolFactory, OwnedPoolOptions};
pub use role::Factory;
pub use zero_transport::pooled_driver::PoolFactory;
