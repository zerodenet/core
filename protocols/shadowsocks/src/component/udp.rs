//! The existing one-socket UDP cache, with its actual native and Scope owner.
mod pool;
pub(crate) use pool::{PoolRegistry, PoolSelection};
pub use pool::{UdpPoolFactory, UdpPoolOptions};

mod logical;

pub(crate) mod role;
