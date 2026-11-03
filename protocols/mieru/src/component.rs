//! Native pool ownership and TCP/UDP logical roles. No production registration or default budgets.
pub mod inbound;
mod pool;
pub use pool::{PoolCarrierSpec, PoolFactory, PoolOptions, PoolProfile};
mod stream;
pub use stream::{PreparedTcp, StreamFactory, StreamOptions, TcpPreparation};
#[cfg(test)]
#[path = "../tests/component/fixtures.rs"]
pub(crate) mod test_support;
