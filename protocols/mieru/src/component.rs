//! Library-only native ownership. No production registration or default budgets.
mod pool;
pub use pool::{PoolFactory, PoolOptions};
mod stream;
pub use stream::{PreparedTcp, StreamFactory, StreamOptions, TcpPreparation};
#[cfg(test)]
#[path = "../tests/component/fixtures.rs"]
pub(crate) mod test_support;
