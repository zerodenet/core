//! Explicit owner-bound library roles. Production gRPC qualification stays gated.
mod options;
pub use options::{PreparedGrpc, StreamOptions};
pub use zero_transport::grpc::NativePoolFactory as PoolFactory;
mod factory;
pub use factory::StreamFactory;
mod connect;

#[cfg(test)]
#[path = "../../tests/component/grpc/mod.rs"]
mod tests;
