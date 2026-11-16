//! TCP outbound data types used by both transport and runtime layers.
//!
//! The TCP pipe orchestration lives in `crate::runtime::tcp_dispatch`.
//! This module keeps only the neutral facade over prepared transport-bridge
//! orchestration shared by transport-backed protocol adapters.

mod error;
mod model;
mod result;

pub(crate) use error::is_block_error;
pub(crate) use model::{EstablishedTcpOutbound, TcpOutboundFailure, TcpRouteResult};
pub(crate) use result::extract_tcp_stream;

#[cfg(test)]
mod tests;
