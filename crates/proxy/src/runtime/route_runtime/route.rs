mod access;
mod model;
mod serve;
#[cfg(test)]
mod tests;

pub(crate) use model::{InboundRouteRuntime, InboundRouteRuntimeFactory};

mod source;
#[cfg(feature = "managed-datagram-runtime")]
pub(in crate::runtime) use source::DatagramContinuityHost;
