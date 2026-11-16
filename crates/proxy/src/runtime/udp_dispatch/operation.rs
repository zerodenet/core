//! Prepared UDP operation contracts plus focused executor modules.
//!
//! Only remaining compatibility executors live here. Direct dispatch consumes
//! Engine-selected facts through its actual native provider outside this SPI.

mod contract;
#[cfg(feature = "managed-datagram-runtime")]
mod managed_datagram;

pub(crate) use contract::PreparedUdpFlowOperation;
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use managed_datagram::{ManagedDatagramStartPlan, ManagedDatagramUdpOperation};
