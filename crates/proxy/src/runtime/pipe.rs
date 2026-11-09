//! Kernel pipe abstraction.
//!
//! The proxy runtime is an orchestration engine. This trait is the top-level
//! runtime boundary: TCP and UDP are the two core pipe implementations, while
//! concrete protocols plug into those pipes through protocol traits and
//! dispatch categories.

mod contract;
mod tcp;
#[cfg(feature = "udp-runtime")]
mod udp;

pub(crate) use contract::KernelPipe;
pub(crate) use tcp::{TcpPipe, TcpPipeInput};
#[cfg(feature = "udp-runtime")]
pub(crate) use udp::{UdpDomainMemory, UdpPipe, UdpPipeInput};
