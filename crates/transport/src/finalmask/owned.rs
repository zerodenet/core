//! Inert server decorators. The actual carrier owner starts their original I/O
//! futures in its existing driver ledger; preparation never spawns a task.
use super::{udp::Mask, Socket};
use crate::datagram_driver::PreparedIoDriver;
use quinn::AsyncUdpSocket;
use std::{io, sync::Arc};
use zero_platform_tokio::controlled_icmp::ControlledIcmpSocket;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DriverKind {
    Noise,
    Xdns,
    Xicmp,
}
pub(crate) struct Driver {
    pub(crate) kind: DriverKind,
    pub(crate) ordinal: u8,
    pub(crate) work: PreparedIoDriver,
}
pub(crate) struct Prepared {
    pub(crate) socket: Arc<dyn AsyncUdpSocket>,
    pub(crate) drivers: Vec<Driver>,
}
mod plan;
pub use plan::PreparedMasks;
pub fn validate(masks: &[Mask]) -> io::Result<()> {
    super::udp::validate(masks)
}
pub fn icmp_bind(masks: &[Mask]) -> io::Result<Option<std::net::IpAddr>> {
    validate(masks)?;
    masks
        .iter()
        .find_map(|mask| match mask {
            Mask::Xicmp { ip, .. } => Some(super::xicmp::address(ip)),
            _ => None,
        })
        .transpose()
}
pub fn driver_count(masks: &[Mask]) -> usize {
    masks
        .iter()
        .filter(|m| matches!(m, Mask::Noise(_) | Mask::Xdns { .. } | Mask::Xicmp { .. }))
        .count()
}
#[cfg(all(feature = "mkcp", feature = "quic"))]
pub mod source;
