//! Inert server decorators. The actual carrier owner starts their original I/O
//! futures in its existing driver ledger; preparation never spawns a task.
use super::udp::Mask;
#[cfg(all(feature = "mkcp", feature = "quic"))]
use super::Socket;
#[cfg(all(feature = "mkcp", feature = "quic"))]
use crate::datagram_driver::PreparedIoDriver;
#[cfg(all(feature = "mkcp", feature = "quic"))]
use quinn::AsyncUdpSocket;
use std::io;
#[cfg(all(feature = "mkcp", feature = "quic"))]
use std::sync::Arc;
#[cfg(all(feature = "mkcp", feature = "quic"))]
use zero_platform_tokio::controlled_icmp::ControlledIcmpSocket;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DriverKind {
    Noise,
    Xdns,
    Xicmp,
}
#[cfg(all(feature = "mkcp", feature = "quic"))]
pub(crate) struct Driver {
    pub(crate) kind: DriverKind,
    pub(crate) ordinal: u8,
    pub(crate) work: PreparedIoDriver,
}
#[cfg(all(feature = "mkcp", feature = "quic"))]
pub(crate) struct Prepared {
    pub(crate) socket: Arc<dyn AsyncUdpSocket>,
    pub(crate) drivers: Vec<Driver>,
}
#[cfg(all(feature = "mkcp", feature = "quic"))]
mod plan;
#[cfg(all(feature = "mkcp", feature = "quic"))]
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
