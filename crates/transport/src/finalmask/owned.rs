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
pub(crate) fn prepare(
    inner: Arc<dyn AsyncUdpSocket>,
    masks: &[Mask],
    icmp: Option<ControlledIcmpSocket>,
) -> io::Result<Prepared> {
    validate(masks)?;
    let mut drivers = Vec::new();
    let socket = layer(inner, masks, icmp, &mut drivers, 0)?;
    Ok(Prepared { socket, drivers })
}
fn layer(
    inner: Arc<dyn AsyncUdpSocket>,
    masks: &[Mask],
    icmp: Option<ControlledIcmpSocket>,
    drivers: &mut Vec<Driver>,
    offset: usize,
) -> io::Result<Arc<dyn AsyncUdpSocket>> {
    if let Some(Mask::Xicmp { id, .. }) = masks.first() {
        let raw = icmp
            .ok_or_else(|| io::Error::other("original ICMP provider omitted its granted socket"))?;
        let (socket, work) = super::xicmp::prepare_server(inner, raw, *id)?;
        drivers.push(Driver {
            kind: DriverKind::Xicmp,
            ordinal: offset as u8,
            work,
        });
        return layer(socket, &masks[1..], None, drivers, offset + 1);
    }
    if icmp.is_some() {
        return Err(io::Error::other("unexpected ICMP grant"));
    }
    if let Some(index) = masks.iter().position(|m| matches!(m, Mask::Xdns { .. })) {
        let Mask::Xdns { domain } = &masks[index] else {
            unreachable!()
        };
        let outer = layer(inner, &masks[..index], None, drivers, offset)?;
        let (socket, work) = super::xdns::prepare_server(outer, domain)?;
        drivers.push(Driver {
            kind: DriverKind::Xdns,
            ordinal: (offset + index) as u8,
            work,
        });
        return layer(
            socket,
            &masks[index + 1..],
            None,
            drivers,
            offset + index + 1,
        );
    }
    Socket::prepare_layer(inner, masks, drivers, offset)
}

#[cfg(all(feature = "mkcp", feature = "quic"))]
pub mod source;
