//! Native socket enforcement for neutral dial requirements.
pub(crate) mod interfaces;
mod tcp;
mod udp;
use crate::{EgressInterface, EgressSelection};
use interfaces::{invalid_policy, inventory_for};
use std::{
    io,
    net::{IpAddr, SocketAddr, SocketAddrV6},
};
use zero_traits::{canonicalize_ip, DialPolicy};

/// Validate local address/interface existence and ownership before publishing
/// a configuration. Repeated when opening a socket to detect host changes.
pub fn validate_dial_policy(policy: &DialPolicy) -> io::Result<()> {
    inventory_for(policy).map(|_| ())
}
pub(super) struct PreparedDial {
    peer: SocketAddr,
    source: Option<SocketAddr>,
    interface: Option<EgressInterface>,
}
fn prepare(
    peer: SocketAddr,
    policy: &DialPolicy,
    selection: &EgressSelection,
) -> io::Result<PreparedDial> {
    let peer = policy.normalize_peer(peer).map_err(invalid_policy)?;
    selection.ensure_connectable()?;
    if selection.dial_policy.as_ref() != Some(policy) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "egress selection was not prepared for this dial policy",
        ));
    }
    let interfaces = inventory_for(policy)?;
    let interface = selection.interface().cloned();
    if let Some(name) = &policy.interface {
        let local = interfaces
            .iter()
            .find(|interface| interface.matches(name))
            .expect("inventory validated policy");
        if !interface
            .as_ref()
            .is_some_and(|selected| local.index(peer.is_ipv6()) == selected.index())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "egress selection does not retain the requested dial interface",
            ));
        }
    }
    let source = policy
        .source_ip
        .map(canonicalize_ip)
        .map(|source| {
            if let Some(selected) = &interface {
                if !interfaces.iter().any(|local| {
                    local.index(peer.is_ipv6()) == selected.index()
                        && local.addresses.contains(&source)
                }) {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrNotAvailable,
                        "dial source_ip no longer belongs to the selected interface",
                    ));
                }
            }
            if let IpAddr::V6(address) = source {
                if address.is_unicast_link_local() {
                    let mut owners = interfaces
                        .iter()
                        .filter(|local| local.addresses.contains(&source));
                    let scope = if let Some(interface) = &interface {
                        interface.index()
                    } else {
                        let first = owners
                            .next()
                            .expect("inventory validated source")
                            .index(true);
                        if owners.next().is_some() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "link-local source_ip requires an unambiguous interface",
                            ));
                        }
                        first
                    };
                    return Ok(SocketAddr::V6(SocketAddrV6::new(address, 0, 0, scope)));
                }
            }
            Ok(SocketAddr::new(source, 0))
        })
        .transpose()?;
    let captured_source = selection.dial_source_address();
    if captured_source.is_some_and(|captured| {
        captured.is_ipv6() != peer.is_ipv6() || source.is_some_and(|source| source != captured)
    }) {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "dial source binding changed after egress selection",
        ));
    }
    Ok(PreparedDial {
        peer,
        source: captured_source.or(source),
        interface,
    })
}
pub(crate) fn capture_source(
    peer: SocketAddr,
    policy: &DialPolicy,
    selection: &EgressSelection,
) -> io::Result<Option<SocketAddr>> {
    let dial = prepare(peer, policy, selection)?;
    if dial.source.is_some() {
        return Ok(dial.source);
    }
    let local = crate::egress::datagram_bind_address(dial.peer, dial.interface.as_ref())?;
    Ok((!local.ip().is_unspecified()).then_some(local))
}
