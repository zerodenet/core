use super::{EgressBindingReason, EgressInterface, EgressInterfaceControl, EgressSelection};
use crate::dial::interfaces::{invalid_policy, inventory_for};
use std::{io, net::SocketAddr};
use zero_traits::{canonicalize_ip, DialPolicy};
impl EgressInterfaceControl {
    /// Merge explicit requirements with automatic TUN-loop avoidance. An
    /// explicit interface is never dropped for loopback or a system route.
    pub fn select_for_peer_with_policy(&self, peer: SocketAddr, policy: &DialPolicy) -> io::Result<EgressSelection> {
        let peer = policy.normalize_peer(peer).map_err(invalid_policy)?;
        let interfaces = inventory_for(policy)?;
        let mut selection = self.select_for_peer(peer);
        selection.ensure_connectable()?;
        selection.dial_policy = Some(policy.clone());
        if policy.interface.is_none() && policy.source_ip.is_none() {
            selection.dial_source_address = crate::dial::capture_source(peer, policy, &selection)?;
            return Ok(selection);
        }
        let topology = self.0.read().expect("egress interface lock poisoned");
        if topology.generation != selection.generation {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "egress topology changed during dial selection"));
        }
        let source = policy.source_ip.map(canonicalize_ip);
        let explicit = policy.interface.as_ref().and_then(|name| interfaces.iter().find(|interface| interface.matches(name)));
        if source.is_some_and(|source| topology.tunnel_addresses.iter().any(|address| canonicalize_ip(*address) == source))
            || explicit.is_some_and(|interface| interface.addresses.iter().any(|address| topology.tunnel_addresses.iter().any(|tunnel| canonicalize_ip(*tunnel) == *address))) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "explicit dial binding would re-enter the active TUN interface"));
        }
        if let Some(interface) = explicit {
            let index = interface.index(peer.is_ipv6());
            if index == 0 || !interface.addresses.iter().any(|address| address.is_ipv6() == peer.is_ipv6()) {
                return Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "dial interface has no address in the destination family"));
            }
            let mut selected = EgressInterface::new(interface.name.clone(), index)?;
            if let Some(strict) = selection.configured_interface.as_ref().filter(|interface| interface.socket_mark().is_some()) {
                if strict.index() != index {
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, "explicit dial interface conflicts with strict-route egress"));
                }
                selected = selected.with_socket_mark(strict.socket_mark().expect("filtered above"))?;
            }
            selection.interface = Some(selected);
        } else if policy.source_ip.is_some() {
            // A strict firewall identity cannot disappear just because a
            // source-bound route probe selected an ordinary system route.
            if let Some(strict) = selection.configured_interface.as_ref().filter(|interface| interface.socket_mark().is_some()) {
                selection.interface = Some(strict.clone());
            } else if selection.tun_active && selection.interface.is_none() {
                // A probe without the requested source does not establish
                // that source-specific routing also bypasses the TUN. Pin its
                // unique owner instead of trusting that unrelated route.
                let source = source.expect("source policy checked above");
                let mut owners = interfaces.iter().filter(|interface| interface.addresses.contains(&source));
                let owner = owners.next().expect("inventory validated source");
                if owners.next().is_some() {
                    return Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "source_ip has ambiguous ownership while TUN capture is active"));
                }
                selection.interface = Some(EgressInterface::new(owner.name.clone(), owner.index(peer.is_ipv6()))?);
            }
        }
        if let (Some(source), Some(selected)) = (source, selection.interface.as_ref()) {
            if !interfaces.iter().any(|interface| interface.index(peer.is_ipv6()) == selected.index() && interface.addresses.contains(&source)) {
                return Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "dial source_ip is incompatible with the required egress interface"));
            }
        }
        selection.binding_reason = EgressBindingReason::ExplicitDialPolicy;
        selection.dial_source_address = crate::dial::capture_source(peer, policy, &selection)?;
        Ok(selection)
    }
}
