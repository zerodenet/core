//! Select only routes whose original algorithm needs no unowned native probe.
use super::*;

impl EgressInterfaceControl {
    pub(crate) fn select_packet_without_native_probe(
        &self,
        peer: SocketAddr,
    ) -> io::Result<EgressSelection> {
        let interfaces = self.0.read().expect("egress interface lock poisoned");
        let snapshot = EgressSelectionSnapshot::capture(&interfaces, peer);
        without_probe(&snapshot, peer).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "controlled packet selection requires a separately owned native route probe",
            )
        })
    }
}
impl EgressSelectionSnapshot {
    pub(super) fn capture(interfaces: &EgressInterfaces, peer: SocketAddr) -> Self {
        let (configured_interface, unavailable_reason) = if peer.is_ipv6() {
            (&interfaces.ipv6, &interfaces.ipv6_unavailable_reason)
        } else {
            (&interfaces.ipv4, &interfaces.ipv4_unavailable_reason)
        };
        Self {
            configured_interface: configured_interface.clone(),
            generation: interfaces.generation,
            tun_active: !interfaces.tunnel_addresses.is_empty(),
            unavailable_reason: unavailable_reason.clone(),
        }
    }
}
/// Both selectors use the same snapshot and the same no-probe decisions. No
/// read/check/unlock/re-read window may turn a denied route into a native probe.
pub(super) fn without_probe(
    snapshot: &EgressSelectionSnapshot,
    peer: SocketAddr,
) -> Option<EgressSelection> {
    let (interface, reason) = if peer.ip().is_loopback() {
        (None, EgressBindingReason::Loopback)
    } else if peer.ip().is_unspecified() {
        // Preserve the existing family-marker semantics, including fail closed
        // when TUN is known active but physical egress is absent.
        match (snapshot.configured_interface.clone(), snapshot.tun_active) {
            (None, false) => (None, EgressBindingReason::NoConfiguredInterface),
            (None, true) => (None, EgressBindingReason::TunEgressUnavailable),
            (Some(physical), false) => {
                (Some(physical), EgressBindingReason::TunAddressesUnavailable)
            }
            (Some(physical), true) => (Some(physical), EgressBindingReason::TunRoute),
        }
    } else if !snapshot.tun_active {
        (
            Some(snapshot.configured_interface.clone()?),
            EgressBindingReason::TunAddressesUnavailable,
        )
    } else {
        return None;
    };
    Some(EgressSelection::from_snapshot(
        snapshot,
        interface,
        None,
        EgressRouteLookupStatus::Skipped,
        None,
        reason,
    ))
}

#[cfg(test)]
#[path = "../../tests/egress/packet_selection.rs"]
mod tests;
