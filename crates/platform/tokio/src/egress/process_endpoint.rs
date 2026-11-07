//! Concrete same-job loopback probe metadata, never external egress authority.
use super::*;
pub(crate) fn process_endpoint_selection(
    peer: SocketAddr,
    generation: zero_core::component::NetworkGeneration,
) -> io::Result<EgressSelection> {
    if !peer.ip().is_loopback() || peer.port() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "process probe is not its actual loopback endpoint",
        ));
    }
    Ok(EgressSelection {
        dial_policy: Some(zero_traits::DialPolicy::default()),
        dial_source_address: Some(SocketAddr::new(peer.ip(), 0)),
        interface: None,
        configured_interface: None,
        generation: generation.0,
        tun_active: false,
        unavailable_reason: None,
        route_source: Some(peer.ip()),
        route_lookup_status: EgressRouteLookupStatus::Skipped,
        route_lookup_error: None,
        binding_reason: EgressBindingReason::Loopback,
    })
}
