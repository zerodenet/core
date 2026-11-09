use super::*;
use zero_core::{request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
    },
    controlled_carrier::{CarrierCandidates, NativeCarrierOpener},
};

pub(super) fn destination(peer: SocketAddr) -> Destination {
    Destination::new(
        match peer.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        peer.port(),
    )
}
pub(super) async fn candidates(
    fixture: &Fixture,
    peer: SocketAddr,
    egress: Arc<zero_platform_tokio::EgressInterfaceControl>,
    cancel: zero_platform_tokio::contract::CancellationToken,
) -> CarrierCandidates {
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(egress.generation()),
    };
    NativeCarrierOpener::new(
        fixture.slot.instance_id(),
        binding.clone(),
        peer,
        egress,
        None,
    )
    .unwrap()
    .resolve_controlled_carrier(
        &binding,
        destination(peer),
        CarrierContext {
            session: None,
            deadline: Some(end()),
            cancel,
        },
    )
    .await
    .unwrap()
}
