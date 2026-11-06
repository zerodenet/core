//! Pure inner-target DNS using the existing host resolver and query owner.
use super::*;
use std::net::{IpAddr, SocketAddr};
use zero_core::{
    Address,
    request::{Destination, ServicePurpose},
};
use zero_platform_tokio::contract::{
    DeliveryState, Impact, Stage,
    context::RequestIdentity,
    dns::{DnsQueryContext, DnsService},
    network::CarrierContext,
};

pub(crate) async fn resolve_with(
    destination: Destination,
    identity: RequestIdentity,
    call: CarrierContext,
    resolver: &dyn DnsService,
) -> Result<Vec<SocketAddr>, ChannelError> {
    let failure = |reason| {
        let mut failure = ChannelError::new(Stage::Prepare, reason, Impact::Session);
        failure.session = identity.session();
        failure.delivery = DeliveryState::NotSubmitted;
        failure
    };
    if call.session != identity.session() || destination.port == 0 {
        return Err(failure(Reason::InvalidConfig));
    }
    if call.cancel.is_cancelled() {
        return Err(failure(Reason::Cancelled));
    }
    if call
        .deadline
        .is_some_and(|end| tokio::time::Instant::now() >= end)
    {
        return Err(failure(Reason::Timeout));
    }
    let literal = match &destination.address {
        Address::Ipv4(ip) => Some(IpAddr::from(*ip)),
        Address::Ipv6(ip) => Some(IpAddr::from(*ip)),
        Address::Domain(name) => zero_core::address::parse_ip_literal(name),
    };
    if let Some(ip) = literal {
        return Ok(vec![SocketAddr::new(ip, destination.port)]);
    }
    let Address::Domain(name) = destination.address else {
        unreachable!("literal handled")
    };
    let role = match &identity {
        RequestIdentity::Business(_) => DnsQueryRole::Default,
        RequestIdentity::Service(facts) => match facts.purpose {
            ServicePurpose::DnsQuery | ServicePurpose::BackgroundControl => DnsQueryRole::Node,
            ServicePurpose::Bootstrap => DnsQueryRole::Bootstrap,
            ServicePurpose::HealthProbe | ServicePurpose::DiagnosticProbe => DnsQueryRole::Default,
        },
        RequestIdentity::Packet(_) => return Err(failure(Reason::InvalidConfig)),
    };
    let generation = resolver.generation();
    let resolved = resolver
        .resolve(
            DnsRequest::new(name, role),
            DnsQueryContext {
                deadline: call.deadline,
                cancel: call.cancel,
                expected_generation: Some(generation),
            },
        )
        .await?;
    if resolved.generation != generation {
        return Err(failure(Reason::StalePlan));
    }
    let addresses: Vec<_> = resolved
        .addresses
        .into_iter()
        .map(|address| SocketAddr::new(super::ip_address_to_std(address), destination.port))
        .collect();
    if addresses.is_empty() {
        return Err(failure(Reason::Io));
    }
    Ok(addresses)
}
impl UpstreamConnectServices {
    pub(crate) async fn resolve_packet_with(
        destination: Destination,
        identity: RequestIdentity,
        call: CarrierContext,
        resolver: &dyn DnsService,
    ) -> Result<Vec<SocketAddr>, ChannelError> {
        resolve_with(destination, identity, call, resolver).await
    }

    pub(crate) async fn resolve_inner_packet_target(
        &self,
        destination: Destination,
        identity: RequestIdentity,
        call: CarrierContext,
    ) -> Result<Vec<SocketAddr>, ChannelError> {
        resolve_with(destination, identity, call, self.resolver.as_ref()).await
    }
}
