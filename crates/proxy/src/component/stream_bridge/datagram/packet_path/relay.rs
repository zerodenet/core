//! Existing flat Associated relay, with its real Source driver caller.
use super::*;
use crate::{
    component::stream_bridge::relay::background::BackgroundPath, inventory::service::ServiceNode,
};
use zero_platform_tokio::contract::dns::DnsQueryRole;
impl StandardPacketPath {
    pub(super) async fn open_source_associated(
        services: &PacketPathExecutionServices,
        slot: Arc<ComponentSlot>,
        nodes: Vec<ServiceNode>,
        binding: UpstreamBinding,
        descriptor: PacketPathCarrierDescriptor,
    ) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
        let execution = services.tcp();
        let plan = execution
            .components
            .as_ref()
            .map_err(|source| failure(crate::component::instance::retained(source.clone())))?;
        let tag = descriptor
            .tag
            .as_deref()
            .expect("validated associated target tag");
        // Exact target/prefix graph and pending capacity are checked by the
        // original Source issuer. Absence cannot relabel another caller.
        let Some(caller) = services
            .packet_driver_call(slot.instance_id(), &binding)
            .map_err(failure)?
        else {
            return Err(failure(invalid(
                Reason::StalePlan,
                "actual packet driver call disappeared",
            )));
        };
        plan.authorize_carrier_prefix(slot.instance_id(), &binding.prefix)
            .map_err(failure)?;
        let identity = caller.grant.identity();
        let stop = caller.call.cancel.clone();
        let endpoint = destination(&descriptor.server, descriptor.port);
        let wire_endpoint = Destination::new(
            zero_core::Address::Domain(descriptor.server),
            descriptor.port,
        );
        let upstream = BackgroundPath {
            nodes,
            binding: binding.clone(),
            endpoint,
            wire_endpoint,
            final_tag: tag,
            call: caller.call.clone(),
        }
        .select_observed_with_identity(
            &execution,
            Some(caller.pending.clone()),
            Some(identity.clone()),
            true,
        )
        .await
        .map_err(failure)?;
        caller.grant.bind_upstream(&upstream).map_err(failure)?;
        let end = caller
            .call
            .deadline
            .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(30));
        let role = slot.datagram(end).await.map_err(failure)?;
        let reservation = execution.reserve_datagram().map_err(failure)?;
        let association = reservation.association();
        let loan = role
            .open_carrier(
                DatagramRequest {
                    association,
                    targets: DatagramTargets::Fixed(services.packet_target().cloned().ok_or_else(
                        || {
                            failure(invalid(
                                Reason::InvalidConfig,
                                "Source relay lacks its actual packet destination",
                            ))
                        },
                    )?),
                },
                OpenContext {
                    identity,
                    deadline: caller.call.deadline,
                    cancel: stop.clone(),
                    upstream,
                    observer: None,
                },
            )
            .await
            .map_err(failure)?;
        let (mut session, handoff) = caller
            .grant
            .install_datagram_loan(loan, association)
            .map_err(failure)?;
        if let Err(source) = plan.validate_carrier_origin(slot.role_id(), handoff.origin()) {
            drop(session);
            handoff.request_retirement();
            return Err(failure(source));
        }
        session.control = reservation.adopt(observe_association(&execution, session.control));
        Ok(
            Arc::new(Path::new(session, association, stop, Some(handoff)))
                as Arc<dyn PacketPathCarrier>,
        )
    }
}
pub(super) fn destination(host: &str, port: u16) -> Destination {
    Destination::new(
        match zero_core::address::parse_ip_literal(host) {
            Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
            None => zero_core::Address::Domain(host.into()),
        },
        port,
    )
}
pub(super) fn node(
    slot: Arc<ComponentSlot>,
    tag: Option<String>,
    endpoint: Option<(String, u16)>,
) -> ServiceNode {
    ServiceNode {
        dial: slot.dial.clone(),
        slot,
        tag,
        wire_endpoint: endpoint
            .as_ref()
            .map(|(host, port)| Destination::new(zero_core::Address::Domain(host.clone()), *port)),
        endpoint: endpoint.map(|(host, port)| destination(&host, port)),
        role: DnsQueryRole::Node,
    }
}
