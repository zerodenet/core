//! Existing flat Associated relay, with its real Source driver caller.
use super::*;
use crate::{
    component::stream_bridge::relay::background::BackgroundPath,
    inventory::{service::ServiceNode, PreparedTcpRelayPrefix},
};
use zero_platform_tokio::contract::dns::DnsQueryRole;
impl StandardPacketPath {
    pub(crate) async fn build_associated(
        services: &PacketPathExecutionServices,
        prefix: &PreparedTcpRelayPrefix,
        descriptor: PacketPathCarrierDescriptor,
    ) -> Result<Option<Arc<dyn PacketPathCarrier>>, EngineError> {
        // Presence distinguishes the original independent library contract;
        // it grants no caller/native authority. Once present, every subsequent
        // target/binding/plan error is terminal for this attempt.
        if !services.has_packet_driver_caller() {
            return Ok(None);
        }
        let execution = services.tcp();
        let plan = execution
            .components
            .as_ref()
            .map_err(|source| failure(crate::component::instance::retained(source.clone())))?;
        let (slot, nodes, binding) = associated_selection(
            plan,
            prefix,
            &descriptor,
            NetworkGeneration(execution.upstream().network_generation()),
        )?;
        let tag = descriptor
            .tag
            .as_deref()
            .expect("validated associated target tag");
        // Exact target/prefix graph and pending capacity are checked by the
        // original Source issuer. None is only its explicit library contract.
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
                    targets: DatagramTargets::Multiple,
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
        session.control = reservation.adopt(session.control);
        Ok(Some(Arc::new(Path::new(
            session,
            association,
            stop,
            Some(handoff),
        ))))
    }
}
fn destination(host: &str, port: u16) -> Destination {
    Destination::new(
        match host.parse::<std::net::IpAddr>() {
            Ok(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
            Ok(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
            Err(_) => zero_core::Address::Domain(host.into()),
        },
        port,
    )
}
fn node(
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

/// Pure same-plan binding shared by cold Source capture and actual execution.
/// This is metadata only; it does not enter a role or acquire any IO.
pub(crate) fn associated_binding(
    plan: &crate::component::catalog::ComponentPlan,
    prefix: &PreparedTcpRelayPrefix,
    descriptor: &PacketPathCarrierDescriptor,
    network_generation: NetworkGeneration,
) -> Result<UpstreamBinding, EngineError> {
    associated_selection(plan, prefix, descriptor, network_generation)
        .map(|(_, _, binding)| binding)
}
fn associated_selection(
    plan: &crate::component::catalog::ComponentPlan,
    prefix: &PreparedTcpRelayPrefix,
    descriptor: &PacketPathCarrierDescriptor,
    network_generation: NetworkGeneration,
) -> Result<(Arc<ComponentSlot>, Vec<ServiceNode>, UpstreamBinding), EngineError> {
    let tag = descriptor.tag.as_deref().ok_or_else(|| {
        failure(invalid(
            Reason::StalePlan,
            "associated target tag is absent",
        ))
    })?;
    if tag != prefix.final_tag
        || descriptor.server != prefix.final_server
        || descriptor.port != prefix.final_port
    {
        return Err(failure(invalid(
            Reason::StalePlan,
            "associated target changed",
        )));
    }
    let slot = plan
        .configured(tag)
        .ok_or_else(|| failure(invalid(Reason::Revoked, "associated final owner retired")))?;
    let mut nodes = Vec::with_capacity(prefix.relay_hops.len() + 1);
    let first = match prefix.first.tag.as_deref() {
        Some(tag) => plan
            .configured(tag)
            .ok_or_else(|| failure(invalid(Reason::Revoked, "associated first owner retired")))?,
        None => plan.direct(None).map_err(failure)?,
    };
    nodes.push(node(
        first,
        prefix.first.tag.clone(),
        prefix.first.endpoint.clone(),
    ));
    for hop in &prefix.relay_hops {
        let slot = plan
            .configured(&hop.tag)
            .ok_or_else(|| failure(invalid(Reason::Revoked, "associated prefix owner retired")))?;
        nodes.push(node(
            slot,
            Some(hop.tag.clone()),
            Some((hop.server.clone(), hop.port)),
        ));
    }
    let binding = UpstreamBinding {
        prefix: nodes.iter().map(|node| node.slot.role_id()).collect(),
        dial: slot.dial.clone(),
        network_generation: network_generation,
    };
    Ok((slot, nodes, binding))
}
