//! Standard business Datagram over host-selected ordinary Stream prefixes.
mod borrowed;
mod opener;
mod pooled;
mod prepare;
use super::*;
use crate::component::{
    business_carrier::BusinessCarrierRequest,
    stream_bridge::datagram::{Opened, WireTraffic},
};
use crate::inventory::PreparedDatagramRelay;
use zero_core::request::DatagramTargets;
use zero_platform_tokio::contract::datagram::DatagramRequest;
pub(crate) async fn open(
    services: &TcpExecutionServices,
    prepared: PreparedDatagramRelay,
    session: &Session,
    association: zero_core::component::AssociationId,
) -> Result<Opened, ChannelError> {
    if prepared.pooled {
        return pooled::open(services, prepared, session, association).await;
    }
    let slot = prepared.target.current_slot(services).map_err(|source| {
        invalid(Reason::StalePlan, "relay Datagram owner changed").with_source(source)
    })?;
    let invocation = services.invocation.clone().ok_or_else(|| {
        invalid(
            Reason::InvalidConfig,
            "relay requires original business admission",
        )
    })?;
    let end = invocation
        .deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(5));
    let role = slot.datagram(end).await?;
    let binding = UpstreamBinding {
        prefix: prepared
            .prefix
            .iter()
            .map(|node| node.slot.role_id())
            .collect(),
        dial: slot.dial.clone(),
        network_generation: NetworkGeneration(services.upstream().network_generation()),
    };
    let plan = services
        .components
        .as_ref()
        .map_err(|error| crate::component::instance::retained(error.clone()))?;
    // Persist exact real-generation dependencies before any root/DNS await.
    plan.authorize_carrier_prefix(slot.instance_id(), &binding.prefix)?;
    let business = BusinessCarrierRequest::new(services.engine(), &slot, &invocation)?
        .ok_or_else(|| {
            invalid(
                Reason::Unsupported,
                "relay has no controlled business owner",
            )
        })?
        .with_prefix(&binding.prefix);
    let (_, server, port) = prepared.target.endpoint();
    let final_endpoint = destination(&(server.into(), port));
    let wire = WireTraffic::new(
        services.outbound_io(prepared.target.endpoint().0, zero_api::TrafficPlane::Outer),
    );
    let opener = Arc::new(
        opener::Opener::new(
            services,
            prepared.prefix,
            prepared.borrowed,
            binding.clone(),
            final_endpoint,
            Destination::new(Address::Domain(server.into()), port),
            business,
            end,
        )
        .await?
        .with_final_observer(
            services.outbound_io(prepared.target.endpoint().0, zero_api::TrafficPlane::Outer),
        ),
    );
    let channel = role
        .open(
            DatagramRequest {
                association,
                targets: DatagramTargets::Fixed(Destination::new(
                    session.target.clone(),
                    session.port,
                )),
            },
            OpenContext {
                identity: invocation.identity.clone(),
                deadline: invocation.deadline,
                cancel: invocation.cancellation(),
                observer: Some(wire.clone()),
                upstream: UpstreamTransport::new(binding, opener.clone()),
            },
        )
        .await;
    opener.observe_result(&channel);
    let channel = channel?;
    Ok(Opened {
        channel,
        wire,
        remote: opener.remote(),
    })
}
