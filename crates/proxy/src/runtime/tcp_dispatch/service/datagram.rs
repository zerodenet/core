//! Execute the same selected standard Datagram role for a DNS service caller.
use super::*;
use crate::inventory::service::ServiceDatagram;
use zero_core::{component::AssociationId, request::DatagramTargets};
use zero_platform_tokio::{
    contract::datagram::DatagramRequest, controlled_carrier::loan::DatagramLoan,
};

pub(crate) async fn connect_carrier_datagram(
    services: &TcpExecutionServices,
    destination: &Destination,
    node: &ServiceDatagram,
    call: &CarrierContext,
    upstream: UpstreamTransport,
) -> Result<DatagramLoan, ChannelError> {
    if call.session.is_some() {
        return Err(failure(Reason::InvalidConfig));
    }
    if call.cancel.is_cancelled() {
        return Err(failure(Reason::Cancelled));
    }
    let deadline = call
        .deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(5));
    if tokio::time::Instant::now() >= deadline {
        return Err(failure(Reason::Timeout));
    }
    let plan = services
        .components
        .as_ref()
        .map_err(|source| retained(source.clone()))?;
    if plan
        .role_slot(node.slot.role_id())
        .is_none_or(|selected| selected.instance_id() != node.slot.instance_id())
    {
        return Err(failure(Reason::StalePlan));
    }
    let role = node.slot.datagram(deadline).await?;
    role.open_carrier(
        DatagramRequest {
            association: AssociationId(0),
            targets: DatagramTargets::Fixed(destination.clone()),
        },
        OpenContext {
            identity: RequestIdentity::Service(
                Arc::new(ServiceRequestFacts {
                    origin: ServiceOrigin::Runtime,
                    purpose: ServicePurpose::DnsQuery,
                })
                .into(),
            ),
            deadline: call.deadline,
            cancel: call.cancel.clone(),
            observer: None,
            upstream,
        },
    )
    .await
}
