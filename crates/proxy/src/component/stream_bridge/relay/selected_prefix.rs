//! Native donor prefix for an existing receiver: original caller -> actual
//! standard Stream loan. No synthetic business Session or alternate executor.
use super::*;
use crate::inventory::{service::ServiceNode, PreparedTcpRelayPrefix};
use zero_platform_tokio::contract::network::CarrierContext;
pub(crate) async fn connect(
    services: &TcpExecutionServices,
    prefix: &PreparedTcpRelayPrefix,
    nodes: Vec<ServiceNode>,
    record_health: bool,
) -> Result<crate::transport::TcpRelayStream, TcpOutboundFailure> {
    let failure = |mut error: ChannelError| {
        error.session = error.session.or_else(|| {
            services
                .invocation
                .as_ref()
                .and_then(|invocation| invocation.identity.session())
        });
        super::super::failure_optional("relay_first_hop", error, prefix.first.endpoint.as_ref())
    };
    let invocation = services.invocation.as_ref().ok_or_else(|| {
        failure(invalid(
            Reason::Revoked,
            "native prefix requires its original invocation",
        ))
    })?;
    let call = CarrierContext {
        session: None,
        deadline: invocation.deadline,
        cancel: invocation.cancellation(),
    };
    let last = nodes
        .last()
        .ok_or_else(|| failure(invalid(Reason::InvalidConfig, "selected prefix is empty")))?;
    let binding = UpstreamBinding {
        prefix: nodes.iter().map(|node| node.slot.role_id()).collect(),
        dial: last.dial.clone(),
        network_generation: NetworkGeneration(services.upstream().network_generation()),
    };
    let waiting = super::super::pending::PendingCaller::new(
        services,
        invocation,
        binding.prefix.clone(),
        binding.clone(),
    )
    .map_err(failure)?;
    let endpoint = destination(&(prefix.final_server.clone(), prefix.final_port));
    // Preserve the previously selected host wire form; this framing operation
    // is internal carrier work under the actual caller, never a business ID 0.
    let wire = Destination::new(
        Address::Domain(prefix.final_server.clone()),
        prefix.final_port,
    );
    // The receiver attaches its own Outer meter after consuming this prefix.
    // Retain donor meters without counting receiver bytes a second time.
    let upstream = background::BackgroundPath {
        nodes,
        binding,
        endpoint: endpoint.clone(),
        wire_endpoint: wire,
        final_tag: "",
        call: call.clone(),
    }
    .select_observed_with_identity(
        services,
        waiting
            .as_ref()
            .map(super::super::pending::PendingCaller::observer),
        None,
        record_health,
    )
    .await
    .map_err(failure)?;
    let loan = upstream
        .borrow_stream(endpoint, call)
        .await
        .map_err(failure)?;
    let addresses = loan.owner.original().native_origin().endpoints();
    let (local, remote) =
        addresses.map_or((None, None), |actual| (actual.local(), Some(actual.peer())));
    Ok(crate::transport::TcpRelayStream::from_client(
        LegacyStream::with_endpoints(
            crate::component::background::stateless::service_stream(loan),
            local,
            remote,
        ),
    ))
}
