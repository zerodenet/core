//! Explicit standalone Service fixtures. Production Query/Node and active
//! ingress supply their actual original root to connect_stream/loan methods.
use super::*;
use zero_platform_tokio::contract::context::with_deadline;
pub(crate) async fn connect(
    services: TcpExecutionServices,
    destination: Destination,
    plan: ServicePlan,
    purpose: ServicePurpose,
    context: CarrierContext,
) -> Result<ServiceConnected, ChannelError> {
    if context.session.is_some() {
        return Err(failure(Reason::InvalidConfig));
    }
    if context.cancel.is_cancelled() {
        return Err(failure(Reason::Cancelled));
    }
    if context
        .deadline
        .is_some_and(|end| tokio::time::Instant::now() >= end)
    {
        return Err(failure(Reason::Timeout));
    }
    let operation = async {
        match plan {
            ServicePlan::Single(node) => {
                chain(&services, &destination, vec![node], purpose, &context).await
            }
            ServicePlan::Relay(nodes) => {
                chain(&services, &destination, nodes, purpose, &context).await
            }
            ServicePlan::Fallback(nodes) => {
                let mut last = None;
                for node in nodes {
                    match chain(&services, &destination, vec![node], purpose, &context).await {
                        Ok(io) => return Ok(io),
                        Err(error) => {
                            if !error.retry_permitted() {
                                return Err(error);
                            }
                            last = Some(error);
                        }
                    }
                }
                Err(last.unwrap_or_else(|| failure(Reason::InvalidConfig)))
            }
        }
    };
    tokio::select! {biased;
        _=context.cancel.cancelled()=>Err(interrupted(Reason::Cancelled)),
        result=with_deadline(context.deadline,operation)=>result.map_err(|source|interrupted(Reason::Timeout).with_source(source))?,
    }
}

async fn chain(
    services: &TcpExecutionServices,
    destination: &Destination,
    nodes: Vec<ServiceNode>,
    purpose: ServicePurpose,
    call: &CarrierContext,
) -> Result<ServiceConnected, ChannelError> {
    let first = nodes
        .first()
        .ok_or_else(|| failure(Reason::InvalidConfig))?;
    let physical = first
        .endpoint
        .clone()
        .or_else(|| nodes.get(1).and_then(|node| node.endpoint.clone()))
        .unwrap_or_else(|| destination.clone());
    let network = match &first.tag {
        Some(tag) => services.upstream_observed(tag),
        None => services.upstream(),
    };
    let generation = NetworkGeneration(network.network_generation());
    let network_updates = network.network_updates();
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: first.dial.clone(),
        network_generation: generation,
    };
    let root = Arc::new(root::RootOpener {
        services: network,
        binding: binding.clone(),
        endpoint: physical,
        role: first.role,
        evidence: Arc::default(),
        source: services.current_source.clone(),
        selected: first.slot.role_id(),
        call: call.clone(),
        candidates: tokio::sync::Mutex::new(None),
    });
    let upstream = UpstreamTransport::new(binding, root.clone());
    let io = connect_stream(services, destination, &nodes, purpose, call, upstream).await?;
    let _evidence = root
        .take_evidence()
        .ok_or_else(|| failure(Reason::Internal))?;
    Ok(ServiceConnected {
        stream: io,
        _binding: ServiceBinding::new(&nodes, destination.clone(), generation, network_updates)?,
    })
}
