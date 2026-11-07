//! Standard peer handoff uses the actual existing listener Scope and resource group.
use super::*;
use crate::runtime::route_runtime::InboundRouteRuntimeFactory;
use zero_platform_tokio::contract::inbound::PendingInbound;
pub(super) fn spawn(
    runtime: &InboundListenerRuntime,
    factory: &InboundRouteRuntimeFactory,
    pending: PendingInbound,
    cancel: CancellationToken,
) -> Result<(), EngineError> {
    let admission = pending
        .datagram_peer()
        .expect("selected actual datagram admission")
        .clone();
    let context = runtime
        .datagram_source()
        .ok_or_else(|| std::io::Error::other("source has no declared peer authority"))?;
    let original = context
        .identity
        .get()
        .ok_or_else(|| std::io::Error::other("original UDP listener was not acquired"))?;
    if !admission.matches(original, context.limits) {
        return Err(std::io::Error::other(
            "peer source differs from original native listener/budget",
        )
        .into());
    }
    let facts = admission.facts().clone();
    let route = factory
        .for_connection(facts.effective_source)
        .with_local_addr(facts.local_endpoint);
    let ingress = runtime.udp_runtime().peer_ingress()?;
    let group = context.peers.clone();
    let retirement = admission.retirement();
    let rejected = retirement.clone();
    let host = runtime
        .host_control()
        .ok_or_else(|| std::io::Error::other("missing actual peer Scope"))?;
    let registrar = host.peer_registrar();
    let structured = context.limits.max_tasks_per_peer.get() > 1;
    let accepted = context.peers.spawn_bound(host, move |binding| {
        let resources = group.resource_context(binding.clone(), ingress);
        async move {
            let stopped = binding.cancellation();
            let route = if structured {
                let task = binding.wait_bound().await.map_err(std::io::Error::other)?;
                let witness = binding
                    .join_witness()
                    .ok_or_else(|| std::io::Error::other("missing original peer task witness"))?;
                let scope = admission
                    .bind_scope(&registrar, task, witness, stopped.clone())
                    .map_err(std::io::Error::other)?;
                let tasks = scope.tasks();
                // Same retained original is installed before protocol preparation.
                resources
                    .claim(
                        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                        |_| {
                            binding.install_scope(scope.clone())?;
                            Ok((
                                zero_platform_tokio::contract::lifecycle::erase_lifecycle(scope),
                                (),
                            ))
                        },
                    )
                    .await
                    .map_err(std::io::Error::other)?;
                route
                    .with_accepted_tasks(tasks)
                    .map_err(std::io::Error::other)?
            } else {
                route
                    .prepare_datagram_peer(resources, retirement, stopped.clone())
                    .await?
            };
            let result = tokio::select! { biased;
                _ = stopped.cancelled() => Ok(()),
                result = route.complete_request(pending, cancel) => result,
            };
            if let Err(error) = result {
                tracing::debug!(%error, "standard UDP peer ended");
            }
            Ok(())
        }
    })?;
    if !accepted {
        rejected.release_after_cleanup();
    } // No task or preparation ever started.
    Ok(())
}

/// A real native multiplexed connection has no invented TCP/UDP accepted entry.
pub(super) fn spawn_carrier(
    runtime: &InboundListenerRuntime,
    factory: &InboundRouteRuntimeFactory,
    pending: PendingInbound,
    cancel: CancellationToken,
) -> Result<(), EngineError> {
    let context = runtime
        .datagram_source()
        .filter(|c| c.carrier.is_some())
        .ok_or_else(|| std::io::Error::other("undeclared carrier source"))?;
    let facts = pending
        .carrier_facts()
        .cloned()
        .ok_or_else(|| std::io::Error::other("missing carrier facts"))?;
    let local = facts
        .local_endpoint
        .ok_or_else(|| std::io::Error::other("missing original carrier endpoint"))?;
    if context.carrier.as_ref().and_then(|c| c.get()) != Some(&local) {
        return Err(
            std::io::Error::other("carrier differs from original metadata endpoint").into(),
        );
    }
    let route = factory
        .for_connection(facts.effective_source)
        .with_local_addr(Some(local));
    let ingress = runtime.udp_runtime().peer_ingress()?;
    let group = context.peers.clone();
    let host = runtime
        .host_control()
        .ok_or_else(|| std::io::Error::other("missing carrier Scope"))?;
    let registrar = host.peer_registrar();
    let limits = context.limits;
    #[cfg(test)]
    let actual_remote=facts.effective_source;
    let admitted=context.peers.spawn_bound(host,move |binding| {
        let resources=group.resource_context(binding.clone(),ingress);
        async move {
            let stopped=binding.cancellation();
            let task=binding.wait_bound().await.map_err(std::io::Error::other)?;
            #[cfg(test)]
            eprintln!("actual carrier original peer task {task:?} remote={actual_remote:?} local={local:?}");
            let witness=binding.join_witness().ok_or_else(||std::io::Error::other("carrier original witness absent"))?;
            let graph=zero_platform_tokio::AcceptedPeerScope::for_carrier(&registrar,task,witness,stopped.clone(),limits.max_tasks_per_peer,limits.max_resources_per_peer,local).map_err(std::io::Error::other)?;
            let tasks=graph.tasks();
            resources.claim(tokio::time::Instant::now()+std::time::Duration::from_secs(5),|_| {binding.install_scope(graph.clone())?;Ok((zero_platform_tokio::contract::lifecycle::erase_lifecycle(graph),()))}).await.map_err(std::io::Error::other)?;
            let route=route.with_accepted_tasks(tasks).map_err(std::io::Error::other)?;
            tokio::select!{biased;_=stopped.cancelled()=>Ok(()),result=route.complete_request(pending,cancel)=>{if let Err(error)=result{tracing::debug!(%error,"standard carrier ended");}Ok(())}}
        }
    })?;
    if !admitted {
        // No primary was registered. The sealed incoming is returned; its
        // actual Quinn driver remains in the original endpoint ledger.
        tracing::debug!("inbound carrier capacity exhausted");
    }
    Ok(())
}
