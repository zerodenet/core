//! Candidate creation begins only after the batch's StopOld barrier.
use super::super::reservation::bind_inbound_with_retry;
use super::plan::Candidate;
use super::*;

pub(super) async fn apply_candidate(
    candidate: Candidate,
    protocols: &ProtocolInventory,
    source_dir: Option<&Path>,
    runtime_factory: &InboundListenerRuntimeFactory,
    rollback_runtime_factory: &InboundListenerRuntimeFactory,
    state: &mut InboundReconcileState<'_>,
) -> Result<(), EngineError> {
    let Candidate {
        inbound,
        previous,
        prepared,
        ..
    } = candidate;
    let prepared = runtime_factory
        .owners()
        .prepare_recovery_bind(inbound.clone(), source_dir, runtime_factory.is_restoring())?
        .or(prepared);
    let inbound = &inbound;
    let (shutdown_tx, _) = watch::channel(false);
    let bound = super::super::bind::bind_prepared_listener(
        protocols,
        runtime_factory,
        source_dir,
        inbound,
        shutdown_tx.clone(),
        std::num::NonZeroUsize::new(50).unwrap(),
        prepared,
    )
    .await;
    let shutdown_tx = bound
        .as_ref()
        .map(|bound| bound.shutdown_sender(&shutdown_tx))
        .unwrap_or(shutdown_tx);
    let error = match bound {
        Ok(bound) => match async {
            let owner = spawn_inbound_listener(
                protocols,
                source_dir,
                runtime_factory,
                inbound,
                bound,
                shutdown_tx.clone(),
                state.listeners,
            )?;
            // Retain actual candidate metadata before waiting. Cancellation
            // re-enters the same installed owner/nonce instead of binding a
            // second listener or mistaking registration for Ready.
            state
                .listener_stops
                .insert(inbound.tag.clone(), shutdown_tx.clone());
            state
                .active_inbounds
                .insert(inbound.tag.clone(), inbound.clone());
            owner.wait_ready().await
        }
        .await
        {
            Ok(()) => {
                state
                    .listener_stops
                    .insert(inbound.tag.clone(), shutdown_tx);
                state
                    .active_inbounds
                    .insert(inbound.tag.clone(), inbound.clone());
                info!(
                    inbound_tag = %inbound.tag,
                    protocol = inbound.protocol.protocol_name(),
                    listen_address = %inbound.listen.address,
                    listen_port = inbound.listen.port,
                    reason = "config_reconciled",
                    "started new inbound listener"
                );
                return Ok(());
            }
            Err(error) => {
                warn!(
                    inbound_tag = %inbound.tag,
                    protocol = inbound.protocol.protocol_name(),
                    listen_address = %inbound.listen.address,
                    listen_port = inbound.listen.port,
                    reason = "listener_prepare_error",
                    error = %error,
                    "failed to prepare inbound listener"
                );
                error
            }
        },
        Err(error) => {
            warn!(
                inbound_tag = %inbound.tag,
                protocol = inbound.protocol.protocol_name(),
                listen_address = %inbound.listen.address,
                listen_port = inbound.listen.port,
                reason = "listener_bind_error",
                error = %error,
                "failed to bind inbound listener"
            );
            error
        }
    };
    // Preparation may have installed a concrete carrier owner before its
    // driver failed. Confirm that candidate before binding any recovery.
    if runtime_factory.owners().owner(&inbound.tag).is_some() {
        wait_listener_stopped(rollback_runtime_factory, &inbound.tag, state).await?;
    }
    if let Some(previous) = previous {
        let (rollback_tx, _) = watch::channel(false);
        match bind_inbound_with_retry(
            protocols,
            rollback_runtime_factory,
            source_dir,
            &previous,
            rollback_tx.clone(),
        )
        .await
        {
            Ok(bound) => match async {
                let owner = spawn_inbound_listener_with_state(
                    protocols,
                    source_dir,
                    rollback_runtime_factory,
                    &previous,
                    bound,
                    rollback_tx.clone(),
                    state.listeners,
                    true,
                )?;
                state
                    .listener_stops
                    .insert(previous.tag.clone(), rollback_tx.clone());
                state
                    .active_inbounds
                    .insert(previous.tag.clone(), previous.clone());
                owner.wait_ready().await
            }
            .await
            {
                Ok(()) => {
                    info!(
                        inbound_tag = %previous.tag,
                        protocol = previous.protocol.protocol_name(),
                        listen_address = %previous.listen.address,
                        listen_port = previous.listen.port,
                        reason = "reload_rollback",
                        "restored previous inbound listener"
                    );
                    state
                        .listener_stops
                        .insert(previous.tag.clone(), rollback_tx);
                    state.active_inbounds.insert(previous.tag.clone(), previous);
                }
                Err(rollback_error) => {
                    warn!(
                        inbound_tag = %inbound.tag,
                        reason = "rollback_prepare_error",
                        %rollback_error,
                        "failed to prepare previous inbound during rollback"
                    );
                }
            },
            Err(rollback_error) => {
                warn!(
                    inbound_tag = %inbound.tag,
                    reason = "rollback_bind_error",
                    %rollback_error,
                    "failed to rebind previous inbound during rollback"
                );
            }
        }
    }
    Err(error)
}
