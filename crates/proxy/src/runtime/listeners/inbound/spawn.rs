use super::*;
use tracing::error;

pub(in crate::runtime) fn spawn_inbound_listener(
    protocols: &ProtocolInventory,
    source_dir: Option<&Path>,
    runtime_factory: &InboundListenerRuntimeFactory,
    inbound: &InboundConfig,
    bound: crate::protocol_registry::BoundInbound,
    shutdown_tx: watch::Sender<bool>,
    listeners: &mut JoinSet<Result<(), EngineError>>,
) -> Result<(), EngineError> {
    spawn_inbound_listener_with_state(
        protocols,
        source_dir,
        runtime_factory,
        inbound,
        bound,
        shutdown_tx,
        listeners,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_inbound_listener_with_state(
    protocols: &ProtocolInventory,
    source_dir: Option<&Path>,
    runtime_factory: &InboundListenerRuntimeFactory,
    inbound: &InboundConfig,
    bound: crate::protocol_registry::BoundInbound,
    shutdown_tx: watch::Sender<bool>,
    listeners: &mut JoinSet<Result<(), EngineError>>,
    rollback: bool,
) -> Result<(), EngineError> {
    let listener_config = runtime_factory.listener_config(inbound);
    let mut operation = (if rollback {
        protocols.prepare_rollback_inbound_listener(listener_config, source_dir)
    } else {
        protocols.prepare_inbound_listener(listener_config, source_dir)
    })
    .map_err(|error| {
        warn!(
            inbound_tag = %inbound.tag,
            protocol = inbound.protocol.protocol_name(),
            listen_address = %inbound.listen.address,
            listen_port = inbound.listen.port,
            reason = "adapter_prepare_error",
            error = %error,
            "inbound listener adapter preparation failed"
        );
        error
    })?;
    // Persistent host ownership is installed synchronously before registration
    // or any activation await. The observer below never owns the actual driver.
    let installed = runtime_factory.owners().install(
        &inbound.tag,
        protocols,
        operation.internal_tasks(),
        shutdown_tx.clone(),
    )?;
    operation.install_resources(&installed)?;
    let owner = installed.owner();
    let inbound_tag = inbound.tag.clone();
    let protocol = inbound.protocol.protocol_name();
    let listen_address = inbound.listen.address.clone();
    let listen_port = inbound.listen.port;
    let listener_runtime = runtime_factory.for_inbound(inbound_tag.clone());

    let shutdown_rx = shutdown_tx.subscribe();
    owner.spawn_driver(async move {
        info!(
            inbound_tag = %inbound_tag,
            protocol = protocol,
            listen_address = %listen_address,
            listen_port = listen_port,
            "inbound listener task started"
        );
        let result = operation
            .execute(listener_runtime, bound, shutdown_rx)
            .await;
        match &result {
            Ok(()) => info!(
                inbound_tag = %inbound_tag,
                protocol = protocol,
                listen_address = %listen_address,
                listen_port = listen_port,
                reason = "listener_task_returned",
                "inbound listener task returned"
            ),
            Err(listener_error) => error!(
                inbound_tag = %inbound_tag,
                protocol = protocol,
                listen_address = %listen_address,
                listen_port = listen_port,
                reason = "listener_task_error",
                error = %listener_error,
                "inbound listener task failed"
            ),
        }
        result
    })?;
    listeners.spawn(async move {
        owner
            .observe_driver()
            .await
            .map_err(|error| owner.observer_error(error))
    });
    Ok(())
}
