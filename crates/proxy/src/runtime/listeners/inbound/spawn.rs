use super::*;
use tracing::error;

pub(in crate::runtime) fn spawn_inbound_listener(
    protocols: &ProtocolInventory,
    source_dir: Option<&Path>,
    runtime_factory: &InboundListenerRuntimeFactory,
    inbound: &InboundConfig,
    bound: PendingInbound,
    shutdown_tx: watch::Sender<bool>,
    listeners: &mut JoinSet<Result<(), EngineError>>,
) -> Result<std::sync::Arc<crate::runtime::inbound_owners::InboundOwner>, EngineError> {
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
    bound: PendingInbound,
    shutdown_tx: watch::Sender<bool>,
    listeners: &mut JoinSet<Result<(), EngineError>>,
    rollback: bool,
) -> Result<std::sync::Arc<crate::runtime::inbound_owners::InboundOwner>, EngineError> {
    let shutdown_tx = bound.shutdown_sender(&shutdown_tx);
    let (mut operation, bound, installed, managed) = match bound {
        PendingInbound::Legacy(bound) => {
            let listener_config = runtime_factory.listener_config(inbound);
            let operation = if rollback {
                protocols.prepare_rollback_inbound_listener(listener_config, source_dir)
            } else {
                protocols.prepare_inbound_listener(listener_config, source_dir)
            }
            .map_err(|error| {
                warn!(inbound_tag = %inbound.tag, protocol = inbound.protocol.protocol_name(),
                    listen_address = %inbound.listen.address, listen_port = inbound.listen.port,
                    reason = "adapter_prepare_error", error = %error,
                    "inbound listener adapter preparation failed");
                error
            })?;
            let installed = runtime_factory.owners().install_listener(
                &inbound.tag,
                protocols,
                operation.internal_tasks(),
                shutdown_tx.clone(),
            )?;
            (operation, bound, installed, false)
        }
        PendingInbound::Leased(lease) => {
            let (operation, view, installed) = lease.promote_native()?;
            let listener = view.into_bound().map_err(|error| {
                let owner = installed.owner();
                let source = if error.reason == zero_platform_tokio::contract::Reason::Revoked
                    && owner.task_access().cancellation().is_cancelled()
                {
                    owner.ready_stopped()
                } else {
                    EngineError::Io(std::io::Error::other(error))
                };
                owner.retain_managed_listener_error(source)
            })?;
            (operation, listener, installed, true)
        }
    };
    let owner = installed.owner();
    if !rollback {
        if let Err(error) = runtime_factory.register_preparing(&owner) {
            return Err(if managed {
                owner.retain_managed_listener_error(error)
            } else {
                owner.retain_unexecuted_bound(bound, error)
            });
        }
    }
    if let Err(error) = operation.install_resources(&installed) {
        return Err(if managed {
            owner.retain_managed_listener_error(error)
        } else {
            owner.retain_unexecuted_bound(bound, error)
        });
    }
    let inbound_tag = inbound.tag.clone();
    let protocol = inbound.protocol.protocol_name();
    let listen_address = inbound.listen.address.clone();
    let listen_port = inbound.listen.port;
    let listener_runtime = runtime_factory
        .for_inbound(inbound_tag.clone())
        .with_readiness(owner.readiness());

    let shutdown_rx = shutdown_tx.subscribe();
    let accepted = owner.spawn_driver(async move {
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
    });
    if let Err(error) = accepted {
        return Err(if managed {
            owner.retain_managed_listener_error(error)
        } else {
            owner.retain_failed_driver_installation(error)
        });
    }
    owner.seal_listener_installation();
    let observing = owner.clone();
    listeners.spawn(async move {
        observing
            .observe_driver()
            .await
            .map_err(|error| observing.observer_error(error))
    });
    Ok(owner)
}
