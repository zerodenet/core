use super::*;
mod component;

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
    let _ = (protocols, source_dir);
    let shutdown_tx = bound.shutdown_sender(&shutdown_tx);
    let PendingInbound::Leased(lease) = bound;
    let (recipe, view, installed) = lease.promote_definition()?;
    component::spawn(
        recipe,
        view,
        installed,
        runtime_factory,
        inbound,
        shutdown_tx,
        listeners,
        rollback,
    )
}
