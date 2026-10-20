//! A closed fault rejects the candidate while enabling real old-config recovery.
use super::*;
pub(super) async fn wait_listener_stopped(
    runtime_factory: &InboundListenerRuntimeFactory,
    tag: &str,
    state: &mut InboundReconcileState<'_>,
) -> Result<(), EngineError> {
    let Some(receipt) = runtime_factory
        .owners()
        .confirm_shutdown(
            tag,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await?
    else {
        return Ok(());
    };
    // Physical absence must be truthful even if reporting fails. Otherwise the
    // compensation pass could mistake the closed old listener for a live one.
    state.listener_stops.remove(tag);
    state.active_inbounds.remove(tag);
    if receipt.terminal().joined_tasks() > 0 && receipt.terminal().failure().is_none() {
        *state.expected_listener_exits = state.expected_listener_exits.saturating_add(1);
    }
    runtime_factory
        .owners()
        .release_closed_reported(receipt, |terminal| {
            runtime_factory.publish_inbound_terminal(tag, terminal);
            Ok(())
        })
}
