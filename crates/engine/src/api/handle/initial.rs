//! The genuine owned initial API retains its original envelope and same book.
use super::*;
pub(super) struct InitialEvent {
    pub(super) event: RawApiEvent,
    _storage: crate::observability::SubscriberStorage,
}
pub(super) fn prepare(
    engine: &Engine,
    registration: &crate::observability::SubscriberRegistration,
) -> std::io::Result<InitialEvent> {
    // Storage owners precede every original view/field guard: every success,
    // refusal or serialization failure drops guards before returning capacity.
    let mut storage = registration.storage();
    let mut workspace = registration.storage();
    storage.grow(std::mem::size_of::<InitialEvent>())?;
    let guard = engine.control_flow_snapshot();
    let view = guard.freeze_subscriber(&mut workspace)?;
    let event = view.materialize_subscriber(&mut storage)?;
    drop(view);
    drop(guard);
    drop(workspace);
    Ok(InitialEvent {
        event,
        _storage: storage,
    })
}

#[cfg(test)]
mod tests;
