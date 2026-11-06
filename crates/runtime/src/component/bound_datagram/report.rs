use super::*;
use zero_platform_tokio::contract::lifecycle::{CloseReport, Unfinished};
impl HostBoundDatagramResources {
    pub(super) fn collect(&self, state: &mut State) {
        let native = state.owner.take_report();
        // Commit confirmations before returning a slot or calling a fence.
        state.pending_closed += native.confirmed_closed;
        state.unknown = !native.unconfirmed.is_empty();
        state.retained_live = native.retained_live;
        if let Some(unknown) = native.unconfirmed.first() {
            state.fault.get_or_insert_with(|| {
                Arc::new(
                    error(
                        self.instance,
                        Reason::Io,
                        "original host UDP close/acquisition remains unknown",
                    )
                    .with_source(unknown.source.clone()),
                )
            });
        }
        let removed: Vec<_> = state
            .originals
            .keys()
            .copied()
            .filter(|id| {
                let original = &state.originals[id];
                !state.owner.retains(*id)
                    && original.view.as_ref().is_none_or(|view| {
                        view.is_returned() && state.internal_driver_joined != Some(false)
                    })
            })
            .collect();
        for id in removed {
            if let Some(original) = state.originals.remove(&id) {
                state.retired_originals.push(original);
            }
        }
        state.bindings.retain(|sequence, binding| {
            !binding.retired()
                || state
                    .originals
                    .values()
                    .any(|owner| owner.binding == *sequence)
        });
    }
    /// Regular operation may consume only actual receipts already in the owner;
    /// it does not close another logical session or claim a task has joined.
    pub fn take_closed_io(&self) -> CloseReport {
        let mut state = self.state();
        self.collect(&mut state);
        confirmed(&mut state)
    }
    /// The actual scoped provider waits for its original wire join while the
    /// containing host independently consumes that same Scope's real exits.
    pub(in crate::component) async fn shutdown_scoped_io(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.revoke();
        let driver = self
            .driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(driver) = driver {
            if tokio::time::timeout_at(deadline, driver.wait())
                .await
                .is_err()
            {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(
                        "actual host-bound UDP wire join remains pending".into(),
                    )],
                    ..Default::default()
                });
            }
        }
        self.shutdown_io()
    }
    /// Host-only terminal cleanup, after the same actual peer's joined receipt.
    /// Original owners/source survive errors and cancelled outer waiters.
    pub fn shutdown_io(&self) -> Result<CloseReport, ChannelError> {
        self.revoke();
        if self.scope.is_some() {
            let driver = self
                .driver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if driver.as_ref().is_some_and(|witness| !witness.is_joined()) {
                let mut failure = error(
                    self.instance,
                    Reason::Revoked,
                    "original UDP driver has not been consumed by its Scope",
                );
                failure.stage = Stage::Close;
                return Err(failure);
            }
        }
        if self.state().originals.values().any(|original| {
            original
                .view
                .as_ref()
                .is_some_and(|view| !view.is_returned())
        }) {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(
                    "original host-bound UDP view retained".into(),
                )],
                ..Default::default()
            });
        }
        let controls: Vec<_> = self
            .state()
            .originals
            .values()
            .filter_map(|original| original.control.clone())
            .collect();
        // Same retained controllers, outside the enclosing owner gate: native
        // retirement can synchronously wake/drop the caller's original wakers.
        for control in &controls {
            let _ = control.close();
        }
        let mut state = self.state();
        self.collect(&mut state);
        let native = state.owner.close_all();
        state.pending_closed += native.confirmed_closed;
        let complete = native.is_complete();
        // Do not remove a sticky task/component fault just because all known
        // originals close. The containing host separately retains that source.
        self.collect(&mut state);
        if !complete {
            let source = state.fault.as_ref().cloned().unwrap_or_else(|| {
                Arc::new(error(
                    self.instance,
                    Reason::Io,
                    "original host UDP outcome remains unknown",
                ))
            });
            let mut failure = error(
                self.instance,
                Reason::Io,
                "host UDP native cleanup unfinished",
            )
            .with_source(source);
            failure.stage = Stage::Close;
            failure.delivery = DeliveryState::Unknown;
            return Err(failure);
        }
        Ok(confirmed(&mut state))
    }
}
fn confirmed(state: &mut State) -> CloseReport {
    let mut report = CloseReport::default();
    let count = std::mem::take(&mut state.pending_closed);
    if count != 0 {
        report.closed_io.push(format!(
            "explicitly closed {count} original host-bound UDP sockets"
        ));
    }
    // Inspection never consumes or substitutes an unknown original outcome.
    if !state.originals.is_empty() {
        report.unfinished.push(Unfinished::Resource(format!(
            "{} original host-bound UDP view/driver responsibilities remain",
            state.originals.len()
        )));
    }
    if state.unknown || state.retained_live != 0 {
        report.unfinished.push(Unfinished::Io(
            "original host-bound UDP socket remains unconfirmed".into(),
        ));
    }
    report
}
