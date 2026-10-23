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
        state.originals.retain(|id, _| state.owner.retains(*id));
        state.bindings.retain(|sequence, binding| {
            !binding.retired() || state.originals.values().any(|owner| owner == sequence)
        });
    }
    /// Regular operation may consume only actual receipts already in the owner;
    /// it does not close another logical session or claim a task has joined.
    pub(crate) fn take_closed_io(&self) -> CloseReport {
        let mut state = self.state();
        self.collect(&mut state);
        confirmed(&mut state)
    }
    /// Host-only terminal cleanup, after the same actual peer's joined receipt.
    /// Original owners/source survive errors and cancelled outer waiters.
    pub(crate) fn shutdown_io(&self) -> Result<CloseReport, ChannelError> {
        self.revoke();
        let mut state = self.state();
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
    if state.unknown || state.retained_live != 0 {
        report.unfinished.push(Unfinished::Io(
            "original host-bound UDP socket remains unconfirmed".into(),
        ));
    }
    report
}
