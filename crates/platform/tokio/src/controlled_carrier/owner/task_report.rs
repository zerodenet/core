//! Actual original task results accompany full settlement; no task is invented.
use super::*;
use crate::contract::{lifecycle::TaskFailure, scope::TaskJoined, ChannelError};
pub(super) fn append(slot: &Slot, report: &mut CloseReport) {
    // A second observation after removal cannot emit these results again.
    if slot.removed.load(std::sync::atomic::Ordering::Acquire) {
        return;
    }
    let (auxiliary, driver, received_connect_rejection) = {
        let state = slot.state();
        (
            state.auxiliary_exit.clone(),
            state.driver.as_ref().map(|(_, task)| task.clone()),
            state
                .users
                .as_ref()
                .and_then(|users| users.preparation.as_ref())
                .and_then(|preparation| preparation.connect_rejection.get())
                .cloned(),
        )
    };
    for joined in auxiliary
        .into_iter()
        .chain(driver.and_then(|actual| actual.try_joined()))
    {
        let received = joined.class() == crate::contract::lifecycle::TaskClass::Auxiliary
            && received_connect_rejection.as_ref().is_some_and(|actual| {
                actual.id() == joined.id()
                    && actual.registration_released()
                    && actual.failure_accepted()
            });
        append_joined(report, joined, received);
    }
}
fn append_joined(report: &mut CloseReport, joined: TaskJoined, received_connect_rejection: bool) {
    if !report.exited_tasks.contains(&joined.id()) {
        report.exited_tasks.push(joined.id());
    }
    if let Err(source) = joined.result() {
        if received_connect_rejection {
            return;
        }
        if !report
            .failed_tasks
            .iter()
            .any(|actual| actual.id == joined.id())
        {
            report.failed_tasks.push(TaskFailure {
                id: joined.id(),
                class: joined.class(),
                error: ChannelError::retained(source),
            });
        }
    }
}
