//! Actual original task results accompany full settlement; no task is invented.
use super::*;
use crate::contract::{lifecycle::TaskFailure, scope::TaskJoined, ChannelError};
pub(super) fn append(slot: &Slot, report: &mut CloseReport) {
    // A second observation after removal cannot emit these results again.
    if slot.removed.load(std::sync::atomic::Ordering::Acquire) {
        return;
    }
    let (auxiliary, driver) = {
        let state = slot.state();
        (
            state.auxiliary_exit.clone(),
            state.driver.as_ref().map(|(_, task)| task.clone()),
        )
    };
    for joined in auxiliary
        .into_iter()
        .chain(driver.and_then(|actual| actual.try_joined()))
    {
        append_joined(report, joined);
    }
}
fn append_joined(report: &mut CloseReport, joined: TaskJoined) {
    if !report.exited_tasks.contains(&joined.id()) {
        report.exited_tasks.push(joined.id());
    }
    if let Err(source) = joined.result() {
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
