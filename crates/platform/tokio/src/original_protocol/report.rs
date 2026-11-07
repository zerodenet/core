use super::*;
pub(super) fn merge(
    stored: &mut CloseReport,
    incoming: CloseReport,
    retired_failures: &mut Vec<crate::contract::lifecycle::TaskFailure>,
) {
    for failure in incoming.resource_failures {
        if !stored
            .resource_failures
            .iter()
            .any(|actual| Arc::ptr_eq(actual, &failure))
        {
            stored.resource_failures.push(failure);
        }
    }
    for task in incoming.exited_tasks {
        if !stored.exited_tasks.contains(&task) {
            stored.exited_tasks.push(task);
        }
    }
    for failure in incoming.failed_tasks {
        if !stored
            .failed_tasks
            .iter()
            .any(|actual| actual.id == failure.id)
        {
            stored.failed_tasks.push(failure);
        } else {
            retired_failures.push(failure);
        }
    }
    for (stored, incoming) in [
        (&mut stored.closed_io, incoming.closed_io),
        (&mut stored.released_resources, incoming.released_resources),
        (
            &mut stored.recovered_resources,
            incoming.recovered_resources,
        ),
    ] {
        for item in incoming {
            if !stored.contains(&item) {
                stored.push(item);
            }
        }
    }
    // One concrete controller reports this same entire phase; only its next
    // actual result may replace its own unfinished facts. Errors do not erase it.
    stored.unfinished = incoming.unfinished;
}
