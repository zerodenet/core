//! Terminal server reports retain actual failures independently of retirement.
//! Only payloads are retained: no IO, task, Scope or close authority.
use std::{
    io,
    sync::{Arc, Mutex},
};
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceFailure, TaskFailure},
    ChannelError,
};
#[derive(Default)]
pub(crate) struct ReportReceiver(Mutex<Failures>);
#[derive(Default)]
struct Failures {
    tasks: Vec<Arc<TaskFailure>>,
    resources: Vec<Arc<ResourceFailure>>,
}
pub(crate) struct ReportObservation {
    pub(crate) complete: bool,
    result: io::Result<()>,
}
impl ReportObservation {
    pub(crate) fn into_result(self) -> io::Result<()> {
        self.result
    }
}
impl ReportReceiver {
    pub(crate) fn receive(
        self: &Arc<Self>,
        label: &'static str,
        mut report: CloseReport,
    ) -> ReportObservation {
        let complete = report.is_complete();
        // The actual report producer has stopped admission. Repeated shutdown
        // observations deduplicate original task/source and RF identities.
        for failure in report.failed_tasks.drain(..) {
            let failure = Arc::new(failure);
            {
                let mut stored = self
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !stored.tasks.iter().any(|same| {
                    same.id == failure.id
                        && same.class == failure.class
                        && same_error(&same.error, &failure.error)
                }) {
                    stored.tasks.push(failure.clone());
                }
            }
            if let Some(receipt) = failure.error.cleanup_failure_receipt() {
                struct Original<'a>(&'a ChannelError);
                impl AsRef<ChannelError> for Original<'_> {
                    fn as_ref(&self) -> &ChannelError {
                        self.0
                    }
                }
                // Strong actual row stored first. This is local fault delivery,
                // never Scope registration ACK, native close or credit release.
                receipt.acknowledge_retained(&Original(&failure.error));
            }
        }
        for failure in report.resource_failures.drain(..) {
            {
                let mut stored = self
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !stored
                    .resources
                    .iter()
                    .any(|same| Arc::ptr_eq(same, &failure))
                {
                    stored.resources.push(failure.clone());
                }
            }
            failure.acknowledge_retained(&failure);
        }
        let first = {
            let stored = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            stored
                .tasks
                .first()
                .cloned()
                .map(First::Task)
                .or_else(|| stored.resources.first().cloned().map(First::Resource))
        };
        let result = if complete && first.is_none() {
            Ok(())
        } else {
            Err(io::Error::other(ReportFailure {
                label,
                failures: self.clone(),
                first,
                report,
            }))
        };
        ReportObservation { complete, result }
    }
}
fn same_error(mut a: &ChannelError, mut b: &ChannelError) -> bool {
    while let Some(source) = a.retained_diagnostic() {
        a = source;
    }
    while let Some(source) = b.retained_diagnostic() {
        b = source;
    }
    std::ptr::eq(a, b)
}
struct ReportFailure {
    label: &'static str,
    failures: Arc<ReportReceiver>,
    first: Option<First>,
    report: CloseReport,
}
impl std::fmt::Debug for ReportFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for ReportFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never format foreign source payloads while holding the receiver lock.
        let (tasks, resources) = {
            let s = self
                .failures
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (s.tasks.len(), s.resources.len())
        };
        write!(
            f,
            "{} cleanup: {} task faults, {} resource faults, {} unfinished",
            self.label,
            tasks,
            resources,
            self.report.unfinished.len()
        )
    }
}
enum First {
    Task(Arc<TaskFailure>),
    Resource(Arc<ResourceFailure>),
}
impl std::error::Error for ReportFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.first.as_ref()? {
            First::Task(task) => Some(&task.error),
            First::Resource(resource) => Some(&*resource.source),
        }
    }
}
#[cfg(test)]
mod tests;
