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
    delivery: std::sync::Weak<Delivery>,
}
/// Immutable originals only. A returned error must never point to the mutable
/// receiver: its actual Scope can return that error to this same receiver.
struct Delivery {
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
                    stored.delivery = std::sync::Weak::new();
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
                    stored.delivery = std::sync::Weak::new();
                }
            }
            failure.acknowledge_retained(&failure);
        }
        let delivery = {
            let mut stored = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if stored.tasks.is_empty() && stored.resources.is_empty() {
                None
            } else if let Some(delivery) = stored.delivery.upgrade() {
                Some(delivery)
            } else {
                // One immutable projection per actual source version, reused
                // while any returned error still holds it. No controller,
                // Scope, callback or back-reference to this receiver.
                let delivery = Arc::new(Delivery {
                    tasks: stored.tasks.clone(),
                    resources: stored.resources.clone(),
                });
                stored.delivery = Arc::downgrade(&delivery);
                Some(delivery)
            }
        };
        let result = if complete && delivery.is_none() {
            Ok(())
        } else {
            Err(io::Error::other(ReportFailure {
                label,
                delivery,
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
    delivery: Option<Arc<Delivery>>,
    report: CloseReport,
}
impl std::fmt::Debug for ReportFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for ReportFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (tasks, resources) = self.delivery.as_ref().map_or((0, 0), |delivery| {
            (delivery.tasks.len(), delivery.resources.len())
        });
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
impl std::error::Error for ReportFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let actual = self.delivery.as_ref()?;
        if let Some(task) = actual.tasks.first() {
            Some(&task.error)
        } else {
            actual
                .resources
                .first()
                .map(|resource| &*resource.source as _)
        }
    }
}

#[cfg(test)]
mod tests;
