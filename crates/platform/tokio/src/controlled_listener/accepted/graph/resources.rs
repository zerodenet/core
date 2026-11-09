//! Original auxiliary owners remain in the accepted entry across cancellation.
//! This is custody only: native acquisition still needs its declared host grant.
use super::*;
use crate::contract::lifecycle::{CloseReport, DynResourceLifecycle};
mod logical;
pub use logical::AcceptedResource;

/// Totals by the finite declared ordinal, never per-client/resource history.
/// Actual failed task receipts retain their original identities and sources.
#[derive(Default)]
pub(in crate::controlled_listener::accepted) struct ChildReports {
    counts: BTreeMap<usize, [usize; 3]>,
    tasks: CloseReport,
}
impl ChildReports {
    pub(in crate::controlled_listener::accepted) fn append(
        &mut self,
        ordinal: usize,
        mut report: CloseReport,
    ) {
        let counts = self.counts.entry(ordinal).or_default();
        counts[0] = counts[0].saturating_add(report.closed_io.len());
        counts[1] = counts[1].saturating_add(report.released_resources.len());
        counts[2] = counts[2].saturating_add(report.recovered_resources.len());
        // Scope owns actual successful task joins. Auxiliary resource owners
        // cannot create another per-session task-history registry here.
        self.tasks.failed_tasks.append(&mut report.failed_tasks);
    }
    pub(in crate::controlled_listener::accepted) fn take(&mut self) -> CloseReport {
        let mut report = std::mem::take(&mut self.tasks);
        for (ordinal, [io, released, recovered]) in std::mem::take(&mut self.counts) {
            if io != 0 {
                report.closed_io.push(format!(
                    "accepted auxiliary original {ordinal}: {io} native I/O close receipts"
                ));
            }
            if released != 0 {
                report.released_resources.push(format!(
                    "accepted auxiliary original {ordinal}: {released} resource-return receipts"
                ));
            }
            if recovered != 0 {
                report.recovered_resources.push(format!(
                    "accepted auxiliary original {ordinal}: {recovered} recovery receipts"
                ));
            }
        }
        report
    }
}

type TaskOrigin = Arc<OnceLock<(TaskId, TaskJoinWitness)>>;
pub(super) struct Retained {
    pub(super) ordinal: usize,
    pub(super) owner: AsyncMutex<Option<Box<dyn DynResourceLifecycle>>>,
    pub(super) complete: AtomicBool,
    pub(super) logical_owner: Option<TaskOrigin>,
    pub(super) driver: OnceLock<TaskOrigin>,
    cleanup_delivery: OnceLock<crate::contract::RetainedError>,
}
impl Retained {
    pub(super) fn complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
}
impl AcceptedCaller {
    /// Install before acquiring native I/O or awaiting preparation. The limit
    /// belongs to the actual listener declaration, not a caller-selected bound.
    pub fn retain_resource(
        &self,
        ordinal: usize,
        owner: Box<dyn DynResourceLifecycle>,
    ) -> Result<(), ChannelError> {
        self.check_open()?;
        let users = self.users.upgrade().ok_or_else(gone)?;
        let mut state = users.state();
        if state.sealed
            || ordinal >= users.authority.max_resources
            || state
                .resources
                .iter()
                .any(|resource| resource.ordinal == ordinal)
        {
            return Err(failure(
                Stage::Prepare,
                Reason::Capacity,
                "accepted resource custody exhausted",
            ));
        }
        state.resources.push(Arc::new(Retained {
            ordinal,
            owner: AsyncMutex::new(Some(owner)),
            complete: AtomicBool::new(false),
            logical_owner: None,
            driver: OnceLock::new(),
            cleanup_delivery: OnceLock::new(),
        }));
        Ok(())
    }
}
fn gone() -> ChannelError {
    failure(
        Stage::Prepare,
        Reason::Revoked,
        "accepted resource owner retired",
    )
}

impl Users {
    pub(super) fn revoke_resources(&self) {
        let resources = self.state().resources.clone();
        for resource in resources {
            if let Ok(slot) = resource.owner.try_lock() {
                if let Some(owner) = slot.as_ref() {
                    owner.revoke(crate::contract::CloseReason::Shutdown);
                }
            }
            // A locked slot is already in shutdown, which revoked it before
            // awaiting. Revocation only wakes I/O; it supplies no close proof.
        }
    }
    pub(super) async fn close_resources(&self, deadline: Instant) -> bool {
        let resources = self.state().resources.clone();
        for resource in resources {
            if !self.close_resource(&resource, deadline).await {
                return false;
            }
        }
        true
    }
    pub(super) async fn close_resource(&self, resource: &Retained, deadline: Instant) -> bool {
        if resource.complete() {
            return true;
        }
        let Ok(mut slot) = tokio::time::timeout_at(deadline, resource.owner.lock()).await else {
            return false;
        };
        let Some(owner) = slot.as_mut() else {
            return resource.complete();
        };
        owner.revoke(crate::contract::CloseReason::Shutdown);
        let mut report = match owner.shutdown(deadline).await {
            Ok(report) => report,
            Err(error) => {
                // The actual stable child source remains in this same finite
                // resource row before local receipt acceptance. This is not
                // native closure or either task/Host acknowledgement.
                let delivery = error.retained_diagnostic().cloned();
                let received = delivery.as_ref().is_some_and(|source| {
                    source.cleanup_failure_receipt().is_some() && {
                        let actual = resource.cleanup_delivery.get_or_init(|| source.clone());
                        actual.ptr_eq(source)
                    }
                });
                self.remember(Arc::new(error));
                if received {
                    let source = delivery.as_ref().unwrap();
                    let _ = source
                        .cleanup_failure_receipt()
                        .unwrap()
                        .acknowledge_retained(source);
                }
                return false;
            }
        };
        let task_failure = if report.failed_tasks.is_empty() {
            None
        } else {
            let actual = report.failed_tasks.remove(0);
            let source = Arc::new(actual.error);
            report.failed_tasks.insert(
                0,
                crate::contract::lifecycle::TaskFailure {
                    id: actual.id,
                    class: actual.class,
                    error: retained(source.clone()),
                },
            );
            Some(source)
        };
        let complete = report.is_complete() && task_failure.is_none();
        // Persist positive original receipts before another await; current
        // unfinished state remains charged to this exact retained owner.
        report.unfinished.clear();
        if !self.append_report(resource.ordinal, report) {
            return false;
        }
        if let Some(source) = task_failure {
            self.remember(source);
            return false;
        }
        if !complete {
            return false;
        }
        let original = slot.take();
        resource.complete.store(true, Ordering::Release);
        if resource.logical_owner.is_some() {
            self.logical_charge(false);
        }
        drop(slot);
        drop(original);
        true
    }
}

#[cfg(test)]
mod tests;
