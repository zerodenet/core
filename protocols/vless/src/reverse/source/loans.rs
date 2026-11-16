//! Original logical controllers survive cancelled Source and cleanup waiters.
use super::*;
use std::sync::Mutex;
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, ResourceLifecycle, Unfinished},
        CloseReason,
    },
    controlled_carrier::loan::LoanOwner,
};
#[derive(Default)]
pub(super) struct Loans {
    owners: Mutex<Vec<Arc<tokio::sync::Mutex<LoanOwner>>>>,
    receipts: Mutex<CloseReport>,
    failure: Mutex<Option<Arc<ChannelError>>>,
}
impl Loans {
    pub(super) fn install(&self, owner: LoanOwner) {
        self.owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::new(tokio::sync::Mutex::new(owner)));
    }
    pub(super) fn reap(&self) -> Result<(), ChannelError> {
        let originals = self
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(source) = self.failure.lock().unwrap().clone() {
            return Err(ChannelError::retained(source));
        }
        for original in originals {
            let Ok(mut owner) = original.try_lock() else {
                continue;
            };
            if !owner.view_returned() {
                continue;
            }
            let report = owner.try_close()?;
            let complete = report.is_complete()
                && owner
                    .closed()
                    .is_some_and(|receipt| receipt.matches(&owner));
            drop(owner);
            self.receive(report);
            if complete {
                self.owners
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .retain(|entry| !Arc::ptr_eq(entry, &original));
            }
        }
        self.failure
            .lock()
            .unwrap()
            .clone()
            .map_or(Ok(()), |source| Err(ChannelError::retained(source)))
    }
    fn receive(&self, mut report: CloseReport) {
        let mut saved = self.receipts.lock().unwrap();
        for task in report.failed_tasks.drain(..) {
            let source = Arc::new(task.error);
            self.failure
                .lock()
                .unwrap()
                .get_or_insert_with(|| source.clone());
            saved
                .failed_tasks
                .push(zero_platform_tokio::contract::lifecycle::TaskFailure {
                    id: task.id,
                    class: task.class,
                    error: ChannelError::retained(source),
                });
        }
        let resources = std::mem::take(&mut report.resource_failures);
        for source in &resources {
            self.failure
                .lock()
                .unwrap()
                .get_or_insert_with(|| Arc::new(ChannelError::retained(source.source.clone())));
            saved.resource_failures.push(source.clone());
        }
        drop(saved);
        for source in resources {
            source.acknowledge_retained(&source);
        }
    }
}
pub(super) struct Owner {
    pub(super) loans: Arc<Loans>,
    pub(super) cancel: CancellationToken,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        if self.cancel.is_cancelled() {
            Err(failure(Reason::Revoked))
        } else {
            Ok(())
        }
    }
    fn revoke(&self, reason: CloseReason) {
        self.cancel.cancel();
        let originals = self
            .loans
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for owner in originals {
            if let Ok(owner) = owner.try_lock() {
                owner.cancel(reason);
            }
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        let originals = self
            .loans
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut combined = CloseReport::default();
        for original in originals {
            let mut owner = tokio::time::timeout_at(deadline, original.lock())
                .await
                .map_err(|_| failure(Reason::Timeout))?;
            let report = owner.close(deadline).await?;
            let complete = report.is_complete()
                && owner
                    .closed()
                    .is_some_and(|receipt| receipt.matches(&owner));
            self.loans.receive(report);
            if complete {
                self.loans
                    .owners
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .retain(|entry| !Arc::ptr_eq(entry, &original));
            }
        }
        combined.append(std::mem::take(&mut *self.loans.receipts.lock().unwrap()));
        if !self
            .loans
            .owners
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
        {
            combined.unfinished.push(Unfinished::Resource(
                "Rvs logical controllers or returned views remain pending".into(),
            ));
        }
        // Donor native receipts and original donor TaskScope joins are separate.
        Ok(combined)
    }
}
