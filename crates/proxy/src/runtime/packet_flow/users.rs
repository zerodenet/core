//! Same-endpoint transferred views and original caller joins are separate facts.
mod caller;
mod stream;
pub(super) use caller::FlowCaller;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
pub(super) use stream::TrackedStream;
use tokio::{
    sync::Notify,
    time::{timeout_at, Instant},
};
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, Unfinished},
        scope::TaskJoinWitness,
        CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    controlled_datagram::metadata::users::{LogicalUserPermit, LogicalUsers, UserAdmission},
};

pub(crate) struct FlowUsers {
    slots: Arc<LogicalUsers<Admission>>,
    state: Mutex<State>,
    changed: Notify,
}
#[derive(Default)]
struct State {
    records: Vec<Arc<Record>>,
    unaccounted: bool,
    first: Option<Arc<ChannelError>>,
}
struct Record {
    // Capacity is returned only when BOTH the view was released and the
    // original task was really joined. Stream Drop alone does not refund it.
    _slot: LogicalUserPermit<Admission>,
    caller: Option<TaskJoinWitness>,
    released: std::sync::atomic::AtomicBool,
}
pub(crate) struct FlowUser {
    owner: Arc<FlowUsers>,
    record: Arc<Record>,
}
struct Admission(CancellationToken);
impl UserAdmission for Admission {
    fn check(&self) -> Result<(), ChannelError> {
        if self.0.is_cancelled() {
            Err(revoked())
        } else {
            Ok(())
        }
    }
}
impl FlowUsers {
    pub(crate) fn new(limit: NonZeroUsize) -> Arc<Self> {
        let cancel = CancellationToken::new();
        Arc::new(Self {
            slots: LogicalUsers::new(limit, cancel.clone(), Admission(cancel)),
            state: Mutex::new(State::default()),
            changed: Notify::new(),
        })
    }
    pub(super) fn acquire(
        self: &Arc<Self>,
        caller: Option<FlowCaller>,
    ) -> Result<FlowUser, ChannelError> {
        self.reap();
        let first = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .first
            .clone();
        if let Some(first) = first {
            self.revoke();
            return Err(crate::component::instance::retained(first));
        }
        let slot = self.slots.acquire()?;
        let record = Arc::new(Record {
            _slot: slot,
            caller: caller.map(|caller| caller.witness),
            released: std::sync::atomic::AtomicBool::new(false),
        });
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if self.cancellation().is_cancelled() {
                return Err(revoked());
            }
            state.records.push(record.clone());
        }
        Ok(FlowUser {
            owner: self.clone(),
            record,
        })
    }
    /// An existing leaf API without a same-owner view token cannot establish
    /// users closure. This does not cancel it or remove its legacy capability.
    pub(crate) fn mark_unaccounted(&self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .unaccounted = true;
        self.changed.notify_waiters();
    }
    pub(crate) fn validates(&self, user: &FlowUser) -> bool {
        std::ptr::eq(self, user.owner.as_ref())
            && !user
                .record
                .released
                .load(std::sync::atomic::Ordering::Acquire)
            && !user
                .record
                .caller
                .as_ref()
                .is_some_and(TaskJoinWitness::is_joined)
            && !self.cancellation().is_cancelled()
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        self.slots.cancellation()
    }
    pub(crate) fn revoke(&self) {
        self.cancellation().cancel();
        self.changed.notify_waiters();
    }
    fn reap(&self) {
        // Never query another owner while holding this ledger's lock.
        let records = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .clone();
        let completed: Vec<_> = records
            .into_iter()
            .filter(|record| {
                record.released.load(std::sync::atomic::Ordering::Acquire)
                    && record
                        .caller
                        .as_ref()
                        .is_some_and(TaskJoinWitness::is_joined)
            })
            .collect();
        let first = completed.iter().find_map(|record| {
            record
                .caller
                .as_ref()
                .and_then(TaskJoinWitness::try_joined)
                .and_then(|joined| joined.result().err())
        });
        let completed: std::collections::HashSet<_> = completed
            .iter()
            .map(|record| Arc::as_ptr(record) as usize)
            .collect();
        let removed = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.first.is_none() {
                state.first = first;
            }
            let mut removed = Vec::new();
            state.records.retain(|record| {
                if completed.contains(&(Arc::as_ptr(record) as usize)) {
                    removed.push(record.clone());
                    false
                } else {
                    true
                }
            });
            removed
        };
        drop(removed);
    }
    /// Only logical view release and original caller joins. No native/socket,
    /// worker-scope ACK, recovery or replacement authorization is issued here.
    pub(crate) async fn shutdown(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.reap();
            let (records, unaccounted, first) = {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                (
                    state.records.clone(),
                    state.unaccounted,
                    state.first.clone(),
                )
            };
            if let Some(first) = first {
                return Err(crate::component::instance::retained(first));
            }
            if unaccounted || records.iter().any(|record| record.caller.is_none()) {
                let mut report = CloseReport::default();
                report.unfinished.push(Unfinished::Io(format!(
                    "packet endpoint users lack original caller provenance ({} retained views, unaccounted leaf: {unaccounted})", records.len()
                )));
                return Ok(report);
            }
            if records.is_empty() {
                return Ok(CloseReport::default());
            }
            // Join observation neither consumes the ingress Scope receipt nor
            // ACKs its failure. That source remains owned by the actual host.
            let caller = records.iter().find_map(|record| {
                record
                    .caller
                    .as_ref()
                    .filter(|witness| !witness.is_joined())
                    .cloned()
            });
            timeout_at(deadline, async {
                match caller {
                    Some(caller) => {
                        tokio::select! { _ = caller.wait() => {}, _ = &mut changed => {} }
                    }
                    None => changed.await,
                }
            })
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                    .with_source(source)
            })?;
        }
    }
}
impl Drop for FlowUser {
    fn drop(&mut self) {
        self.record
            .released
            .store(true, std::sync::atomic::Ordering::Release);
        self.owner.changed.notify_waiters();
    }
}
fn revoked() -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session)
}
#[cfg(test)]
mod tests;
