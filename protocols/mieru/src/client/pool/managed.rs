//! One actual pool's explicit native owner; cache retirement never releases a budget lease.
use super::ClientPool;
mod record;
pub(crate) use record::CarrierLease;
use record::CarrierRecord;
use std::{
    io,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    network::UpstreamTransport, scope::TaskRegistrar, CancellationToken,
};

#[derive(Default)]
pub(crate) enum Ownership {
    #[default]
    Pristine,
    LegacyUsed,
    Managed(Arc<ManagedState>),
}
pub(crate) struct ManagedState {
    pub(crate) instance: ComponentInstanceId,
    pub(crate) tasks: TaskRegistrar,
    pub(crate) cancel: CancellationToken,
    pub(crate) scope_cancel: CancellationToken,
    pub(crate) upstream: UpstreamTransport,
    limit: NonZeroUsize,
    state: Mutex<State>,
    changed: Notify,
}
#[derive(Default)]
struct State {
    records: Vec<Arc<CarrierRecord>>,
    stopped: bool,
}
impl ClientPool {
    pub(super) fn enter_legacy(&self) -> io::Result<()> {
        let mut owner = self.ownership.lock().unwrap();
        match &*owner {
            Ownership::Pristine | Ownership::LegacyUsed => {
                *owner = Ownership::LegacyUsed;
                Ok(())
            }
            Ownership::Managed(_) => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "mieru pool belongs to a native generation; legacy dial is forbidden",
            )),
        }
    }
    pub(crate) fn claim_managed(
        &self,
        instance: ComponentInstanceId,
        limit: NonZeroUsize,
        tasks: TaskRegistrar,
        cancel: CancellationToken,
        upstream: UpstreamTransport,
    ) -> io::Result<Arc<ManagedState>> {
        let mut ownership = self.ownership.lock().unwrap();
        if !matches!(*ownership, Ownership::Pristine) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "mieru pool was already used or claimed; no implicit legacy driver transfer",
            ));
        }
        let scope_cancel = tasks.cancellation();
        if cancel.is_cancelled() || scope_cancel.is_cancelled() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let owner = Arc::new(ManagedState {
            instance,
            limit,
            tasks,
            cancel,
            scope_cancel,
            upstream,
            state: Mutex::new(State::default()),
            changed: Notify::new(),
        });
        *ownership = Ownership::Managed(owner.clone());
        Ok(owner)
    }
    pub(crate) fn managed(&self, instance: ComponentInstanceId) -> io::Result<Arc<ManagedState>> {
        let ownership = self.ownership.lock().unwrap();
        match &*ownership {
            Ownership::Managed(owner) if owner.instance == instance && owner.ready() => {
                Ok(owner.clone())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "mieru native pool dependency is not ready for this generation",
            )),
        }
    }
}
impl ManagedState {
    pub(crate) fn ready(&self) -> bool {
        !self.cancel.is_cancelled()
            && !self.scope_cancel.is_cancelled()
            && !self.state.lock().unwrap().stopped
    }
    pub(crate) fn reserve(self: &Arc<Self>) -> io::Result<CarrierLease> {
        let mut state = self.state.lock().unwrap();
        if state.stopped || self.cancel.is_cancelled() || self.scope_cancel.is_cancelled() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if state.records.len() >= self.limit.get() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "mieru native carrier budget exhausted",
            ));
        }
        let record = Arc::new(CarrierRecord::new());
        state.records.push(record.clone());
        Ok(CarrierLease::new(self.clone(), record))
    }
    pub(crate) fn stop(&self) {
        self.state.lock().unwrap().stopped = true;
        self.cancel.cancel();
        self.changed.notify_waiters();
    }
    pub(crate) fn physical_unconfirmed(&self) -> bool {
        self.state
            .lock()
            .unwrap()
            .records
            .iter()
            .any(|record| record.io_attempted())
    }
    pub(crate) async fn drained(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let records = self.state.lock().unwrap().records.clone();
            let mut waiting = false;
            for record in records {
                if !record.body_finished() {
                    waiting = true;
                    break;
                }
                record.joined().await;
            }
            if !waiting {
                return;
            }
            changed.await;
        }
    }
    fn release_unattempted(&self, record: &Arc<CarrierRecord>) {
        self.state
            .lock()
            .unwrap()
            .records
            .retain(|stored| !Arc::ptr_eq(stored, record));
    }
}
