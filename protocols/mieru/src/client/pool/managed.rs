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
use zero_platform_tokio::controlled_carrier::CarrierGrants;

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
    pub(crate) grants: Option<CarrierGrants>,
    pub(crate) limit: NonZeroUsize,
    state: Mutex<State>,
    changed: Notify,
}
#[derive(Default)]
struct State {
    records: Vec<Arc<CarrierRecord>>,
    stopped: bool,
    ready: bool,
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
        grants: Option<CarrierGrants>,
    ) -> io::Result<Arc<ManagedState>> {
        if grants
            .as_ref()
            .is_some_and(|grants| grants.instance() != instance)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Mieru controlled grants belong to another component generation",
            ));
        }
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
            grants,
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
        !self.cancel.is_cancelled() && !self.scope_cancel.is_cancelled() && {
            let state = self.state.lock().unwrap();
            state.ready && !state.stopped
        }
    }
    pub(crate) fn activate(&self) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        let mut state = self.state.lock().unwrap();
        if state.stopped || self.cancel.is_cancelled() || self.scope_cancel.is_cancelled() {
            let mut error = zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::Revoked,
                zero_platform_tokio::contract::Impact::Component,
            );
            error.component = Some(self.instance.component);
            return Err(error);
        }
        state.ready = true;
        Ok(())
    }
    pub(crate) fn reserve(self: &Arc<Self>) -> io::Result<CarrierLease> {
        self.harvest_closed();
        let mut state = self.state.lock().unwrap();
        if !state.ready
            || state.stopped
            || self.cancel.is_cancelled()
            || self.scope_cancel.is_cancelled()
        {
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
    pub(crate) fn harvest_closed(&self) {
        self.state
            .lock()
            .unwrap()
            .records
            .retain(|record| !record.closed(self.instance));
    }
    pub(crate) async fn retire_finished(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<bool, zero_platform_tokio::contract::ChannelError> {
        let records = self.state.lock().unwrap().records.clone();
        let mut complete = true;
        for record in records {
            if record.body_finished() {
                if let Some(adoption) = record.adoption() {
                    complete &= adoption.retire(deadline).await?.is_complete();
                }
            }
        }
        self.harvest_closed();
        Ok(complete)
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
