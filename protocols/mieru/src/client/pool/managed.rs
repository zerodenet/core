//! One actual pool's explicit native owner; cache retirement never releases a budget lease.
use super::ClientPool;
mod protocols;
mod record;
use record::CarrierRecord;
pub(crate) use record::{CarrierLease, PendingCarriers};
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
    pub(crate) resources:
        std::sync::Weak<dyn zero_platform_tokio::contract::component::ResourceAccess>,
    pub(crate) cancel: CancellationToken,
    pub(crate) scope_cancel: CancellationToken,
    pub(crate) identity: Arc<[u8]>,
    pub(crate) carriers: Vec<ManagedCarrier>,
    plans: Arc<[super::profile::CarrierPlan]>,
    profiles: Option<Arc<[super::profile::PoolProfile]>>,
    pub(crate) dynamic: bool,
    pub(crate) limit: NonZeroUsize,
    state: Mutex<State>,
    changed: Notify,
}
pub(crate) struct ManagedBindings {
    pub(crate) resources:
        std::sync::Weak<dyn zero_platform_tokio::contract::component::ResourceAccess>,
    pub(crate) identity: Arc<[u8]>,
    pub(crate) carriers: Vec<ManagedCarrier>,
    pub(crate) plans: Arc<[super::profile::CarrierPlan]>,
    pub(crate) profiles: Option<Arc<[super::profile::PoolProfile]>>,
    pub(crate) dynamic: bool,
}
pub(crate) struct ManagedCarrier {
    pub(crate) upstream: UpstreamTransport,
    pub(crate) grants: Option<CarrierGrants>,
}
#[derive(Default)]
struct State {
    epoch: Option<(
        zero_core::component::NetworkGeneration,
        Option<zero_platform_tokio::contract::dns::DnsGeneration>,
    )>,
    records: Vec<Arc<CarrierRecord>>,
    stopped: bool,
    ready: bool,
    users: usize,
    retired: bool,
}
mod epoch;
mod ownership;

impl ManagedState {
    pub(crate) fn carrier(
        &self,
        key: &super::PoolKey,
        options: &mieru_config::MieruTransportOptions,
    ) -> io::Result<&ManagedCarrier> {
        let index = if let Some(profiles) = &self.profiles {
            let profile = profiles
                .iter()
                .find(|profile| profile.matches(key, options))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Mieru request is absent from the complete pool profile set",
                    )
                })?;
            self.plans
                .iter()
                .position(|plan| plan == &profile.carrier)
                .ok_or_else(|| io::Error::other("Mieru profile lost its declared carrier"))?
        } else {
            0
        };
        self.carriers
            .get(index)
            .ok_or_else(|| io::Error::other("Mieru carrier provider is missing"))
    }
    pub(crate) fn user(self: &Arc<Self>) -> io::Result<UserLease> {
        let mut state = self.state.lock().unwrap();
        if !state.ready
            || state.stopped
            || self.cancel.is_cancelled()
            || self.scope_cancel.is_cancelled()
        {
            return Err(io::ErrorKind::Interrupted.into());
        }
        state.users = state
            .users
            .checked_add(1)
            .ok_or_else(|| io::Error::other("Mieru user count exhausted"))?;
        Ok(UserLease(self.clone()))
    }
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
    #[cfg(test)]
    pub(crate) fn reserve(self: &Arc<Self>) -> io::Result<CarrierLease> {
        self.reserve_with_pending_limit(0)
    }
    pub(crate) fn reserve_with_pending_limit(
        self: &Arc<Self>,
        pending_limit: usize,
    ) -> io::Result<CarrierLease> {
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
        let record = Arc::new(CarrierRecord::new(self.cancel.child_token(), pending_limit));
        state.records.push(record.clone());
        Ok(CarrierLease::new(self.clone(), record))
    }
    pub(crate) fn stop(&self) {
        self.state.lock().unwrap().stopped = true;
        self.cancel.cancel();
        self.changed.notify_waiters();
    }
    pub(crate) fn harvest_closed(&self) {
        let records = self.state.lock().unwrap().records.clone();
        let confirmed = records
            .into_iter()
            .filter(|record| record.closed(self.instance))
            .collect::<Vec<_>>();
        let removed = {
            let mut state = self.state.lock().unwrap();
            let mut removed = Vec::new();
            let mut index = 0;
            while index < state.records.len() {
                if confirmed
                    .iter()
                    .any(|record| Arc::ptr_eq(record, &state.records[index]))
                {
                    removed.push(state.records.remove(index));
                } else {
                    index += 1;
                }
            }
            removed
        };
        // Even positive closure does not authorize final protocol/loan drops
        // underneath the one actual owner census lock.
        drop(removed);
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
                    if let Some(children) = record.protocols() {
                        complete &= children.wait_returned(deadline).await?;
                        children.reclaim_returned();
                    }
                }
                for pending in record.pending() {
                    complete &= pending.retire(deadline).await?.is_complete();
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
            let (records, users) = {
                let state = self.state.lock().unwrap();
                (state.records.clone(), state.users)
            };
            let mut waiting = users != 0;
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
/// One actual pending open or returned native logical stream. Read/write split
/// halves retain that stream; dropping a lease proves no task or FD closure.
pub(crate) struct UserLease(Arc<ManagedState>);
impl UserLease {
    pub(crate) fn fork(&self) -> io::Result<Self> {
        self.0.user()
    }
}
impl Drop for UserLease {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.users = state
            .users
            .checked_sub(1)
            .expect("one actual Mieru user lease");
        drop(state);
        self.0.changed.notify_waiters();
    }
}
