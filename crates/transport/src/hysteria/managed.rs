//! The existing Hysteria pool's host mode. One finite table covers cached,
//! establishing and retiring originals; a failed initializer never loses it.
use super::*;
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::{
        component::PacketDriverMetadataView, dns::DnsGeneration, network::UpstreamBinding,
        scope::TaskScopeIdentity, CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    controlled_carrier::{CarrierOrigin, CarrierProtocolRetirement},
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};
mod evidence;
mod slot;
pub(super) use evidence::OwnerRecord;
pub use slot::NativeSlot;
#[derive(Clone, PartialEq, Eq)]
struct Epoch(
    zero_core::component::NetworkGeneration,
    Option<DnsGeneration>,
);
pub struct EpochLease {
    _read: tokio::sync::OwnedRwLockReadGuard<Option<Epoch>>,
    owner: Arc<ManagedPool>,
}
pub struct ManagedPool {
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    limit: NonZeroUsize,
    memory: PacketDriverMetadataView,
    epoch: Arc<tokio::sync::RwLock<Option<Epoch>>>,
    slots: Mutex<Vec<Arc<NativeSlot>>>,
    stop: CancellationToken,
    evidence: Arc<evidence::Evidence>,
    _memory: PacketMetadataCredit,
}
impl Pool {
    pub fn exclude_native(&self) {
        let mut mode = self.0.ownership.lock().unwrap();
        if matches!(*mode, super::client::Ownership::Cold) {
            if self.0.family.enter_legacy().is_ok() {
                *mode = super::client::Ownership::Legacy;
            }
        }
    }
    pub fn native_eligible(&self) -> bool {
        self.0.family.native_eligible()
            && !matches!(
                *self.0.ownership.lock().unwrap(),
                super::client::Ownership::Legacy
            )
    }
    pub fn has_host_owner(&self) -> bool {
        matches!(&*self.0.ownership.lock().unwrap(), super::client::Ownership::Native(owner) if !owner.is_closed())
    }
    pub fn install_owner(
        &self,
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        limit: NonZeroUsize,
        memory: PacketDriverMetadataView,
    ) -> Result<Arc<ManagedPool>, ChannelError> {
        use super::client::Ownership;
        let mode = self.0.ownership.lock().unwrap();
        match &*mode {
            Ownership::Legacy => return Err(error(Reason::Unsupported)),
            Ownership::Native(owner)
                if owner.instance == instance && owner.scope.same_scope(&scope) =>
            {
                return owner.actual().ok_or_else(|| error(Reason::Revoked))
            }
            Ownership::Native(owner) if !owner.is_closed() => return Err(error(Reason::Revoked)),
            _ => {}
        }
        drop(mode);
        let evidence = evidence::Evidence::prepare(&memory)?;
        let layout = (|| {
            PacketMetadataLayout::array::<Arc<NativeSlot>>(
                limit
                    .get()
                    .checked_mul(2)
                    .ok_or(zero_traits::packet_metadata::PacketMemoryError::Capacity)?,
            )
        })()
        .and_then(|layout| {
            layout.checked_add(PacketMetadataLayout::arc::<
                tokio::sync::RwLock<Option<Epoch>>,
            >()?)
        })
        .map_err(|source| error(Reason::Capacity).with_source(source))?;
        let credit = memory
            .reserve(layout)
            .map_err(|source| error(Reason::Capacity).with_source(source))?;
        let owner = Arc::new(ManagedPool {
            instance,
            scope,
            limit,
            memory,
            epoch: Arc::new(tokio::sync::RwLock::new(None)),
            slots: Mutex::new(Vec::with_capacity(limit.get())),
            stop: CancellationToken::new(),
            evidence,
            _memory: credit,
        });
        let mut mode = self.0.ownership.lock().unwrap();
        match &*mode {
            Ownership::Legacy => return Err(error(Reason::Unsupported)),
            Ownership::Native(previous) if !previous.is_closed() => {
                return Err(error(Reason::Revoked))
            }
            _ => {}
        }
        let previous_family = self.0.family.install(&owner)?;
        let previous = std::mem::replace(&mut *mode, Ownership::Native(OwnerRecord::new(&owner)));
        drop(mode);
        drop(previous);
        drop(previous_family);
        Ok(owner)
    }
}
impl ManagedPool {
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn is_closed(&self) -> bool {
        self.evidence.closed.load(Ordering::Acquire)
    }
    pub fn stop(&self) {
        self.stop.cancel();
    }
    pub async fn enter(
        self: &Arc<Self>,
        binding: &UpstreamBinding,
        dns: Option<DnsGeneration>,
    ) -> Result<EpochLease, ChannelError> {
        let target = Epoch(binding.network_generation, dns);
        loop {
            if self.stop.is_cancelled() {
                return Err(error(Reason::Revoked));
            }
            let read = self.epoch.clone().read_owned().await;
            if read.as_ref() == Some(&target) {
                return Ok(EpochLease {
                    _read: read,
                    owner: self.clone(),
                });
            }
            drop(read);
            let mut write = self.epoch.write().await;
            if write.as_ref() != Some(&target) {
                // An interrupted observation leaves the old epoch selected.
                // Its actual Root cleanup continues; no new IO is admitted.
                self.drain_originals().await?;
                if self.stop.is_cancelled() {
                    return Err(error(Reason::Revoked));
                }
                *write = Some(target.clone());
            }
        }
    }
    pub fn slot(
        self: &Arc<Self>,
        binding: &UpstreamBinding,
        lease: &EpochLease,
    ) -> Result<Arc<NativeSlot>, ChannelError> {
        if !Arc::ptr_eq(&lease.owner, self)
            || self.stop.is_cancelled()
            || lease
                ._read
                .as_ref()
                .is_none_or(|epoch| epoch.0 != binding.network_generation)
        {
            return Err(error(Reason::Revoked));
        }
        let mut slots = self.slots.lock().unwrap();
        if let Some(slot) = slots.iter().find(|slot| slot.binding == *binding) {
            return Ok(slot.clone());
        }
        if slots.len() >= self.limit.get() {
            return Err(error(Reason::Capacity));
        }
        let slot = NativeSlot::new(binding.clone(), &self.memory)?;
        slots.push(slot.clone());
        Ok(slot)
    }
    async fn drain_originals(&self) -> Result<(), ChannelError> {
        let slots = self.slots.lock().unwrap().clone();
        // Nested prefixes return their logical users before their donors close.
        let mut slots = slots;
        slots.sort_by_key(|slot| std::cmp::Reverse(slot.binding.prefix.len()));
        for slot in &slots {
            slot.retire_original().await?;
        }
        // Only sealed original completion reaches this mutation.
        self.slots.lock().unwrap().clear();
        Ok(())
    }
    pub async fn shutdown_original(&self) -> Result<(), ChannelError> {
        self.stop();
        let _exclusive = self.epoch.write().await;
        self.drain_originals().await?;
        self.evidence.closed.store(true, Ordering::Release);
        Ok(())
    }
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
}
