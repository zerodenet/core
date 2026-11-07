//! Actual software Packet driver lineage; deliberately no native-close authority.
use super::*;
use crate::contract::{component::PacketDeviceIdentity, CancellationToken};

mod custody;
struct ActualLogicalPacket {
    custody: custody::Custody,
    device: PacketDeviceIdentity,
    drivers: Arc<[TaskJoinWitness]>,
    cancel: CancellationToken,
    source_admission: Option<crate::controlled_datagram::provider::NativeDatagramOrigin>,
}
/// Observes the real registered software driver. Its owning factory retains
/// the actual stack and any separately retained physical ancestor. This token
/// cannot stand in for a descriptor/provider or close that physical ancestor.
#[derive(Clone)]
pub struct LogicalPacketProviderRegistration(Arc<ActualLogicalPacket>);
impl LogicalPacketProviderRegistration {
    pub fn from_registered(
        device: PacketDeviceIdentity,
        driver: TaskJoinWitness,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        Self::from_registered_drivers(device, vec![driver], cancel)
    }
    /// The complete original driver set of one finite component Scope. Every
    /// member is an actual registered task, so deduplicated membership cannot
    /// exceed that Scope's existing task budget. No aggregate task is created.
    pub fn from_registered_drivers(
        device: PacketDeviceIdentity,
        drivers: Vec<TaskJoinWitness>,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let mut seen = std::collections::BTreeSet::new();
        if drivers.is_empty()
            || drivers.iter().any(|driver| {
                !device.scope().same_scope(&driver.scope_identity())
                    || driver.task_id().is_none_or(|id| !seen.insert(id))
                    || driver.is_joined()
            })
            || !device.scope().resource_open()
            || cancel.is_cancelled()
        {
            return Err(error(
                device.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(Self(Arc::new(ActualLogicalPacket {
            custody: custody::Custody::default(),
            device,
            drivers: drivers.into(),
            cancel,
            source_admission: None,
        })))
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.device.instance()
    }
    pub fn device(&self) -> &PacketDeviceIdentity {
        &self.0.device
    }
    /// One lineage representative for existing origin observations. This is
    /// never a whole-driver-set join or native-close receipt.
    pub fn driver(&self) -> &TaskJoinWitness {
        &self.0.drivers[0]
    }
    pub fn drivers(&self) -> &[TaskJoinWitness] {
        &self.0.drivers
    }

    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn limit(&self) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::MIN
    }
    pub fn is_retired(&self) -> bool {
        self.0.cancel.is_cancelled()
            || !self.0.device.scope().resource_open()
            || self.0.drivers.iter().any(TaskJoinWitness::is_joined)
    }
    pub async fn retired(&self) {
        if self.is_retired() {
            return;
        }
        let mut waits = self
            .0
            .drivers
            .iter()
            .map(|driver| Box::pin(driver.wait()))
            .collect::<Vec<_>>();
        let any_exit = std::future::poll_fn(|cx| {
            use std::future::Future;
            for waiting in &mut waits {
                if waiting.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(());
                }
            }
            std::task::Poll::Pending
        });
        tokio::select! { _ = self.0.cancel.cancelled() => {}, _ = any_exit => {} }
    }
}

#[cfg(test)]
mod tests;

impl LogicalPacketProviderRegistration {
    /// Concrete actual Source assembly, requiring its original registered native
    /// controller and live real registrar. This is admission lineage only: the
    /// source UDP is never declared to be the opaque outer Packet ancestor.
    pub fn from_registered_source(
        device: PacketDeviceIdentity,
        native: crate::controlled_datagram::provider::NativeDatagramOrigin,
        tasks: &crate::contract::scope::WeakTaskRegistrar,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        if native.owner().instance() != device.instance()
            || native.is_retired()
            || !native.driver().scope_identity().same_scope(device.scope())
            || !tasks.scope_identity().same_scope(device.scope())
            || !tasks.registration_open()
        {
            return Err(error(
                device.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let driver = native.driver().clone();
        let mut actual = Self::from_registered_drivers(device, vec![driver], cancel)?;
        Arc::get_mut(&mut actual.0)
            .expect("fresh actual Source lineage")
            .source_admission = Some(native);
        Ok(actual)
    }
    pub fn source_admission(
        &self,
    ) -> Option<&crate::controlled_datagram::provider::NativeDatagramOrigin> {
        self.0.source_admission.as_ref()
    }
}
