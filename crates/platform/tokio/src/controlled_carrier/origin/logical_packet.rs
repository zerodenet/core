//! Actual software Packet driver lineage; deliberately no native-close authority.
use super::*;
use crate::contract::{CancellationToken, component::PacketDeviceIdentity};

mod custody;
struct ActualLogicalPacket {
    custody: custody::Custody,
    device: PacketDeviceIdentity,
    driver: TaskJoinWitness,
    cancel: CancellationToken,
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
        if !device.scope().same_scope(&driver.scope_identity())
            || !device.scope().resource_open()
            || driver.task_id().is_none()
            || driver.is_joined()
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
            driver,
            cancel,
        })))
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.device.instance()
    }
    pub fn device(&self) -> &PacketDeviceIdentity {
        &self.0.device
    }
    pub fn driver(&self) -> &TaskJoinWitness {
        &self.0.driver
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
            || self.0.driver.is_joined()
    }
    pub async fn retired(&self) {
        if self.is_retired() {
            return;
        }
        tokio::select! { _ = self.0.cancel.cancelled() => {}, _ = self.0.driver.wait() => {} }
    }
}

#[cfg(test)]
mod tests;
