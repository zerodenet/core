//! Decorators drain after their actual consumers, from inner to outer.
use super::*;
use crate::quic::owned::OwnedDriverOrigin;
impl OwnedEndpointOwner {
    pub fn start_prepared_masks(
        &mut self,
        plan: crate::finalmask::owned::PreparedMasks,
        icmp: Option<zero_platform_tokio::controlled_icmp::ControlledIcmpSocket>,
    ) -> Result<(), ChannelError> {
        if self.shared.phase.load(Ordering::Acquire) != 0 {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let drivers = match &mut self.native {
            Native::Hosted(native) => native.prepare_prepaid_masks(plan, icmp),
            Native::Packet(native) => plan.prepare(native.socket.clone(), icmp).map(|prepared| {
                native.socket = prepared.socket;
                prepared.drivers
            }),
            Native::Owned(_) => return Err(error(Stage::Prepare, Reason::Unsupported)),
        }
        .map_err(|source| error(Stage::Prepare, Reason::Io).with_source(source))?;
        self.masks.extend(
            drivers
                .iter()
                .map(|driver| (driver.ordinal, driver.work.cancel.clone())),
        );
        for driver in drivers {
            self.drivers.start_mask(driver)?;
        }
        Ok(())
    }
    /// The actual registered drivers in consumer-before-mask shutdown order.
    /// These identities are observations, never task-join or close receipts.
    pub fn shutdown_driver_tasks(&self) -> Vec<zero_core::component::TaskId> {
        let mut ids = self.drivers.tasks();
        ids.sort_by_key(|id| match self.drivers.origin(*id) {
            Some(OwnedDriverOrigin::Mask { ordinal, .. }) => (1, std::cmp::Reverse(ordinal)),
            _ => (0, std::cmp::Reverse(0)),
        });
        ids
    }
    /// Request this owner's mask stop after its real logical users return.
    /// The owning protocol must still receive, join and ACK the actual driver.
    pub async fn stop_mask_driver(
        &self,
        id: zero_core::component::TaskId,
        deadline: Instant,
    ) -> Result<(), ChannelError> {
        if let Some(OwnedDriverOrigin::Mask { ordinal, .. }) = self.drivers.origin(id) {
            self.shared.users.wait_empty(deadline).await?;
            if let Some((_, stop)) = self.masks.iter().find(|(index, _)| *index == ordinal) {
                stop.cancel();
            }
        }
        Ok(())
    }
    /// Stop this actual mask only after its original consumers return.
    pub async fn stop_mask_driver_original(
        &self,
        id: zero_core::component::TaskId,
    ) -> Result<(), ChannelError> {
        if let Some(OwnedDriverOrigin::Mask { ordinal, .. }) = self.drivers.origin(id) {
            self.shared.users.wait_empty_original().await?;
            if let Some((_, stop)) = self.masks.iter().find(|(index, _)| *index == ordinal) {
                stop.cancel();
            }
        }
        Ok(())
    }
}
