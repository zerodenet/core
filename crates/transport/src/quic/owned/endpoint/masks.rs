//! Decorators drain after their actual consumers, from inner to outer.
use super::*;
use crate::quic::owned::OwnedDriverOrigin;
impl OwnedEndpointOwner {
    /// The real owner/claim is already installed before native mask acquisition
    /// or the first actual driver registration. Failed preparation stays owned.
    pub(crate) fn start_masks(
        &mut self,
        masks: &[crate::finalmask::udp::Mask],
        resources: &dyn zero_platform_tokio::contract::component::ResourceAccess,
    ) -> Result<(), ChannelError> {
        let Native::Hosted(native) = &mut self.native else {
            return Err(error(Stage::Prepare, Reason::Unsupported));
        };
        let icmp = crate::finalmask::owned::icmp_bind(masks)
            .map_err(|source| error(Stage::Prepare, Reason::InvalidConfig).with_source(source))?
            .map(|bind| resources.bind_inbound_icmp(bind))
            .transpose()?;
        let drivers = native
            .prepare_masks(masks, icmp)
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
    pub(crate) fn shutdown_driver_tasks(&self) -> Vec<zero_core::component::TaskId> {
        let mut ids = self.drivers.tasks();
        ids.sort_by_key(|id| match self.drivers.origin(*id) {
            Some(OwnedDriverOrigin::Mask { ordinal, .. }) => (1, std::cmp::Reverse(ordinal)),
            _ => (0, std::cmp::Reverse(0)),
        });
        ids
    }
    pub(crate) async fn stop_mask_driver(
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
}
