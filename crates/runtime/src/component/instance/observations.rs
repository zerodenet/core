//! The same actual component slot, separated without changing ownership.
use super::*;

impl ComponentSlot {
    /// Seal only a never installed cold candidate. Revocation is an admission
    /// fence, not native/task close evidence; the ordinary replacement still
    /// confirms this slot. A concurrent initializer makes refresh fail closed.
    pub(in crate::component) fn seal_unbuilt_refresh(&self) -> Result<(), ChannelError> {
        self.revoke();
        let _owned = self.owned_start_gate.try_lock().map_err(|_| {
            invalid(
                Reason::Capacity,
                "cold initializer remains owned during refresh",
            )
        })?;
        let _starting = self.start_gate.try_lock().map_err(|_| {
            invalid(
                Reason::Capacity,
                "cold component start remains active during refresh",
            )
        })?;
        if self.live.get().is_some()
            || self.owned_start.get().is_some()
            || self.owned_start_host.get().is_some()
        {
            return Err(invalid(
                Reason::StalePlan,
                "network refresh cannot replace a built candidate",
            ));
        }
        #[cfg(any(target_os = "linux", windows))]
        if self.prepaid_process.get().is_some() {
            return Err(invalid(
                Reason::StalePlan,
                "cold refresh retains an actual process reservation",
            ));
        }
        Ok(())
    }
    /// Observe the installed original for one declared ordinal. This weak
    /// identity grants no acquisition, cleanup or task authority; absence is
    /// unavailable evidence, never a successful close. Its opaque receipts
    /// remain independently matchable after the host retires this slot.
    pub fn original_carrier_owner(
        &self,
        ordinal: usize,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration> {
        self.live
            .get()?
            .as_ref()
            .ok()?
            .resources
            .original_carrier_owner(ordinal)
    }
    pub fn original_packet_owner(
        &self,
        ordinal: usize,
    ) -> Option<zero_platform_tokio::controlled_carrier::PacketProviderRegistration> {
        self.live
            .get()?
            .as_ref()
            .ok()?
            .resources
            .original_packet_owner(ordinal)
    }
    pub fn original_metadata_owner(
        &self,
        ordinal: usize,
    ) -> Option<zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolRegistration>
    {
        self.live
            .get()?
            .as_ref()
            .ok()?
            .resources
            .original_metadata_owner(ordinal)
    }
}
