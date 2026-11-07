//! Limited publication through the same original Source native controller.
use super::*;
use zero_platform_tokio::{
    contract::{component::PacketDeviceIdentity, scope::TaskJoinWitness},
    controlled_carrier::{CarrierOrigin, LogicalPacketProviderRegistration},
};
struct Preparing<'a>(&'a AtomicBool);
impl Drop for Preparing<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl NativeDatagramGrant {
    pub(in crate::runtime::inbound_owners) fn register_software_driver(
        &self,
        driver: &TaskJoinWitness,
    ) -> Result<LogicalPacketProviderRegistration, ChannelError> {
        let denied = || ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component);
        let tasks = self.scope.as_ref().ok_or_else(denied)?;
        let native = self.original_origin().ok_or_else(denied)?;
        if self.cancel.is_cancelled()
            || self.packet_reads.is_none()
            || !native.driver().same_registration(driver)
            || driver.is_joined()
            || !tasks.scope_identity().same_scope(&driver.scope_identity())
        {
            return Err(denied());
        }
        let memory = self.packet_memory.get().ok_or_else(denied)?;
        let expected = self
            .packet_reads
            .as_ref()
            .ok_or_else(denied)?
            .packet_memory_view()?;
        if !memory.view.same_original(&expected) {
            return Err(denied());
        }
        if let Some(actual) = self.packet_logical.get() {
            return Ok(actual.clone());
        }
        if self.packet_calls.get().is_some() {
            return Err(denied());
        }
        self.packet_logical_preparing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| denied())?;
        let _preparing = Preparing(&self.packet_logical_preparing);
        if let Some(actual) = self.packet_logical.get() {
            return Ok(actual.clone());
        }
        // No owner mutex surrounds Root reservation/waker callbacks. The same
        // atomic phase forbids recursive/concurrent second logical originals.
        let actual = LogicalPacketProviderRegistration::from_registered_source(
            PacketDeviceIdentity::new(self.identity, tasks.scope_identity()),
            native,
            &tasks.downgrade(),
            self.cancel.child_token(),
        )?;
        actual.reserve_original_totals(
            &tasks.downgrade(),
            &self.originals.as_ref().ok_or_else(denied)?.carrier,
        )?;
        self.packet_logical
            .set(actual.clone())
            .map_err(|_| denied())?;
        Ok(actual)
    }
    pub(in crate::runtime::inbound_owners) fn packet_source_origin(&self) -> Option<CarrierOrigin> {
        if self.cancel.is_cancelled() {
            return None;
        }
        match self.packet_logical.get() {
            Some(actual) if !actual.is_retired() => {
                Some(CarrierOrigin::from_logical_packet(actual.clone()))
            }
            Some(_) => None,
            None => self.original_origin().map(CarrierOrigin::from_listener),
        }
    }
}
