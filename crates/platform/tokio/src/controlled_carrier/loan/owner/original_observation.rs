//! Read-only completion of this owner's concrete NativeControl, never an
//! inferred protocol completion from a related origin or arbitrary native root.
use super::*;
use crate::controlled_carrier::CarrierAdoption;
impl LoanOwner {
    /// Called only by the three existing constructors that install the exact
    /// concrete NativeControl. No outside protocol can attach this observation.
    pub(in crate::controlled_carrier::loan) fn observe_native_control(
        &mut self,
        actual: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let origin = actual.transferred_origin()?;
        if self.native_control.is_some() || !origin.same_original(self.original()) {
            return Err(crate::controlled_carrier::error(
                actual.instance(),
                crate::contract::Stage::Prepare,
                crate::contract::Reason::StalePlan,
                crate::contract::DeliveryState::NotSubmitted,
            ));
        }
        self.native_control_only = true;
        self.native_control = Some(actual);
        Ok(())
    }
    pub(super) fn observed_original_closed(&self) -> bool {
        if !self.state.returned.load(Ordering::Acquire) {
            return false;
        }
        let logical = self.observed_logical_closed();
        let native = self.native_control.as_ref().is_some_and(fully_closed);
        match &self.related {
            // A custom protocol is observed only if its own positive close was
            // already persisted before waiting on this exact related parent.
            Some(related) => {
                (native || logical || related.protocol_closed)
                    && fully_closed(&related.token.parent)
            }
            None => native || logical,
        }
    }
}
fn fully_closed(actual: &CarrierAdoption) -> bool {
    actual.closed().is_some_and(|same| same.matches(actual))
        && actual
            .users_closed()
            .is_some_and(|same| same.matches(actual))
        && actual.packet_driver_registrations_released()
}
