//! The original native phase without waiting for an external caller's own join.
use super::{owner, CarrierAdoption, CarrierClosed};
use crate::contract::{lifecycle::CloseReport, ChannelError};
use tokio::time::Instant;

/// Native-stage result for this exact slot, not whole-user or generation
/// completion. A complete report is accompanied by the original native receipt;
/// unfinished reports retain their actual task/I/O diagnostics. The original
/// successful close report stays in the owner for full settlement exactly once.
pub struct CarrierNativeSettlement {
    report: CloseReport,
    closed: Option<CarrierClosed>,
}
impl CarrierNativeSettlement {
    /// Consume diagnostics only; this returns no NativeClosed or UsersClosed permission.
    pub fn into_report(self) -> CloseReport {
        self.report
    }
    pub fn report(&self) -> &CloseReport {
        &self.report
    }
    pub fn closed(&self) -> Option<&CarrierClosed> {
        self.closed.as_ref()
    }
}
impl CarrierAdoption {
    /// Try only already returned native/driver work, without inventing a timer.
    /// A borrowed logical control or any unfinished work remains owned; this
    /// does not remove the slot, refund capacity or issue a UsersClosed receipt.
    pub fn try_retire_native(&self) -> Result<CarrierNativeSettlement, ChannelError> {
        self.request_retirement();
        let report = owner::try_close_native_slot(&self.slot)?;
        let closed = report.is_complete().then(|| self.closed()).flatten();
        Ok(CarrierNativeSettlement { report, closed })
    }
    /// Retire only this original native carrier. Physical I/O must have returned;
    /// actual transport/auxiliary tasks must be joined and their local failures
    /// accepted before the original control can confirm native closure.
    ///
    /// This never waits for an external caller's own join, consumes pending full
    /// reports, removes the slot, refunds capacity, or issues UsersClosed. The
    /// actual logical lease/caller obligation remains for ordinary retire or
    /// owner shutdown. Deadline/cancellation/error retains the original state.
    pub async fn retire_native(
        &self,
        deadline: Instant,
    ) -> Result<CarrierNativeSettlement, ChannelError> {
        self.slot.cancel.cancel();
        self.slot.notify();
        let report = owner::close_native_slot(&self.slot, deadline).await?;
        let closed = if report.is_complete() {
            self.closed()
        } else {
            None
        };
        Ok(CarrierNativeSettlement { report, closed })
    }
}
