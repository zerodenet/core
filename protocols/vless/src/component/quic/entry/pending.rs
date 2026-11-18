//! Foreign donor originals stay on the actual HTTP/H3 parent until its retained
//! child Root installs their LoanOwner, or the real originals close on failure.
use super::*;
use zero_platform_tokio::contract::lifecycle::{CloseReport, Unfinished};
use zero_platform_tokio::controlled_carrier::CarrierAdoption;

impl Owners {
    pub fn retain_pending_carrier(
        &self,
        memory: &PacketDriverMetadataView,
        limit: usize,
        original: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.release_installed_carriers();
        let notifications = memory.notifications();
        let mut pending = self.pending.lock().unwrap();
        if self.stop.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        if pending.iter().any(|actual| actual.same_slot(&original)) {
            return Ok(());
        }
        pending
            .try_reserve_one_with_metadata(memory, limit)
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let result = pending.push_reserved(original);
        drop(pending);
        drop(notifications);
        result.map_err(|original| {
            drop(original);
            error(Stage::Prepare, Reason::Capacity)
        })
    }
    fn next_pending(&self, index: usize) -> Option<CarrierAdoption> {
        self.pending.lock().unwrap().get(index).cloned()
    }
    fn remove_pending(&self, original: &CarrierAdoption) {
        let removed = {
            let mut pending = self.pending.lock().unwrap();
            let index = pending.iter().position(|actual| actual.same_slot(original));
            index.map(|index| pending.remove(index))
        };
        drop(removed);
    }
    pub fn release_installed_carriers(&self) {
        let mut index = 0;
        while let Some(original) = self.next_pending(index) {
            let closed = original
                .closed()
                .is_some_and(|closed| closed.matches(&original))
                && original
                    .users_closed()
                    .is_some_and(|closed| closed.matches(&original));
            let installed = self
                .protocols
                .get()
                .is_some_and(|children| children.retains_carrier_loan(&original));
            if closed || installed {
                self.remove_pending(&original);
            } else {
                index += 1;
            }
        }
    }
    pub(super) fn close_pending_carriers(&self) -> Result<CloseReport, ChannelError> {
        let mut report = CloseReport::default();
        let mut index = 0;
        while let Some(original) = self.next_pending(index) {
            report.append(original.try_retire()?);
            if original
                .closed()
                .is_some_and(|closed| closed.matches(&original))
                && original
                    .users_closed()
                    .is_some_and(|closed| closed.matches(&original))
            {
                self.remove_pending(&original);
            } else {
                index += 1;
            }
        }
        if self.next_pending(0).is_some() {
            report.unfinished.push(Unfinished::Resource(
                "actual prefix donor original remains retained".into(),
            ));
        }
        Ok(report)
    }
}
