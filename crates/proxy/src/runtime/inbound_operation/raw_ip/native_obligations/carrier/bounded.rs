//! Observe only the real original physical owner's declared finite capacity.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration;

impl NativeRegistration {
    /// The private host issuer supplies this weak registration from the same
    /// actual business ticket. Neither public numeric IDs nor caller caps grant
    /// permission. This method observes no new I/O and holds no native budget.
    pub(crate) fn observe_carrier_with_owner(
        &self,
        physical: &CarrierOwnerRegistration,
        adoption: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.check_open()?;
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| pre_io_rejection(Reason::Revoked))?;
        // Only irrevocably closed NoAcquisition reservations may be discarded
        // before the caller joins. Query original slot getters outside our lock.
        let mut unused = {
            let mut state = owner.lock().unwrap_or_else(|error| error.into_inner());
            let record = state
                .records
                .get_mut(&self.id)
                .ok_or_else(|| pre_io_rejection(Reason::Revoked))?;
            let used = record
                .additional_carriers
                .len()
                .checked_add(usize::from(record.carrier.is_some()))
                .ok_or_else(|| pre_io_rejection(Reason::Capacity))?;
            let mut observed = Vec::new();
            if let Err(source) = observed.try_reserve(used) {
                record.carrier_conflict = true;
                return Err(pre_io_rejection(Reason::Capacity).with_source(source));
            }
            observed.extend(
                record
                    .carrier
                    .iter()
                    .chain(&record.additional_carriers)
                    .cloned(),
            );
            observed
        };
        unused.retain(|slot| {
            slot.closed()
                .is_some_and(|closed| closed.matches(slot) && !closed.had_native_acquisition())
        });
        let mut state = owner.lock().unwrap_or_else(|error| error.into_inner());
        let record = state
            .records
            .get_mut(&self.id)
            .ok_or_else(|| pre_io_rejection(Reason::Revoked))?;
        if record.kind != ChildKind::Tcp || !record.entered || record.carrier_conflict {
            return Err(pre_io_rejection(Reason::InvalidConfig));
        }
        if !physical.matches_adoption(&adoption)
            || record
                .carrier_owner
                .as_ref()
                .is_some_and(|actual| !actual.same_owner(physical))
            || record
                .carrier
                .iter()
                .chain(&record.additional_carriers)
                .any(|slot| !physical.matches_adoption(slot))
        {
            record.carrier_conflict = true;
            return Err(
                pre_io_rejection(Reason::StalePlan).with_source(std::io::Error::other(
                    "raw caller carrier belongs to another original physical owner",
                )),
            );
        }
        record.carrier_owner.get_or_insert_with(|| physical.clone());
        if record
            .carrier
            .iter()
            .chain(&record.additional_carriers)
            .any(|actual| actual.same_slot(&adoption))
        {
            return Ok(());
        }
        record
            .additional_carriers
            .retain(|actual| !unused.iter().any(|closed| closed.same_slot(actual)));
        if record
            .carrier
            .as_ref()
            .is_some_and(|actual| unused.iter().any(|closed| closed.same_slot(actual)))
        {
            record.carrier = if record.additional_carriers.is_empty() {
                None
            } else {
                Some(record.additional_carriers.remove(0))
            };
        }
        let used =
            usize::from(record.carrier.is_some()).checked_add(record.additional_carriers.len());
        if used.is_none_or(|used| used >= physical.limit().get()) {
            record.carrier_conflict = true;
            return Err(pre_io_rejection(Reason::Capacity));
        }
        if record.carrier.is_none() {
            record.carrier = Some(adoption);
        } else {
            if let Err(source) = record.additional_carriers.try_reserve(1) {
                record.carrier_conflict = true;
                return Err(pre_io_rejection(Reason::Capacity).with_source(source));
            }
            record.additional_carriers.push(adoption);
        }
        Ok(())
    }
}

fn pre_io_rejection(reason: Reason) -> ChannelError {
    let mut error = rejected(reason);
    error.stage = Stage::Prepare;
    error.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
    error
}
