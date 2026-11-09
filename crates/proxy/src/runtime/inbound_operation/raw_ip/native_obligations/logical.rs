//! Actual standard Datagram views/control, including borrowed donor returns.
use super::*;
impl NativeRegistration {
    pub(crate) fn observe_logical_datagram(
        &self,
        original: crate::component::datagram_retirement::DatagramRegistration,
    ) -> Result<(), ChannelError> {
        self.check_open()?;
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let caller = self
            .caller_witness()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        if !original.belongs_to(&caller) {
            return Err(rejected(Reason::StalePlan));
        }
        // Inspect each original outside the Source lock. Keep one positive
        // historical receipt plus every still-unsettled original; retries must
        // not accumulate an unbounded strong history or invert Entry locks.
        let mut index = 0;
        let mut kept_closed = false;
        loop {
            let candidate = {
                let state = owner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state
                    .records
                    .get(&self.id)
                    .and_then(|record| record.logical_datagrams.get(index))
                    .cloned()
            };
            let Some(candidate) = candidate else {
                break;
            };
            if candidate.closed() {
                if !kept_closed {
                    kept_closed = true;
                    index += 1;
                    continue;
                }
                let removed = {
                    let mut state = owner
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.records.get_mut(&self.id).and_then(|record| {
                        record
                            .logical_datagrams
                            .get(index)
                            .is_some_and(|actual| actual.same_entry(&candidate))
                            .then(|| record.logical_datagrams.remove(index))
                    })
                };
                if removed.is_none() {
                    index += 1;
                }
                drop(removed);
            } else {
                index += 1;
            }
        }
        let mut state = owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closing {
            return Err(rejected(Reason::Revoked));
        }
        let record = state
            .records
            .get_mut(&self.id)
            .ok_or_else(|| rejected(Reason::Revoked))?;
        if !record.entered || record.retiring || record.kind == ChildKind::Dns {
            return Err(rejected(Reason::Revoked));
        }
        if record
            .logical_datagrams
            .iter()
            .any(|actual| actual.same_entry(&original))
        {
            return Ok(());
        }
        record
            .logical_datagrams
            .try_reserve(1)
            .map_err(|source| rejected(Reason::Capacity).with_source(source))?;
        record.gateways = record
            .gateways
            .checked_add(1)
            .ok_or_else(|| rejected(Reason::Capacity))?;
        record.logical_datagrams.push(original);
        Ok(())
    }
}
