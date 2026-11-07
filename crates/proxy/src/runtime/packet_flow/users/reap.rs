use super::*;
impl FlowUsers {
    pub(super) fn reap(&self) {
        // One actual row at a time. Predicate/Scope observations and removed
        // user-permit drops never happen under this ledger's owner mutex.
        let count = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .len();
        let mut index = 0;
        for _ in 0..count {
            let record = self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .records
                .get(index)
                .cloned();
            let Some(record) = record else { break };
            if !record.returned() {
                index += 1;
                continue;
            }
            let first = if record.packet_closed.is_none() && record.packet_driver.is_none() {
                record
                    .caller
                    .as_ref()
                    .and_then(TaskJoinWitness::try_joined)
                    .and_then(|joined| joined.result().err())
            } else {
                None
            };
            let removed = {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                if state.first.is_none() {
                    state.first = first;
                }
                state
                    .records
                    .iter()
                    .position(|same| Arc::ptr_eq(same, &record))
                    .map(|at| {
                        state.revision = state.revision.checked_add(1).unwrap_or_else(|| {
                            state.unaccounted = true;
                            state.revision
                        });
                        state.records.remove(at)
                    })
            };
            drop(removed);
        }
    }
}
impl Record {
    fn returned(&self) -> bool {
        self.released.load(std::sync::atomic::Ordering::Acquire)
            && match &self.packet_closed {
                Some(closed) => closed.load(std::sync::atomic::Ordering::Acquire),
                None => match &self.packet_driver {
                    Some(custody) => {
                        self.logical_closed
                            .as_ref()
                            .is_some_and(|closed| closed.load(std::sync::atomic::Ordering::Acquire))
                            && self.caller.as_ref().is_some_and(|caller| {
                                custody.matches_caller(caller)
                                    && (custody.retained() || custody.settled())
                            })
                    }
                    None => self.caller.as_ref().is_some_and(TaskJoinWitness::is_joined),
                },
            }
    }
}
