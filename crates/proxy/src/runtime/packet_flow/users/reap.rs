use super::*;
impl FlowUsers {
    pub(super) fn reap(&self) {
        loop {
            // Restart after one actual removal. A stable revision is required
            // before publishing the first outstanding failure: concurrent
            // reapers must not skip a secondary row or clear a new cause.
            let (count, revision) = {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                (state.records.len(), state.revision)
            };
            let mut first = None;
            let mut retire = None;
            for index in 0..count {
                let record = {
                    let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    if state.revision != revision {
                        break;
                    }
                    state.records.get(index).cloned()
                };
                let Some(record) = record else { break };
                if !record.returned() {
                    continue;
                }
                if record.packet_closed.is_none() && record.packet_driver.is_none() {
                    if let Some(joined) = record
                        .caller
                        .as_ref()
                        .and_then(TaskJoinWitness::try_joined)
                        .filter(|joined| joined.result().is_err())
                    {
                        // Retain the SAME original joined outcome, not another
                        // failure receiver or a synthetic callee task receipt.
                        let actual = record.failed_caller.get_or_init(|| joined.clone());
                        let same_original = record
                            .caller
                            .as_ref()
                            .is_some_and(|caller| caller.matches_joined(actual))
                            && actual
                                .result()
                                .err()
                                .zip(joined.result().err())
                                .is_some_and(|(retained, observed)| {
                                    Arc::ptr_eq(&retained, &observed)
                                });
                        if !same_original
                            || !actual.failure_accepted()
                            || !actual.registration_released()
                        {
                            first.get_or_insert_with(|| actual.result().unwrap_err());
                            continue;
                        }
                    }
                }
                retire = Some(record);
                break;
            }
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.revision != revision {
                drop(state);
                continue;
            }
            if let Some(record) = retire {
                let position = state
                    .records
                    .iter()
                    .position(|same| Arc::ptr_eq(same, &record))
                    .expect("same observed user revision");
                state.revision = state.revision.checked_add(1).unwrap_or_else(|| {
                    state.unaccounted = true;
                    state.revision
                });
                let removed = state.records.remove(position);
                drop(state);
                // User permits, error payloads and their external wake/drop
                // paths run only after the owning users mutex was released.
                drop(removed);
                drop(record);
                continue;
            }
            let previous = std::mem::replace(&mut state.first, first);
            drop(state);
            drop(previous);
            return;
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
