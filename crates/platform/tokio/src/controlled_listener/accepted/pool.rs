use super::*;
impl Pool {
    pub(super) fn new(limit: NonZeroUsize, cancel: CancellationToken) -> Self {
        Self {
            identity: Arc::new(()),
            limit,
            cancel,
            relay: Arc::new(relay::AcceptRelay::new()),
            state: Mutex::new(PoolState {
                next: 0,
                entries: BTreeMap::new(),
                closed: 0,
                reported: 0,
                transferred: false,
                first_failure: None,
                first_close_failure: None,
            }),
        }
    }
    pub(super) fn state(&self) -> std::sync::MutexGuard<'_, PoolState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) fn reserve(self: &Arc<Self>) -> Result<Reservation, ChannelError> {
        let mut state = self.state();
        if self.cancel.is_cancelled() {
            return Err(failure(
                Stage::Prepare,
                Reason::Revoked,
                "accepted TCP admission revoked",
            ));
        }
        for entry in state.entries.values() {
            let status = entry.state();
            if matches!(status.phase, Phase::Unknown | Phase::Acquiring) {
                return Err(retained(
                    status
                        .unknown
                        .clone()
                        .unwrap_or_else(|| entry.uncertain.clone()),
                ));
            }
        }
        if state.entries.len() == self.limit.get() {
            return Err(failure(
                Stage::Prepare,
                Reason::Capacity,
                "accepted TCP originals exhausted",
            ));
        }
        let id = state.next.checked_add(1).ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Capacity,
                "accepted TCP identity exhausted",
            )
        })?;
        state.next = id;
        let mut uncertain = failure(
            Stage::Prepare,
            Reason::Io,
            "accepted TCP acquisition did not return an observed native outcome",
        );
        uncertain.delivery = DeliveryState::Unknown;
        let entry = Arc::new(Entry {
            id,
            identity: self.identity.clone(),
            state: Mutex::new(EntryState {
                phase: Phase::Waiting,
                failure: None,
                unknown: None,
            }),
            control: AsyncMutex::new(None),
            uncertain: Arc::new(uncertain),
        });
        state.entries.insert(id, entry.clone());
        Ok(Reservation {
            pool: self.clone(),
            entry,
        })
    }
    pub(super) fn remember(
        &self,
        entry: &Entry,
        source: ChannelError,
        unknown: bool,
    ) -> ChannelError {
        let source = Arc::new(source);
        {
            let mut state = entry.state();
            state.failure.get_or_insert_with(|| source.clone());
            if unknown {
                state.phase = Phase::Unknown;
                state.unknown.get_or_insert_with(|| source.clone());
            }
        }
        {
            let mut state = self.state();
            state.first_failure.get_or_insert_with(|| source.clone());
            if source.stage == Stage::Close {
                state
                    .first_close_failure
                    .get_or_insert_with(|| source.clone());
            }
        }
        retained(source)
    }
    pub(super) fn evidence(&self) -> AcceptedTcpEvidence {
        let state = self.state();
        let unknown = state
            .entries
            .values()
            .filter_map(|entry| {
                let state = entry.state();
                matches!(state.phase, Phase::Unknown | Phase::Acquiring).then(|| {
                    (
                        entry.id,
                        state
                            .unknown
                            .clone()
                            .unwrap_or_else(|| entry.uncertain.clone()),
                    )
                })
            })
            .collect();
        AcceptedTcpEvidence {
            admission_closed: self.cancel.is_cancelled(),
            retained: state.entries.len(),
            unknown,
            confirmed_native_closes: state.closed,
            users: if state.transferred {
                AcceptedTcpUsers::Unaccounted
            } else {
                AcceptedTcpUsers::NoTransfer
            },
            first_failure: state.first_failure.clone(),
            first_close_failure: state.first_close_failure.clone(),
        }
    }
    pub(super) fn take_report(&self) -> CloseReport {
        let mut state = self.state();
        let count = state.closed - state.reported;
        state.reported = state.closed;
        let mut report = CloseReport::default();
        if count != 0 {
            report.closed_io.push(format!(
                "{count} accepted TCP originals explicitly closed (native only)"
            ));
        }
        report
    }
    pub(super) fn confirm(&self, entry: &Arc<Entry>) {
        let mut state = self.state();
        if state
            .entries
            .get(&entry.id)
            .is_some_and(|actual| Arc::ptr_eq(actual, entry))
        {
            state.entries.remove(&entry.id);
            state.closed += 1;
        }
    }
}
pub(super) struct Reservation {
    pub(super) pool: Arc<Pool>,
    pub(super) entry: Arc<Entry>,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let unused = self.entry.state().phase == Phase::Waiting;
        if unused {
            let mut state = self.pool.state();
            if state
                .entries
                .get(&self.entry.id)
                .is_some_and(|actual| Arc::ptr_eq(actual, &self.entry))
            {
                state.entries.remove(&self.entry.id);
            }
        }
        // Acquiring includes native registration unwind. Drop never releases
        // that reservation, a live original, or an Unknown; it is not a receipt.
    }
}
