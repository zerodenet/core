use super::*;
impl Pool {
    pub(super) fn new(limit: NonZeroUsize, cancel: CancellationToken) -> Self {
        Self {
            identity: Arc::new(()),
            limit,
            cancel,
            relay: Arc::new(relay::AcceptRelay::new()),
            graph: None,
            state: Mutex::new(PoolState {
                next: 0,
                entries: BTreeMap::new(),
                closed: 0,
                reported: 0,
                transferred: false,
                first_failure: None,
                first_close_failure: None,
                first_user_failure: None,
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
        let cancel = self.cancel.child_token();
        let entry = Arc::new_cyclic(|entry| Entry {
            id,
            identity: self.identity.clone(),
            state: Mutex::new(EntryState {
                phase: Phase::Waiting,
                native_counted: false,
                failure: None,
                unknown: None,
            }),
            control: OnceLock::new(),
            uncertain: Arc::new(uncertain),
            users: self.graph.as_ref().map(|authority| {
                Arc::new(graph::Users::new(
                    authority.clone(),
                    entry.clone(),
                    Arc::downgrade(self),
                    cancel.clone(),
                ))
            }),
            cancel,
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
        let (entries, transferred, closed, first_failure, first_close_failure, first_user_failure) = {
            let state = self.state();
            (
                state.entries.values().cloned().collect::<Vec<_>>(),
                state.transferred,
                state.closed,
                state.first_failure.clone(),
                state.first_close_failure.clone(),
                state.first_user_failure.clone(),
            )
        };
        let unknown = entries
            .iter()
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
        let users = if !transferred {
            AcceptedTcpUsers::NoTransfer
        } else if self.graph.is_none() {
            AcceptedTcpUsers::Unaccounted
        } else {
            let mut status = AcceptedTcpUsers::Confirmed;
            for entry in &entries {
                match entry.users.as_ref().map(|users| users.status()) {
                    None | Some(AcceptedTcpUsers::Unaccounted) => {
                        status = AcceptedTcpUsers::Unaccounted;
                        break;
                    }
                    Some(AcceptedTcpUsers::Pending) => status = AcceptedTcpUsers::Pending,
                    _ => {}
                }
            }
            status
        };
        AcceptedTcpEvidence {
            admission_closed: self.cancel.is_cancelled(),
            retained: entries.len(),
            unknown,
            confirmed_native_closes: closed,
            users,
            first_failure,
            first_close_failure,
            first_user_failure,
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
        let reclaim = entry.users.as_ref().is_none_or(|users| users.complete());
        let mut state = self.state();
        if state
            .entries
            .get(&entry.id)
            .is_some_and(|actual| Arc::ptr_eq(actual, entry))
        {
            let mut status = entry.state();
            if !status.native_counted {
                status.native_counted = true;
                state.closed += 1;
            }
            drop(status);
            if reclaim {
                state.entries.remove(&entry.id);
            }
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
