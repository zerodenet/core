//! Validate actual original ownership and transfer only sealed installed custody.
use super::*;
impl PendingOpen {
    pub(crate) fn cancellation(&self) -> zero_platform_tokio::contract::CancellationToken {
        self.stop.clone()
    }
    fn valid(&self, adoption: &CarrierAdoption) -> bool {
        self.plan.upgrade().is_some_and(|plan| {
            self.roles
                .iter()
                .any(|role| plan.validate_carrier_adoption(*role, adoption).is_ok())
        })
    }
    fn valid_owned(&self, adoption: &CarrierAdoption) -> bool {
        self.source.upgrade().is_some_and(|source| {
            self.roles.iter().any(|role| {
                source
                    .validate_owned_carrier_adoption(*role, adoption)
                    .is_ok()
            })
        })
    }
    pub(super) fn retain(&self, adoption: CarrierAdoption) -> Result<(), ChannelError> {
        if !self.valid(&adoption) {
            return Err(admission_error(Reason::StalePlan));
        }
        self.transfer_installed();
        let mut state = self.state.lock().unwrap();
        if state.retired || (!state.preparing && state.protocol.is_none()) {
            return Err(admission_error(Reason::Revoked));
        }
        if state
            .pending
            .iter()
            .any(|actual| actual.same_slot(&adoption))
        {
            return Ok(());
        }
        if state.pending.len() >= self.limit {
            return Err(admission_error(Reason::Capacity));
        }
        state
            .pending
            .try_reserve(1)
            .map_err(|source| admission_error(Reason::Capacity).with_source(source))?;
        state.pending.push(adoption);
        Ok(())
    }
    fn transfer_installed(&self) {
        let pending = self.state.lock().unwrap().pending.clone();
        let loans = pending
            .iter()
            .filter_map(|receiver| {
                let proof = receiver.retained_loan()?;
                proof
                    .receiver_adoption()
                    .is_some_and(|actual| actual.same_slot(receiver))
                    .then_some((receiver.clone(), proof))
            })
            .collect::<Vec<_>>();
        // Same actual receiver/driver rules as the existing DNS caller ledger.
        // Do this BEFORE cancellation, or the first waiter could stop a pool
        // already used by another caller while its tunnel ACK is still pending.
        let transferred = pending
            .iter()
            .filter(|original| {
                original.retained_driver().is_some_and(|proof| {
                    proof.retains(original)
                        && proof
                            .adoption()
                            .is_some_and(|actual| actual.same_slot(original))
                        && proof
                            .owner()
                            .is_some_and(|owner| owner.matches_adoption(original))
                        && self.valid_owned(original)
                }) || original.retained_preparation_job().is_some_and(|proof| {
                    proof.retains(original)
                        && proof
                            .owner()
                            .is_some_and(|owner| owner.matches_adoption(original))
                        && self.valid_owned(original)
                }) || loans.iter().any(|(receiver, proof)| {
                    !receiver.same_slot(original) && proof.retains(original)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let removed = {
            let mut state = self.state.lock().unwrap();
            let mut removed = Vec::new();
            for index in (0..state.pending.len()).rev() {
                if transferred
                    .iter()
                    .any(|actual| actual.same_slot(&state.pending[index]))
                {
                    removed.push(state.pending.remove(index));
                }
            }
            removed
        };
        drop(removed);
    }
    pub(super) fn cancel(&self, reason: CloseReason) {
        self.state.lock().unwrap().retired = true;
        self.transfer_installed();
        self.stop.cancel();
        if let Some(children) = &self.protocols {
            children.request_return();
        }
        let (pending, protocol) = {
            let state = self.state.lock().unwrap();
            (state.pending.clone(), state.protocol.clone())
        };
        for original in pending {
            original.request_retirement();
        }
        if let Some(protocol) = protocol {
            if let Ok(actual) = protocol.try_lock() {
                actual.cancel(reason);
            }
        }
    }
}

impl PendingOpenRef {
    pub(crate) fn observer(
        &self,
        binding: UpstreamBinding,
    ) -> Result<Option<PendingCarrierObserver>, ChannelError> {
        if self.limit == 0 {
            return Ok(None);
        }
        let existing = {
            let state = self.state.lock().unwrap();
            state
                .observers
                .iter()
                .find(|actual| actual.binding() == &binding)
                .cloned()
        };
        if let Some(actual) = existing {
            return Ok(Some(actual.pending_carrier_observer()));
        }
        let observer = Observer {
            pending: self.downgrade(),
            anchor: std::sync::OnceLock::new(),
        };
        let prepared = if let Some(memory) = &self.memory {
            let credit = memory
                .reserve(
                    UpstreamTransport::opener_metadata_layout::<Observer>(&binding)
                        .map_err(|source| admission_error(Reason::Capacity).with_source(source))?,
                )
                .map_err(|source| admission_error(Reason::Capacity).with_source(source))?;
            let (transport, actual) = UpstreamTransport::from_prepaid_opener(
                binding, observer, credit,
            )
            .map_err(|(source, _, _, _)| admission_error(Reason::Capacity).with_source(source))?;
            assert!(
                actual
                    .actual()
                    .anchor
                    .set(transport.pending_carrier_observer())
                    .is_ok(),
                "one association observer"
            );
            drop(actual);
            transport
        } else {
            let observer = Arc::new(observer);
            let transport = UpstreamTransport::new(binding, observer.clone());
            assert!(
                observer
                    .anchor
                    .set(transport.pending_carrier_observer())
                    .is_ok(),
                "one standalone observer"
            );
            transport
        };
        let (chosen, result) = {
            let mut state = self.state.lock().unwrap();
            if state.retired {
                return Err(admission_error(Reason::Revoked));
            }
            if let Some(actual) = state
                .observers
                .iter()
                .find(|actual| actual.binding() == prepared.binding())
            {
                (actual.clone(), Ok(()))
            } else if state.observers.len() >= self.roles.len().saturating_add(1) {
                (prepared.clone(), Err(admission_error(Reason::Capacity)))
            } else {
                state.observers.push(prepared.clone());
                (prepared.clone(), Ok(()))
            }
        };
        drop(prepared);
        result?;
        Ok(Some(chosen.pending_carrier_observer()))
    }
}
