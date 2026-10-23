//! Whole gateway settlement uses declared coverage and original close witnesses.
use super::*;
use crate::runtime::inbound_owners::PeerBoundDatagramRegistration;

impl NativeRegistration {
    /// Called at every selected operation, before execution can acquire native
    /// I/O. One uncovered fallback remains sticky even if a later one succeeds.
    pub(crate) fn declare_gateway(&self, covered: bool) -> Result<(), ChannelError> {
        self.check_open()?;
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let mut state = owner.lock().unwrap_or_else(|error| error.into_inner());
        let record = state
            .records
            .get_mut(&self.id)
            .ok_or_else(|| rejected(Reason::Revoked))?;
        record.gateways = record
            .gateways
            .checked_add(1)
            .ok_or_else(|| rejected(Reason::Capacity))?;
        record.uncovered_gateway |= !covered;
        Ok(())
    }

    pub(crate) fn observe_datagram(
        &self,
        provider: PeerBoundDatagramRegistration,
    ) -> Result<(), ChannelError> {
        self.check_open()?;
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let mut state = owner.lock().unwrap_or_else(|error| error.into_inner());
        let record = state
            .records
            .get_mut(&self.id)
            .ok_or_else(|| rejected(Reason::Revoked))?;
        if record.kind != ChildKind::Udp
            || !record
                .caller
                .as_ref()
                .is_some_and(|caller| provider.belongs_to(caller))
            || record
                .datagram
                .as_ref()
                .is_some_and(|actual| !actual.same_owner(&provider))
        {
            record.uncovered_gateway = true;
            return Err(rejected(Reason::InvalidConfig));
        }
        record.datagram = Some(provider);
        Ok(())
    }

    async fn settle(&self, deadline: Instant) -> Result<bool, ChannelError> {
        let Some(owner) = self.owner.upgrade() else {
            return Ok(false);
        };
        let (kind, caller, adoptions, datagram) = {
            let state = owner.lock().unwrap_or_else(|error| error.into_inner());
            let Some(record) = state.records.get(&self.id) else {
                return Ok(true);
            };
            if !record.retiring
                || record.gateways == 0
                || record.uncovered_gateway
                || record.carrier_conflict
            {
                return Ok(false);
            }
            (
                record.kind,
                record.caller.clone(),
                record
                    .carrier
                    .iter()
                    .chain(&record.additional_carriers)
                    .cloned()
                    .collect::<Vec<_>>(),
                record.datagram.clone(),
            )
        };
        let Some(caller) = caller else {
            return Ok(false);
        };
        let Some(joined) = caller.observed_join() else {
            return Ok(false);
        };
        if let Some(failure) = joined.failure() {
            tracing::debug!(task_id = joined.task().0, error = %failure, "raw-IP caller failure retained during physical cleanup");
        }
        let Some(witness) = caller.join_witness() else {
            return Ok(false);
        };
        if witness
            .try_joined()
            .is_none_or(|actual| actual.id() != joined.task() || actual.class() != joined.class())
        {
            return Ok(false);
        }
        match kind {
            ChildKind::Tcp => {
                for adoption in adoptions {
                    // This drives the same native original after its actual
                    // caller joined. A returned view or Drop is never enough.
                    let report = adoption.retire(deadline).await?;
                    let Some(closed) = adoption.closed() else {
                        return Ok(false);
                    };
                    if !report.is_complete() || !closed.matches(&adoption) {
                        return Ok(false);
                    }
                    if closed.had_native_acquisition()
                        && adoption.users_closed().is_none_or(|users| {
                            !users.matches(&adoption)
                                || !users.matches_caller(&witness)
                                || users.caller_exit().is_none_or(|exit| {
                                    exit.id() != joined.task() || exit.class() != joined.class()
                                })
                        })
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            ChildKind::Udp => Ok(datagram.is_some_and(|provider| {
                provider.closed().is_some_and(|closed| {
                    let original = provider.identity();
                    let actual = closed.identity();
                    closed.matches(&provider)
                        && actual.instance == original.instance
                        && actual.association == original.association
                        && actual.task == original.task
                        && actual.task == joined.task()
                        && closed.joined().task() == joined.task()
                        && closed.joined().class() == joined.class()
                })
            })),
            ChildKind::Dns => Ok(false),
        }
    }
}

impl NativeObligations {
    pub(super) fn has_covered_gateway(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .values()
            .any(|record| {
                record.gateways > 0
                    && !record.uncovered_gateway
                    && !record.carrier_conflict
                    && record.kind != ChildKind::Dns
            })
    }
    /// A finite quota measures outstanding originals, not lifetime traffic.
    /// Reclaim only after the original caller and every covered native owner
    /// have really completed; an unknown sibling remains charged.
    pub(crate) async fn reserve_reclaimed(
        &self,
        kind: ChildKind,
        deadline: Instant,
    ) -> Result<NativePermit, ChannelError> {
        match self.reserve(kind) {
            Err(error) if error.reason == Reason::Capacity => {
                self.settle_joined(deadline).await?;
                self.reserve(kind)
            }
            result => result,
        }
    }
    pub(super) async fn settle_joined(&self, deadline: Instant) -> Result<(), ChannelError> {
        let registrations = self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .keys()
            .map(|id| NativeRegistration {
                owner: Arc::downgrade(&self.0),
                id: *id,
            })
            .collect::<Vec<_>>();
        let settled = futures_util::future::join_all(
            registrations
                .iter()
                .map(|registration| registration.settle(deadline)),
        )
        .await;
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let mut failure = None;
        for (registration, result) in registrations.iter().zip(settled) {
            match result {
                Ok(true) => {
                    state.records.remove(&registration.id);
                }
                Ok(false) => {}
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
