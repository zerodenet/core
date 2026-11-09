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
        if !self.receiver_released()? {
            return Ok(false);
        }
        // This actual caller has its own protocol-original return gate. It is
        // not native coverage and cannot excuse an uncovered gateway below.
        if !self.settle_protocols(deadline).await? {
            return Ok(false);
        }
        if let Some(settled) = self.settle_dns(deadline).await? {
            return Ok(settled);
        }
        if let Some(settled) = self.settle_pre_admission()? {
            return Ok(settled);
        }
        // Pending preparation cleanup is local to this actual caller. It does
        // not certify any separately uncovered raw-IP gateway.
        if !self.settle_pending(deadline).await? {
            return Ok(false);
        }
        let Some(owner) = self.owner.upgrade() else {
            return Ok(false);
        };
        let (kind, caller, adoptions, datagram, logical) = {
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
                record.logical_datagrams.clone(),
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
        if witness.try_joined().is_none_or(|actual| {
            actual.id() != joined.task()
                || actual.class() != joined.class()
                || !actual.registration_released()
        }) {
            return Ok(false);
        }
        if logical
            .iter()
            .any(|original| !original.belongs_to(&witness) || !original.closed())
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
            ChildKind::Udp => Ok((!logical.is_empty() || datagram.is_some())
                && datagram.is_none_or(|provider| {
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
                // A real receiver registration also needs this continuation
                // to retain/accept its actual exit while Source collect runs.
                // This changes progress only, never native coverage evidence.
                record.receiver_starting
                    || record.receiver.is_some()
                    || record.dns.is_some()
                    || record.no_business_admission
                    || !record.pending_carriers.is_empty()
                    || record.protocols.is_some()
                    || record.gateways > 0
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
                // This is an old original's cleanup result, not proof about
                // the new reservation. Never swallow its nested Capacity as
                // a fresh not-submitted admission failure.
                self.settle_joined(deadline)
                    .await
                    .map_err(|source| rejected(Reason::Io).with_source(source))?;
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
        let mut failure = None;
        for (registration, result) in registrations.iter().zip(settled) {
            match result {
                Ok(true) => {
                    let removed = self
                        .0
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .records
                        .remove(&registration.id);
                    drop(removed);
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
