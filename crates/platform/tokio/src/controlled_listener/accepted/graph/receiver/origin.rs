//! The actual accepted original and its independent real global return.
//! No native IO, alternate controller, task creator, ACK or pool is exposed.
use super::*;
use crate::contract::scope::TaskScopeIdentity;
use std::any::Any;
use zero_core::component::ComponentInstanceId;
#[derive(Clone)]
pub struct AcceptedTcpCarrierOrigin {
    // Same existing Entry Arc, unsized without an allocation. Keeping a
    // concrete Entry here would reopen recursive auto-trait storage paths.
    entry: Arc<dyn Any + Send + Sync>,
    original: crate::controlled_listener::totals::RegisteredSourceOriginal,
    instance: ComponentInstanceId,
    limit: NonZeroUsize,
    scope: TaskScopeIdentity,
}
impl AcceptedTcpTaskReceiver {
    pub fn carrier_origin(
        &self,
        claim: &AcceptedTcpClaim,
    ) -> Result<AcceptedTcpCarrierOrigin, ChannelError> {
        let pool = self.0.upgrade().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Revoked,
                "actual accepted provider retired",
            )
        })?;
        if !Arc::ptr_eq(&pool.identity, &claim.entry.identity) {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "accepted claim belongs to another actual provider",
            ));
        }
        let binding = pool.originals.get().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Unsupported,
                "accepted provider has no actual Host registration",
            )
        })?;
        let authority = pool.graph.as_ref().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Unsupported,
                "accepted provider has no original task graph",
            )
        })?;
        let users = claim.entry.users.as_ref().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::StalePlan,
                "accepted original has no actual users",
            )
        })?;
        if !authority.scope.same_scope(&binding.scope.scope_identity())
            || !users.authority.scope.same_scope(&authority.scope)
            || claim.entry.control.get().is_none()
        {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "accepted original differs from its actual bound Scope",
            ));
        }
        let returner = {
            claim
                .entry
                .original_return
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
        .ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::StalePlan,
                "accepted original was not registered in the Host Root",
            )
        })?;
        let original = returner.observe_registered()?;
        Ok(AcceptedTcpCarrierOrigin {
            entry: claim.entry.clone(),
            original,
            instance: binding.instance,
            limit: pool.limit,
            scope: authority.scope.clone(),
        })
    }
}
impl AcceptedTcpCarrierOrigin {
    fn entry(&self) -> &Entry {
        self.entry
            .as_ref()
            .downcast_ref::<Entry>()
            .expect("only actual accepted issuer constructs this original")
    }
    fn claim(&self) -> AcceptedTcpClaim {
        AcceptedTcpClaim {
            entry: self
                .entry
                .clone()
                .downcast::<Entry>()
                .unwrap_or_else(|_| unreachable!("actual accepted original type")),
        }
    }
    /// Publish only the same already registered Source original. This is
    /// metadata custody, never permission to open native or background IO.
    pub fn protocol_original(
        &self,
        upstream: &crate::contract::network::UpstreamTransport,
    ) -> Result<Option<crate::controlled_carrier::ProtocolOriginal>, ChannelError> {
        if self.is_retired() || !self.scope.resource_open() {
            return Err(failure(
                Stage::Prepare,
                Reason::Revoked,
                "accepted original is retiring",
            ));
        }
        let Some(observer) = upstream.protocol_original_observer() else {
            return Ok(None);
        };
        Ok(Some(
            crate::controlled_carrier::ProtocolOriginal::accepted_original(
                self.clone(),
                upstream.original_binding(),
                observer,
            ),
        ))
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn limit(&self) -> NonZeroUsize {
        self.limit
    }
    pub(crate) fn matches_closed(&self, closed: &AcceptedTcpClosed) -> bool {
        closed.matches(&self.claim())
    }
    pub fn is_retired(&self) -> bool {
        self.entry().cancel.is_cancelled()
    }
    pub async fn retired(&self) {
        self.entry().cancel.cancelled().await
    }
    pub fn scope_identity(&self) -> &TaskScopeIdentity {
        &self.scope
    }
    pub fn belongs_to_root(
        &self,
        root: &Arc<crate::controlled_carrier::CarrierOriginalTotals>,
    ) -> bool {
        self.original.same_root(root)
    }
    pub fn same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.entry, &other.entry) && self.original.same_original(&other.original)
    }
    pub fn native_closed(&self) -> Option<AcceptedTcpNativeClosed> {
        (self.entry().state().phase == Phase::Closed).then(|| AcceptedTcpNativeClosed {
            entry: self.claim().entry,
        })
    }
    pub fn closed(&self) -> Option<AcceptedTcpClosed> {
        let entry = self.entry();
        if entry.state().phase != Phase::Closed {
            return None;
        }
        let users = entry.users.as_ref()?;
        (users.complete() && users.original_acks_returned()).then(|| AcceptedTcpClosed {
            entry: self.claim().entry,
        })
    }
    pub fn is_settled(&self) -> bool {
        self.original.is_settled()
    }
    pub async fn wait_settled_original(&self) -> Result<(), ChannelError> {
        self.original.wait_settled().await
    }
    pub fn request_retirement(&self) {
        let entry = self.entry();
        loop {
            let before = {
                entry
                    .protocol_receiver
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            };
            if before
                .as_ref()
                .and_then(|proof| proof.as_ref().ok())
                .is_some_and(|proof| proof.observation(self).is_some())
            {
                return;
            }
            let mut guard = entry
                .protocol_receiver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if before.is_none() && guard.is_some() {
                drop(guard);
                continue;
            }
            // No new receiver may publish once this exact original's real
            // retirement starts. This records admission only, never close.
            let previous = guard.replace(Err(()));
            drop(guard);
            entry.cancel.cancel();
            if let Some(users) = &entry.users {
                users.cancel.cancel();
                users.changed.notify_waiters();
            }
            // Witness/keeper final Drop and arbitrary synchronous wakers may
            // re-enter observation, hence neither executes under the gate.
            drop(previous);
            drop(before);
            return;
        }
    }
    pub(crate) fn receiver(&self) -> Option<crate::controlled_carrier::ProtocolOriginalReceiver> {
        let proof = {
            self.entry()
                .protocol_receiver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .and_then(|proof| proof.as_ref().ok())
                .cloned()
        }?;
        proof.observation(self)
    }
    pub(crate) fn install_receiver(
        &self,
        proof: crate::controlled_carrier::AcceptedLoanReceiver,
    ) -> Result<(), ChannelError> {
        if proof.observation(self).is_none() {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "accepted original is not retained by the actual cached loan",
            ));
        }
        let mut current = self
            .entry()
            .protocol_receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_retired() || current.is_some() {
            drop(current);
            return Err(failure(
                Stage::Prepare,
                Reason::Revoked,
                "accepted original already retired or received",
            ));
        }
        *current = Some(Ok(proof));
        drop(current);
        self.entry().protocol_changed.notify_waiters();
        Ok(())
    }
    pub(crate) async fn wait_returned_original(&self) -> Result<(), ChannelError> {
        self.request_retirement();
        loop {
            let received = self.entry().protocol_changed.notified();
            tokio::pin!(received);
            received.as_mut().enable();
            let settled = self.original.wait_settled();
            tokio::pin!(settled);
            if self.is_settled() || self.receiver().is_some() {
                return Ok(());
            }
            tokio::select! { result = &mut settled => return result, _ = &mut received => {} }
        }
    }
    pub fn matches_receiver(&self, receiver: &AcceptedTcpTaskReceiver) -> bool {
        receiver.0.upgrade().is_some_and(|pool| {
            Arc::ptr_eq(&pool.identity, &self.entry().identity)
                && pool.originals.get().is_some_and(|actual| {
                    actual.instance == self.instance
                        && actual.scope.scope_identity().same_scope(&self.scope)
                })
        })
    }
}
