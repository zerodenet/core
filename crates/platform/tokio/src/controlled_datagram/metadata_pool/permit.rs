mod credit;
mod open;
use super::super::metadata::users::MetadataUsersClosedReceipt;
use super::*;
pub(crate) use open::MetadataCandidate;
pub struct MetadataPoolPermit(pub(super) Arc<Slot>);
#[derive(Clone)]
pub struct MetadataPoolOrigin {
    pub(super) slot: Weak<Slot>,
    instance: ComponentInstanceId,
    ordinal: u64,
    closed: completion::Completion,
    owner: MetadataPoolRegistration,
    retired: CancellationToken,
    sources: Option<MetadataOriginalSources>,
}
#[derive(Clone)]
pub struct MetadataPoolClosed {
    origin: MetadataPoolOrigin,
}
impl MetadataPoolPermit {
    /// Return the unique claim rejected before a user/driver graph existed.
    /// Retain it until all actual original views are gone; no empty graph or
    /// invented driver outcome substitutes for this original capability.
    /// Rejection leaves the caller's unique claim untouched.
    pub fn return_unbound(
        &self,
        claim: &mut Option<super::super::metadata::MetadataViewClaim>,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        let owner = self
            .0
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "metadata pool dropped"))?;
        let scope = owner
            .state
            .lock()
            .expect("metadata pool poisoned")
            .bound
            .as_ref()
            .map(|bound| bound.scope.clone())
            .ok_or_else(|| error(Reason::InvalidConfig, "metadata pool unregistered"))?;
        if !scope.matches(tasks) {
            return Err(error(Reason::StalePlan, "foreign unbound metadata scope"));
        }
        let mut native = self.0.native()?;
        if native.confirmed.is_some() || native.unbound.is_some() {
            return Err(error(
                Reason::InvalidConfig,
                "metadata claim already returned",
            ));
        }
        if !claim.as_ref().is_some_and(|claim| {
            native
                .issuer
                .as_ref()
                .is_some_and(|issuer| issuer.owns_claim(claim, tasks))
        }) {
            return Err(error(Reason::StalePlan, "foreign original metadata claim"));
        }
        native.unbound = Some((
            claim.take().expect("validated original claim"),
            tasks.clone(),
        ));
        Ok(())
    }
    pub fn origin(&self) -> MetadataPoolOrigin {
        MetadataPoolOrigin {
            slot: Arc::downgrade(&self.0),
            instance: self.0.instance,
            ordinal: self.0.ordinal,
            closed: self.0.closed.clone(),
            owner: self.0.registration.clone(),
            retired: self.0.retired.clone(),
            sources: self.0.close_cohort.get().map(|cohort| cohort.originals()),
        }
    }
    pub fn close(
        &self,
        receipt: Option<&MetadataUsersClosedReceipt>,
        tasks: &TaskRegistrar,
    ) -> Result<MetadataPoolClosed, ChannelError> {
        self.retain_users_inner(receipt, tasks)?;
        self.close_stored()?.ok_or_else(|| {
            error(
                Reason::Timeout,
                "metadata users or native original remains unconfirmed",
            )
        })
    }
    /// Drive this exact close while another observer holds its native gate.
    /// The caller retains its original observation deadline.
    pub async fn close_original(
        &self,
        receipt: Option<&MetadataUsersClosedReceipt>,
        tasks: &TaskRegistrar,
    ) -> Result<MetadataPoolClosed, ChannelError> {
        retry_native_busy(|| self.close(receipt, tasks)).await
    }
    /// Retain only the same actual users/driver graph receipt. The persistent
    /// Root still closes the original native main/probes and checks real ACKs.
    async fn retain_users_receipt(
        &self,
        receipt: &MetadataUsersClosedReceipt,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        retry_native_busy(|| self.retain_users_inner(Some(receipt), tasks)).await
    }
    fn retain_users_inner(
        &self,
        receipt: Option<&MetadataUsersClosedReceipt>,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        let owner = self
            .0
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "metadata pool dropped"))?;
        let bound = owner
            .state
            .lock()
            .expect("metadata pool poisoned")
            .bound
            .clone()
            .ok_or_else(|| error(Reason::InvalidConfig, "metadata original has no binding"))?;
        if !bound.scope.matches(tasks) {
            return Err(error(Reason::StalePlan, "foreign metadata close scope"));
        }
        if let Some(receipt) = receipt {
            let mut native = self.0.native()?;
            if !native
                .issuer
                .as_ref()
                .is_some_and(|issuer| issuer.validates_users(receipt, tasks))
            {
                return Err(error(
                    Reason::StalePlan,
                    "metadata users receipt belongs to another original",
                ));
            }
            if let Some(previous) = native.confirmed.as_ref() {
                if let Some(completed) = previous.with_later_protocol_receipt(receipt) {
                    native.confirmed = Some(completed);
                }
            } else {
                native.confirmed = Some(receipt.clone());
            }
        }
        Ok(())
    }
    pub(super) fn close_stored(&self) -> Result<Option<MetadataPoolClosed>, ChannelError> {
        let phase = self
            .0
            .protocol
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if phase.as_ref().is_some_and(|phase| !phase.complete()) {
            return Ok(None);
        }
        // A real gated initializer must either install its actual witness or
        // abandon its unstarted reservation before native close may settle it.
        if self.0.preparing.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(None);
        }
        if let Some(receipt) = self.origin().closed() {
            self.release_registration_credit();
            return Ok(Some(receipt));
        }
        if self.close_native_views()?.is_none() {
            return Ok(None);
        }
        let acquired = self.0.native()?.acquired_confirmed;
        let _ = self.0.closed.set(acquired);
        self.0.retired.cancel();
        if let Some(owner) = self.0.owner.upgrade() {
            if acquired {
                owner
                    .confirmed_native
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
        self.release_registration_credit();
        Ok(self.origin().closed())
    }

    /// Native FD/probe close after the exact view/driver receipt. This does not
    /// complete the enclosing protocol phase or release its original Root N.
    fn close_native_views(&self) -> Result<Option<Arc<MetadataOwnerReport>>, ChannelError> {
        if self.0.preparing.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(None);
        }
        let mut native = self.0.native()?;
        if native.wire_closed {
            return Ok(native.report.clone());
        }
        let returned_unbound = native.unbound.as_ref().is_some_and(|(claim, tasks)| {
            claim.try_closed().is_some_and(|views| {
                native
                    .issuer
                    .as_ref()
                    .is_some_and(|issuer| issuer.validates(&views, tasks))
            })
        });
        if native.transferred && native.confirmed.is_none() && !returned_unbound {
            return Ok(None);
        }
        let report = Arc::new(native.owner.close_all());
        let complete = report.is_complete();
        let unknown = !report.main.unconfirmed.is_empty() || !report.probes.unconfirmed.is_empty();
        native.acquired_confirmed |= report.main.confirmed_closed > 0;
        // All actual sources were captured by MetadataOwner before report
        // extraction. Retire the former report only after the native lock.
        let previous = native.report.replace(report.clone());
        if unknown {
            let source = self
                .0
                .close_cohort
                .get()
                .and_then(|cohort| cohort.failure())
                .expect("captured actual unknown native original");
            drop(native);
            drop(previous);
            return Err(ChannelError::retained(source));
        }
        if !complete {
            drop(native);
            drop(previous);
            return Ok(None);
        }
        native.wire_closed = true;
        drop(native);
        drop(previous);
        Ok(Some(report))
    }
}
impl MetadataPoolOrigin {
    /// Consume this original's actual users/driver/view receipt and close only
    /// its native metadata wire. The protocol cleanup owner must still finish
    /// its phase and Scope/registration ACK; this is not MetadataPoolClosed.
    pub fn close_native_wire(
        &self,
        receipt: &MetadataUsersClosedReceipt,
        tasks: &TaskRegistrar,
    ) -> Result<Option<Arc<MetadataOwnerReport>>, ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(|| {
            error(
                Reason::Revoked,
                "metadata wire owner disappeared before native close",
            )
        })?;
        let permit = MetadataPoolPermit(slot);
        permit.retain_users_inner(Some(receipt), tasks)?;
        permit.close_native_views()
    }
    /// Wait for the same native gate before consuming the wire close result.
    pub async fn close_native_wire_original(
        &self,
        receipt: &MetadataUsersClosedReceipt,
        tasks: &TaskRegistrar,
    ) -> Result<Option<Arc<MetadataOwnerReport>>, ChannelError> {
        retry_native_busy(|| self.close_native_wire(receipt, tasks)).await
    }
    /// Read only the actual establishing registration already installed in this
    /// original. No registrar, new task, native grant or ACK is reconstructed.
    pub fn initializer_registration(&self) -> Option<TaskJoinWitness> {
        self.slot.upgrade()?.caller.get().cloned()
    }

    pub fn return_unbound(
        &self,
        claim: &mut Option<super::super::metadata::MetadataViewClaim>,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(|| {
            error(
                Reason::Revoked,
                "metadata original disappeared before claim return",
            )
        })?;
        MetadataPoolPermit(slot).return_unbound(claim, tasks)
    }
    /// Retry only transient contention while returning this same unique claim.
    pub async fn return_unbound_original(
        &self,
        claim: &mut Option<super::super::metadata::MetadataViewClaim>,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        retry_native_busy(|| self.return_unbound(claim, tasks)).await
    }
    /// Same original issuer/scope checks, without retaining its Slot from an
    /// installed protocol controller. This cannot open IO or mint a receipt.
    pub async fn retain_users_receipt(
        &self,
        receipt: &MetadataUsersClosedReceipt,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(|| {
            error(
                Reason::Revoked,
                "metadata original disappeared before actual receipt",
            )
        })?;
        MetadataPoolPermit(slot)
            .retain_users_receipt(receipt, tasks)
            .await
    }

    pub fn registration_observation(&self) -> MetadataPoolRegistrationObservation {
        self.closed.observation(self.instance, self.ordinal)
    }
    /// Durable native-only sources. No Slot, controller, Scope or receiver back edge.
    pub fn original_sources(&self) -> Option<MetadataOriginalSources> {
        self.sources.clone()
    }
    pub fn is_retired(&self) -> bool {
        self.retired.is_cancelled() || self.slot.upgrade().is_none()
    }
    pub async fn retired(&self) {
        self.retired.cancelled().await;
    }
    pub fn selected_binding(
        &self,
    ) -> Option<(
        zero_core::request::Destination,
        crate::contract::network::UpstreamBinding,
    )> {
        self.binding_snapshot()
            .map(|(endpoint, binding, _)| (endpoint, binding))
    }
    pub(crate) fn binding_snapshot(&self) -> Option<MetadataPoolBinding> {
        self.slot.upgrade()?.native().ok()?.binding.clone()
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn owner(&self) -> &MetadataPoolRegistration {
        &self.owner
    }
    pub fn same_original(&self, other: &Self) -> bool {
        self.instance == other.instance
            && self.ordinal == other.ordinal
            && self.closed.same(&other.closed)
    }
    pub fn closed(&self) -> Option<MetadataPoolClosed> {
        self.closed.get().map(|_| MetadataPoolClosed {
            origin: self.clone(),
        })
    }
    pub fn endpoints(&self) -> Option<(std::net::SocketAddr, std::net::SocketAddr)> {
        let slot = self.slot.upgrade()?;
        let result = slot.native().ok()?.endpoints;
        result
    }
}
impl MetadataPoolClosed {
    pub fn matches(&self, origin: &MetadataPoolOrigin) -> bool {
        self.origin.same_original(origin) && self.origin.closed.get().is_some()
    }
    pub fn had_native_acquisition(&self) -> bool {
        self.origin.closed.get() == Some(&true)
    }
}
