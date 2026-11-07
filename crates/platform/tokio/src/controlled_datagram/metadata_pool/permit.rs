mod open;
use super::super::metadata::users::MetadataUsersClosedReceipt;
use super::*;
pub(crate) use open::MetadataCandidate;
pub struct MetadataPoolPermit(pub(super) Arc<Slot>);
#[derive(Clone)]
pub struct MetadataPoolOrigin {
    slot: Weak<Slot>,
    instance: ComponentInstanceId,
    ordinal: u64,
    closed: Arc<OnceLock<bool>>,
    owner: MetadataPoolRegistration,
    retired: CancellationToken,
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
        }
    }
    pub fn close(
        &self,
        receipt: Option<&MetadataUsersClosedReceipt>,
        tasks: &TaskRegistrar,
    ) -> Result<MetadataPoolClosed, ChannelError> {
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
            native.confirmed.get_or_insert_with(|| receipt.clone());
        }
        self.close_stored()?.ok_or_else(|| {
            error(
                Reason::Timeout,
                "metadata users or native original remains unconfirmed",
            )
        })
    }
    /// Native/users close remains independently observable. Only this owner's
    /// local reservation credit waits for every actual registration's Host ACK.
    pub(super) fn release_registration_credit(&self) -> bool {
        if self.0.closed.get().is_none() {
            return false;
        }
        let (caller, users) = {
            let Ok(native) = self.0.native() else {
                return false;
            };
            (self.0.caller.clone(), native.confirmed.clone())
        };
        // Fixed original snapshot: receipt Clone shares its already-owned Arc
        // registrations. No per-attempt witness Vec or full owner census copy.
        let released = |witness: &TaskJoinWitness| {
            witness.try_joined().is_some_and(|joined| {
                witness.matches_joined(&joined) && joined.registration_released()
            })
        };
        if !released(&caller)
            || users
                .as_ref()
                .is_some_and(|users| !users.registrations().iter().all(released))
        {
            return false;
        }
        if let Some(owner) = self.0.owner.upgrade() {
            let removed = {
                let mut state = owner.state.lock().expect("metadata pool poisoned");
                if state
                    .slots
                    .get(&self.0.ordinal)
                    .is_some_and(|actual| Arc::ptr_eq(actual, &self.0))
                {
                    state.slots.remove(&self.0.ordinal)
                } else {
                    None
                }
            };
            drop(removed);
        }
        true
    }
    pub(super) fn close_stored(&self) -> Result<Option<MetadataPoolClosed>, ChannelError> {
        if let Some(receipt) = self.origin().closed() {
            self.release_registration_credit();
            return Ok(Some(receipt));
        }
        let mut native = self.0.native()?;
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
        let first = report
            .main
            .unconfirmed
            .first()
            .or_else(|| report.probes.unconfirmed.first())
            .map(|unknown| unknown.source.clone());
        native.acquired_confirmed |= report.main.confirmed_closed > 0;
        let acquired = native.acquired_confirmed;
        native.report = Some(report);
        if let Some(source) = first {
            return Err(error(Reason::Io, "metadata original close is unknown")
                .with_source(OriginalIo(source)));
        }
        if !complete {
            return Ok(None);
        }
        let _ = self.0.closed.set(acquired);
        self.0.retired.cancel();
        drop(native);
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
}
#[derive(Debug)]
struct OriginalIo(Arc<std::io::Error>);
impl std::fmt::Display for OriginalIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for OriginalIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl MetadataPoolOrigin {
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
            && Arc::ptr_eq(&self.closed, &other.closed)
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
