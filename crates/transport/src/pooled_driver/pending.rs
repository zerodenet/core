//! One real setup task, original selected graph and finite same-record claims.
use super::{failure as error, ManagedPool as Managed, Physical};
use std::sync::Arc;
use zero_platform_tokio::contract::{ChannelError, Reason, Stage};
use zero_platform_tokio::{
    contract::{
        context::RequestIdentity,
        network::{
            CarrierContext, PendingCarrierObserver, UpstreamBinding, UpstreamOpener,
            UpstreamTransport,
        },
        scope::TaskJoinWitness,
        BoxFuture, DeliveryState,
    },
    controlled_carrier::CarrierAdoption,
};
struct Pending {
    owner: Arc<Managed>,
    physical: Arc<Physical>,
    call: CarrierContext,
    caller: TaskJoinWitness,
    waiting: Option<PendingCarrierObserver>,
    limit: usize,
    anchor: std::sync::OnceLock<PendingCarrierObserver>,
}
pub(super) fn transport(
    owner: Arc<Managed>,
    physical: Arc<Physical>,
    binding: UpstreamBinding,
    call: CarrierContext,
    waiting: Option<PendingCarrierObserver>,
    caller: TaskJoinWitness,
) -> Result<UpstreamTransport, ChannelError> {
    let limit = match owner.upstream.pending_carrier_capacity_for(&binding) {
        Some(actual) => actual,
        // A fixed root library provider has no foreign carrier graph. This
        // records no claims and grants no capacity/IO. Every nonempty graph
        // requires its actual declaration-derived finite census.
        None if binding.prefix.is_empty() => 0,
        None => return Err(error(owner.instance, Stage::Prepare, Reason::StalePlan)),
    };
    let instance = owner.instance;
    if physical.binding != binding {
        return Err(error(instance, Stage::Prepare, Reason::StalePlan));
    }
    let memory = owner
        .resources
        .upgrade()
        .ok_or_else(|| error(instance, Stage::Prepare, Reason::Revoked))?
        .packet_driver_metadata()?;
    let credit = memory
        .reserve(
            UpstreamTransport::opener_metadata_layout::<Pending>(&binding).map_err(|source| {
                error(instance, Stage::Prepare, Reason::Capacity).with_source(source)
            })?,
        )
        .map_err(|source| error(instance, Stage::Prepare, Reason::Capacity).with_source(source))?;
    let receiver = Pending {
        owner,
        physical,
        call,
        caller,
        waiting,
        limit,
        anchor: std::sync::OnceLock::new(),
    };
    let (transport, lease) = UpstreamTransport::from_prepaid_opener(binding, receiver, credit)
        .map_err(|(source, _, _, _)| {
            error(instance, Stage::Prepare, Reason::Capacity).with_source(source)
        })?;
    let anchor = transport.pending_carrier_observer();
    let _ = lease.actual().anchor.set(anchor);
    drop(lease);
    Ok(transport)
}
impl Pending {
    fn current_receiver(&self) -> Result<TaskJoinWitness, ChannelError> {
        // An unjoined initializer uses its original published start gate. A
        // later cached request is accepted only from this physical original's
        // installed retained driver, never an arbitrary task in the same Scope.
        let native = tokio::task::try_id();
        if self.physical.original.matches_auxiliary(&self.caller)
            && !self.caller.is_joined()
            && self
                .caller
                .native_task_id()
                .is_some_and(|id| Some(id) == native)
        {
            return Ok(self.caller.clone());
        }
        if let Some(driver) = self.physical.original.retained_driver() {
            if driver.retains(&self.physical.original)
                && driver
                    .adoption()
                    .is_some_and(|same| same.same_slot(&self.physical.original))
                && !driver.driver().is_joined()
                && driver
                    .driver()
                    .native_task_id()
                    .is_some_and(|id| Some(id) == native)
            {
                return Ok(driver.driver().clone());
            }
        }
        Err(error(
            self.owner.instance,
            Stage::Prepare,
            Reason::StalePlan,
        ))
    }

    fn check(&self, binding: &UpstreamBinding) -> Result<(), ChannelError> {
        let bound = self
            .owner
            .grants
            .registration()
            .bound()
            .ok_or_else(|| error(self.owner.instance, Stage::Prepare, Reason::Revoked))?;
        let caller = self.current_receiver()?;
        if binding != &self.physical.binding
            || !self.owner.ready()
            || self.call.cancel.is_cancelled()
            || caller.is_joined()
            || !caller.scope_identity().resource_open()
            || !bound.scope_identity().same_scope(&caller.scope_identity())
            || bound.host_cancelled()
        {
            return Err(error(
                self.owner.instance,
                Stage::Prepare,
                Reason::StalePlan,
            ));
        }
        Ok(())
    }
}
impl UpstreamOpener for Pending {
    fn protocol_original_observer(&self) -> Option<PendingCarrierObserver> {
        self.physical.protocols.as_ref()?;
        // Identity publication is not permission. A declared receiver remains
        // present on foreign/cancelled calls so retain returns its real error
        // before first IO rather than silently selecting NoObserver cleanup.
        self.anchor.get().cloned()
    }
    fn retain_protocol_original(
        &self,
        binding: &UpstreamBinding,
        original: zero_platform_tokio::controlled_carrier::ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        self.check(binding)?;
        self.current_receiver()?;
        if !self
            .anchor
            .get()
            .is_some_and(|anchor| original.matches_observer(anchor))
        {
            return Err(error(
                self.owner.instance,
                Stage::Prepare,
                Reason::StalePlan,
            ));
        }
        self.owner
            .upstream
            .validate_pending_protocol_original(binding, &original)?;
        self.physical
            .protocols
            .as_ref()
            .ok_or_else(|| error(self.owner.instance, Stage::Prepare, Reason::StalePlan))?
            .retain(original)
    }

    fn has_pending_carrier_observer(&self) -> bool {
        true
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        self.check(binding).ok().map(|_| self.limit)
    }
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &RequestIdentity,
        call: &CarrierContext,
    ) -> Result<TaskJoinWitness, ChannelError> {
        self.check(binding)?;
        self.current_receiver()?;
        if call.session.is_some()
            || call.deadline != self.call.deadline
            || call.cancel.is_cancelled()
            || !matches!(identity,RequestIdentity::Service(facts) if facts.origin==zero_core::request::ServiceOrigin::Runtime && facts.purpose==zero_core::request::ServicePurpose::BackgroundControl)
        {
            return Err(error(
                self.owner.instance,
                Stage::Prepare,
                Reason::StalePlan,
            ));
        }
        self.current_receiver()
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.check(binding)?;
        self.owner
            .upstream
            .validate_pending_carrier(binding, &adoption)?;
        if let Some(waiting) = &self.waiting {
            waiting.retain(adoption.clone())?;
        }
        self.physical.release_installed_pending();
        let mut pending = self.physical.pending.lock().unwrap();
        if pending.iter().any(|slot| slot.same_slot(&adoption)) {
            return Ok(());
        }
        if pending.len() >= self.limit {
            return Err(error(self.owner.instance, Stage::Prepare, Reason::Capacity));
        }
        pending.try_reserve(1).map_err(|source| {
            error(self.owner.instance, Stage::Prepare, Reason::Capacity).with_source(source)
        })?;
        pending.push(adoption);
        Ok(())
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: zero_core::request::Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::contract::stream::BoxStream, ChannelError>> {
        Box::pin(async {
            let mut failure = error(self.owner.instance, Stage::Prepare, Reason::Unsupported);
            failure.delivery = DeliveryState::NotSubmitted;
            Err(failure)
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async {
            let mut failure = error(self.owner.instance, Stage::Prepare, Reason::Unsupported);
            failure.delivery = DeliveryState::NotSubmitted;
            Err(failure)
        })
    }
}
impl Physical {
    pub(super) fn release_installed_pending(&self) {
        let observed = self.pending.lock().unwrap().clone();
        let installed = self.original.retained_loan();
        let owner = self.owner.upgrade();
        let removed = observed
            .iter()
            .filter(|slot| {
                slot.closed().is_some_and(|closed| closed.matches(slot))
                    && slot
                        .users_closed()
                        .is_some_and(|closed| closed.matches(slot))
                    || installed.as_ref().is_some_and(|proof| proof.retains(slot))
                    || slot.retained_driver().is_some_and(|proof| {
                        proof.retains(slot)
                            && owner.as_ref().is_some_and(|owner| {
                                owner
                                    .upstream
                                    .validate_pending_carrier(&self.binding, slot)
                                    .is_ok()
                            })
                    })
                    || slot.retained_preparation_job().is_some_and(|proof| {
                        proof.retains(slot)
                            && proof
                                .owner()
                                .is_some_and(|registration| registration.matches_adoption(slot))
                            && owner.as_ref().is_some_and(|owner| {
                                owner
                                    .upstream
                                    .validate_pending_carrier(&self.binding, slot)
                                    .is_ok()
                            })
                    })
                    || observed.iter().any(|receiver| {
                        !receiver.same_slot(slot)
                            && receiver
                                .retained_loan()
                                .is_some_and(|proof| proof.retains(slot))
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        self.pending
            .lock()
            .unwrap()
            .retain(|slot| !removed.iter().any(|removed| removed.same_slot(slot)));
    }
}
