//! One real setup task, original selected graph and finite same-record claims.
use super::{state::Physical, *};
use std::sync::Arc;
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
    binding: UpstreamBinding,
    call: CarrierContext,
    caller: TaskJoinWitness,
    waiting: Option<PendingCarrierObserver>,
    limit: usize,
}
pub(super) fn transport(
    owner: Arc<Managed>,
    physical: Arc<Physical>,
    binding: UpstreamBinding,
    call: CarrierContext,
    waiting: Option<PendingCarrierObserver>,
    caller: TaskJoinWitness,
) -> Result<UpstreamTransport, ChannelError> {
    let limit = owner
        .upstream
        .pending_carrier_capacity_for(&binding)
        .ok_or_else(|| error(owner.instance, Stage::Prepare, Reason::StalePlan))?;
    Ok(UpstreamTransport::new(
        binding.clone(),
        Arc::new(Pending {
            owner,
            physical,
            binding,
            call,
            caller,
            waiting,
            limit,
        }),
    ))
}
impl Pending {
    fn check(&self, binding: &UpstreamBinding) -> Result<(), ChannelError> {
        let bound = self
            .owner
            .grants
            .registration()
            .bound()
            .ok_or_else(|| error(self.owner.instance, Stage::Prepare, Reason::Revoked))?;
        if binding != &self.binding
            || !self.owner.ready()
            || self.call.cancel.is_cancelled()
            || self.caller.is_joined()
            || !self.caller.scope_identity().resource_open()
            || !bound
                .scope_identity()
                .same_scope(&self.caller.scope_identity())
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
        Ok(self.caller.clone())
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
        let installed = self.adoption.retained_loan();
        let removed = observed
            .iter()
            .filter(|slot| {
                slot.closed().is_some_and(|closed| closed.matches(slot))
                    && slot
                        .users_closed()
                        .is_some_and(|closed| closed.matches(slot))
                    || installed.as_ref().is_some_and(|proof| proof.retains(slot))
                    || slot
                        .retained_driver()
                        .is_some_and(|proof| proof.retains(slot))
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
