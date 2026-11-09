//! Exact original consumer dependency; distinct from native encapsulation.
use super::*;
use crate::{
    contract::network::UpstreamBinding,
    controlled_carrier::{CarrierGrant, CarrierOwnerBinding, CarrierOwnerRegistration},
    EgressInterfaceControl,
};
/// A single grant's sealed derived peer. It grants neither Business/Service
/// admission nor native capacity. Its weak back-reference cannot pin the job.
#[derive(Clone)]
pub struct ProcessEndpointUse {
    original: Weak<ProcessEndpointHandoff>,
    consumer: CarrierOwnerRegistration,
    binding: CarrierOwnerBinding,
    claim: CarrierAdoption,
    egress: Option<Arc<EgressInterfaceControl>>,
}
#[derive(Clone)]
pub(crate) struct ProcessEndpointFence {
    original: Weak<ProcessEndpointHandoff>,
    consumer: CarrierOwnerRegistration,
    binding: CarrierOwnerBinding,
    egress: Option<Arc<EgressInterfaceControl>>,
}
impl ProcessEndpointFence {
    pub(crate) fn check(&self) -> io::Result<()> {
        let owner = self
            .original
            .upgrade()
            .ok_or_else(|| denied("derived original was released"))?;
        owner.live_process()?;
        if self.binding.host_cancelled()
            || !self.binding.matches_registration(&self.consumer)
            || !self.binding.scope_identity().resource_open()
            || self
                .egress
                .as_ref()
                .is_none_or(|egress| egress.generation() != owner.spec.network_generation.0)
            || !self
                .consumer
                .observe()
                .is_some_and(|actual| actual.is_live() && !actual.is_stopped())
        {
            return Err(denied(
                "derived original consumer or network is no longer current",
            ));
        }
        Ok(())
    }
}
impl ProcessEndpointHandoff {
    /// Actual Host only, after current-plan direct-dependency and both started
    /// instances are verified. The platform independently checks exact native
    /// registration/Scope/grant, rather than trusting a claimed consumer ID.
    pub fn authorize_consumer(
        self: &Arc<Self>,
        host_fence: &Arc<CancellationToken>,
        bound: CarrierOwnerBinding,
        grant: &CarrierGrant,
    ) -> io::Result<ProcessEndpointUse> {
        self.live_process()?;
        let consumer = bound.registration().clone();
        if !bound.matches_host_cancel(host_fence)
            || !consumer.matches_grant(grant)
            || !bound.matches_registration(&consumer)
            || bound.host_cancelled()
            || !bound.scope_identity().resource_open()
            || !consumer
                .observe()
                .is_some_and(|owner| owner.is_live() && !owner.is_stopped())
        {
            return Err(denied(
                "derived endpoint has no actual registered consumer grant",
            ));
        }
        Ok(ProcessEndpointUse {
            original: Arc::downgrade(self),
            consumer,
            binding: bound,
            claim: grant.adoption(),
            egress: None,
        })
    }
}
impl ProcessEndpointUse {
    pub fn process_instance(&self) -> Option<ComponentInstanceId> {
        Some(self.original.upgrade()?.instance)
    }
    pub fn peer(&self) -> io::Result<SocketAddr> {
        self.owner()?.address()
    }
    fn owner(&self) -> io::Result<Arc<ProcessEndpointHandoff>> {
        self.original
            .upgrade()
            .ok_or_else(|| denied("original endpoint handoff was released"))
    }
    pub(crate) fn with_egress(mut self, egress: Arc<EgressInterfaceControl>) -> io::Result<Self> {
        if egress.generation() != self.owner()?.spec.network_generation.0 {
            return Err(denied(
                "derived endpoint has a different actual network generation",
            ));
        }
        self.egress = Some(egress);
        self.check()?;
        Ok(self)
    }
    pub(crate) fn check(&self) -> io::Result<()> {
        let owner = self.owner()?;
        owner.live_process()?;
        if !self.binding.matches_registration(&self.consumer)
            || self.binding.host_cancelled()
            || !self.binding.scope_identity().resource_open()
            || self.claim.closed().is_some()
            || self
                .egress
                .as_ref()
                .is_some_and(|egress| egress.generation() != owner.spec.network_generation.0)
            || !self
                .consumer
                .observe()
                .is_some_and(|actual| actual.is_live() && !actual.is_stopped())
        {
            return Err(denied(
                "derived endpoint consumer is not its live original registration",
            ));
        }
        Ok(())
    }
    pub(crate) fn fence(&self) -> ProcessEndpointFence {
        ProcessEndpointFence {
            original: self.original.clone(),
            consumer: self.consumer.clone(),
            binding: self.binding.clone(),
            egress: self.egress.clone(),
        }
    }
    pub(crate) fn claim(
        &self,
        grant: &CarrierGrant,
        binding: &UpstreamBinding,
        datagram: bool,
    ) -> io::Result<()> {
        self.check()?;
        let owner = self.owner()?;
        if !self.claim.same_slot(&grant.adoption())
            || !self.consumer.matches_grant(grant)
            || binding.network_generation != owner.spec.network_generation
            || if datagram {
                !owner.spec.datagram
            } else {
                !owner.spec.stream
            }
        {
            return Err(denied(
                "derived endpoint use crossed its original slot, transport or epoch",
            ));
        }
        // Actual receipts sampled outside the original metadata lock. Equal
        // instance IDs do not merge different native registrations.
        let previous = { owner.lock().users.clone() };
        let settled: Vec<_> = previous
            .iter()
            .filter(|user| {
                user.adoption
                    .closed()
                    .is_some_and(|closed| closed.matches(&user.adoption))
                    && user
                        .adoption
                        .users_closed()
                        .is_some_and(|closed| closed.matches(&user.adoption))
            })
            .map(|user| user.adoption.clone())
            .collect();
        let (removed, accepted) = {
            let mut state = owner.lock();
            if state.stopping {
                return Err(denied("derived admission is terminal"));
            }
            let mut removed = Vec::new();
            let old = std::mem::take(&mut state.users);
            for user in old {
                if settled
                    .iter()
                    .any(|closed| closed.same_slot(&user.adoption))
                {
                    removed.push(user);
                } else {
                    state.users.push(user);
                }
            }
            let already = state
                .users
                .iter()
                .any(|user| user.adoption.same_slot(&self.claim));
            let used = state
                .users
                .iter()
                .filter(|user| user.owner.same_owner(&self.consumer))
                .count();
            let accepted = already || used < self.consumer.limit().get();
            if accepted && !already {
                // Strong cleanup capability before native creation/first await.
                // Its back-reference in the Slot's fence remains weak.
                state.users.push(User {
                    adoption: self.claim.clone(),
                    owner: self.consumer.clone(),
                });
            }
            (removed, accepted)
        };
        drop(removed);
        if !accepted {
            return Err(denied("actual consumer's original capacity is occupied"));
        }
        if owner.cancel.is_cancelled() {
            self.claim.request_retirement();
            return Err(denied("endpoint revoked during actual admission"));
        }
        Ok(())
    }
}
