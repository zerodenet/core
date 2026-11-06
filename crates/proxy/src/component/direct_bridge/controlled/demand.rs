//! One actual Direct request, with business and diagnostic authority kept distinct.
use super::*;
use crate::component::{direct_bridge::Invocation, instance::ComponentSlot};
use std::sync::Weak;
use zero_platform_tokio::{
    contract::scope::TaskJoinWitness,
    controlled_carrier::{CarrierGrant, ControlledCarrier},
};

#[derive(Clone)]
pub(in crate::component) enum DirectDemand {
    Business(BusinessCarrierRequest),
    Probe(ProbeRequest),
}
#[derive(Clone)]
pub(in crate::component) struct ProbeRequest {
    call: zero_runtime::ProbeCall,
    slot: Weak<ComponentSlot>,
    destination: Destination,
}
impl ProbeRequest {
    pub(in crate::component) fn new(
        slot: &Arc<ComponentSlot>,
        invocation: &Invocation,
        destination: Destination,
    ) -> Result<Option<Self>, ChannelError> {
        let Some(call) = &invocation.caller_probe else {
            return Ok(None);
        };
        if !matches!(&invocation.identity,zero_platform_tokio::contract::context::RequestIdentity::Service(facts) if Arc::ptr_eq(facts,&call.identity()))
            || invocation.deadline != Some(call.deadline())
        {
            return Err(invalid(
                Reason::StalePlan,
                "probe identity differs from its original task",
            ));
        }
        call.check_open()?;
        Ok(Some(Self {
            call: call.clone(),
            slot: Arc::downgrade(slot),
            destination,
        }))
    }
    fn owner(&self) -> Result<Arc<ComponentSlot>, ChannelError> {
        self.call.check_open()?;
        self.slot
            .upgrade()
            .ok_or_else(|| invalid(Reason::Revoked, "probe original Direct was retired"))
    }
    fn issuer(
        &self,
        caller: TaskJoinWitness,
        endpoint: &Destination,
        binding: &UpstreamBinding,
        grant: &CarrierGrant,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierServiceGrant, ChannelError> {
        self.owner()?
            .issue_direct_probe_carrier(&self.call, caller, endpoint, binding, grant)
    }
}
impl DirectDemand {
    pub(in crate::component) fn check_direct_destination(
        &self,
        destination: &Destination,
    ) -> Result<(), ChannelError> {
        match self {
            Self::Business(request) => request.check_direct_destination(destination),
            Self::Probe(request) => {
                request.call.check_open()?;
                if destination != &request.destination {
                    return Err(invalid(
                        Reason::StalePlan,
                        "probe Direct destination changed",
                    ));
                }
                Ok(())
            }
        }
    }
    pub(in crate::component) fn check_direct_owner(
        &self,
        policy: &DialPolicy,
        registration: &zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration,
        tasks: &WeakTaskRegistrar,
    ) -> Result<(), ChannelError> {
        match self {
            Self::Business(request) => request.check_direct_owner(policy, registration, tasks),
            Self::Probe(request) => request.owner()?.check_direct_probe_owner(
                &request.call,
                policy,
                registration,
                &tasks.scope_identity(),
            ),
        }
    }
    pub(super) fn retain_refresh(
        &self,
        candidate: &DirectCandidate,
        binding: &UpstreamBinding,
        grant: &CarrierGrant,
    ) -> Result<(), ChannelError> {
        match self {
            Self::Business(request) => {
                request.authorize_direct(candidate, binding, grant)?;
            }
            Self::Probe(request) => {
                request.call.check_open()?;
                if candidate.generation() != binding.network_generation {
                    return Err(invalid(Reason::StalePlan, "probe DNS generation changed"));
                }
                request.call.observe_pending_carrier(grant.adoption())?;
            }
        }
        Ok(())
    }
    pub(super) async fn open(
        &self,
        candidate: DirectCandidate,
        binding: UpstreamBinding,
        context: CarrierContext,
        grant: CarrierGrant,
        tasks: &WeakTaskRegistrar,
        control: EgressInterfaceControl,
    ) -> Result<ControlledCarrier, ChannelError> {
        let endpoint = ControlledCandidates::destination(candidate.peer());
        match self {
            Self::Business(request) => {
                let ticket = request.authorize_direct(&candidate, &binding, &grant)?;
                NativeCarrierOpener::business(ticket, Arc::new(control), None)?
                    .prepare_controlled_stream(&binding, endpoint, context.clone(), grant)?
                    .open_with_deadline(context.deadline)
                    .await
            }
            Self::Probe(request) => {
                if candidate.generation() != binding.network_generation {
                    return Err(invalid(
                        Reason::StalePlan,
                        "probe native candidate generation changed",
                    ));
                }
                request.call.observe_pending_carrier(grant.adoption())?;
                let caller = request
                    .call
                    .witness()
                    .ok_or_else(|| invalid(Reason::Revoked, "probe task missing"))?;
                if caller.scope_identity().same_scope(&tasks.scope_identity()) {
                    let ticket = request.issuer(caller, &endpoint, &binding, &grant)?;
                    return NativeCarrierOpener::service(ticket, Arc::new(control), None)?
                        .prepare_controlled_stream(&binding, endpoint, context.clone(), grant)?
                        .open_with_deadline(context.deadline)
                        .await;
                }
                // A configured Direct has its own original Scope. Register the
                // actual initializer there; the outer probe remains its caller.
                let adoption = grant.adoption();
                let (begin, started) = tokio::sync::oneshot::channel();
                let (send, receive) = tokio::sync::oneshot::channel();
                let actual = request.clone();
                let (_, witness) = adoption.spawn_auxiliary_weak(tasks, async move {
                    let Ok(caller) = started.await else {
                        return Ok(());
                    };
                    let result = async {
                        let ticket = actual.issuer(caller, &endpoint, &binding, &grant)?;
                        NativeCarrierOpener::service(ticket, Arc::new(control), None)?
                            .prepare_controlled_stream(&binding, endpoint, context.clone(), grant)?
                            .open_with_deadline(context.deadline)
                            .await
                    }
                    .await;
                    let _ = send.send(result);
                    Ok(())
                })?;
                let _ = begin.send(witness);
                let result = receive.await;
                adoption.accept_auxiliary().await?;
                result.map_err(|source| {
                    invalid(
                        Reason::Cancelled,
                        "probe initializer did not deliver its original",
                    )
                    .with_source(source)
                })?
            }
        }
    }
}
