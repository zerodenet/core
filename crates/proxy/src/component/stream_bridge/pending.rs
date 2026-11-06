//! The actual accepted caller retains pre-delivery originals in its own ledger.
//! This weak observer grants no native permission and owns no task or pool.
use super::*;
use crate::{
    component::{business_demand::AdmittedBusinessDemand, direct_bridge::Invocation},
    runtime::current_services::CurrentServiceSource,
};
use std::sync::Weak;
use zero_core::component::RoleInstanceId;
use zero_platform_tokio::{
    contract::network::{PendingCarrierObserver, UpstreamOpener},
    controlled_carrier::CarrierAdoption,
    AcceptedCaller,
};

pub(super) struct PendingCaller(UpstreamTransport);
struct Observer {
    source: Weak<CurrentServiceSource>,
    caller: AcceptedCaller,
    admitted: AdmittedBusinessDemand,
    roles: Arc<[RoleInstanceId]>,
    binding: UpstreamBinding,
}
impl PendingCaller {
    pub(super) fn new(
        services: &TcpExecutionServices,
        invocation: &Invocation,
        roles: Vec<RoleInstanceId>,
        binding: UpstreamBinding,
    ) -> Result<Option<Self>, ChannelError> {
        let Some(caller) = invocation.caller_accepted.clone() else {
            // Raw/diagnostic callers require their own real finalizer. A
            // borrowed accepted identity cannot stand in for those owners.
            return Ok(None);
        };
        let zero_platform_tokio::contract::context::RequestIdentity::Business(facts) =
            &invocation.identity
        else {
            return Err(invalid(
                Reason::StalePlan,
                "accepted pending caller lacks its business facts",
            ));
        };
        let admitted = invocation.admitted.clone().ok_or_else(|| {
            invalid(
                Reason::Revoked,
                "accepted pending caller lacks actual Engine admission",
            )
        })?;
        admitted
            .check_live(services.engine(), facts.session)
            .map_err(|source| {
                invalid(Reason::Revoked, "accepted pending caller admission ended")
                    .with_source(source)
            })?;
        caller.check_open()?;
        let plan = services.components.as_ref().map_err(|source| {
            invalid(
                Reason::StalePlan,
                "pending caller has no actual selected host",
            )
            .with_source(source.clone())
        })?;
        if plan.pending_carrier_capacity(&roles)? == 0 {
            return Ok(None);
        }
        let observer = Observer {
            source: services.current_source.clone(),
            caller,
            admitted,
            roles: roles.into(),
            binding: binding.clone(),
        };
        Ok(Some(Self(UpstreamTransport::new(
            binding,
            Arc::new(observer),
        ))))
    }
    pub(super) fn observer(&self) -> PendingCarrierObserver {
        self.0.pending_carrier_observer()
    }
}
impl UpstreamOpener for Observer {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: zero_platform_tokio::contract::network::CarrierContext,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::stream::BoxStream, ChannelError>,
    > {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "caller cleanup observer grants no I/O",
            ))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: zero_platform_tokio::contract::network::CarrierContext,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "caller cleanup observer grants no I/O",
            ))
        })
    }

    fn has_pending_carrier_observer(&self) -> bool {
        true
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        if binding != &self.binding || self.caller.check_open().is_err() {
            return None;
        }
        let source = self.source.upgrade()?;
        let services = source.runtime_services().ok()?;
        services
            .execution()
            .components
            .as_ref()
            .ok()?
            .pending_carrier_capacity(&self.roles)
            .ok()
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if binding != &self.binding {
            return Err(invalid(Reason::StalePlan, "pending caller binding changed"));
        }
        self.caller.check_open()?;
        self.admitted.check_active().map_err(|source| {
            invalid(Reason::Revoked, "pending caller Engine admission ended").with_source(source)
        })?;
        let services = self
            .source
            .upgrade()
            .ok_or_else(|| invalid(Reason::Revoked, "pending caller host retired"))?
            .runtime_services()
            .map_err(|source| {
                invalid(Reason::Revoked, "pending caller services retired").with_source(source)
            })?;
        let execution = services.execution();
        let plan = execution.components.as_ref().map_err(|source| {
            invalid(Reason::StalePlan, "pending caller plan retired").with_source(source.clone())
        })?;
        if binding.network_generation.0 != services.upstream().network_generation()
            || !self
                .roles
                .iter()
                .any(|role| plan.validate_carrier_adoption(*role, &adoption).is_ok())
        {
            return Err(invalid(
                Reason::StalePlan,
                "pending original differs from actual selected caller graph",
            ));
        }
        self.caller.observe_pending_carrier(adoption)
    }
}
