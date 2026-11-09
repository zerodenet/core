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
mod caller;
mod protocol;
pub(crate) use caller::actual_protocol_caller;
use caller::Caller;

struct Observer {
    source: Weak<CurrentServiceSource>,
    caller: Caller,
    admitted: Option<AdmittedBusinessDemand>,
    roles: Vec<RoleInstanceId>,
    anchor: std::sync::OnceLock<PendingCarrierObserver>,
    _roles_memory: zero_traits::packet_metadata::PacketMetadataCredit,
}
impl PendingCaller {
    pub(super) fn new(
        services: &TcpExecutionServices,
        invocation: &Invocation,
        roles: Vec<RoleInstanceId>,
        binding: UpstreamBinding,
    ) -> Result<Option<Self>, ChannelError> {
        let caller = invocation.caller_accepted.clone().map(Caller::Accepted);
        #[cfg(feature = "raw-ip-runtime")]
        let caller = caller.or_else(|| invocation.caller_native.clone().map(Caller::Raw));
        let caller = caller.or_else(|| invocation.caller_probe.clone().map(Caller::Probe));
        let Some(caller) = caller else {
            // Diagnostics still require their own actual invocation Scope;
            // neither a raw nor an accepted caller may impersonate it.
            return Ok(None);
        };
        let admitted = match (&caller, &invocation.identity) {
            (
                Caller::Probe(call),
                zero_platform_tokio::contract::context::RequestIdentity::Service(facts),
            ) if facts.matches_shared(&call.identity())
                && invocation.deadline == Some(call.deadline()) =>
            {
                None
            }
            (Caller::Probe(_), _) => {
                return Err(invalid(
                    Reason::StalePlan,
                    "probe pending caller facts changed",
                ))
            }
            (_, zero_platform_tokio::contract::context::RequestIdentity::Business(facts)) => {
                let admitted = invocation.admitted.clone().ok_or_else(|| {
                    invalid(
                        Reason::Revoked,
                        "pending caller lacks actual Engine admission",
                    )
                })?;
                admitted
                    .check_live(services.engine(), facts.session)
                    .map_err(|source| {
                        invalid(Reason::Revoked, "pending caller admission ended")
                            .with_source(source)
                    })?;
                Some(admitted)
            }
            _ => {
                return Err(invalid(
                    Reason::StalePlan,
                    "pending caller lacks actual invocation facts",
                ))
            }
        };
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
        let memory = caller.protocol_metadata()?;
        let roles_memory = memory
            .reserve(
                zero_traits::packet_metadata::PacketMetadataLayout::array::<RoleInstanceId>(
                    roles.capacity(),
                )
                .map_err(|source| {
                    invalid(Reason::Capacity, "pending role storage overflow").with_source(source)
                })?,
            )
            .map_err(|source| {
                invalid(Reason::Capacity, "pending role metadata capacity").with_source(source)
            })?;
        let opener_memory = memory
            .reserve(
                UpstreamTransport::opener_metadata_layout::<Observer>(&binding).map_err(
                    |source| {
                        invalid(Reason::Capacity, "pending observer storage overflow")
                            .with_source(source)
                    },
                )?,
            )
            .map_err(|source| {
                invalid(Reason::Capacity, "pending observer metadata capacity").with_source(source)
            })?;
        let observer = Observer {
            source: services.current_source.clone(),
            caller,
            admitted,
            roles,
            anchor: std::sync::OnceLock::new(),
            _roles_memory: roles_memory,
        };
        let (transport, actual) =
            UpstreamTransport::from_prepaid_opener(binding, observer, opener_memory).map_err(
                |(source, _, _, _)| {
                    invalid(Reason::Capacity, "pending observer storage refused")
                        .with_source(source)
                },
            )?;
        assert!(
            actual
                .actual()
                .anchor
                .set(transport.pending_carrier_observer())
                .is_ok(),
            "one actual pending receiver"
        );
        drop(actual);
        Ok(Some(Self(transport)))
    }
    pub(super) fn observer(&self) -> PendingCarrierObserver {
        self.0.pending_carrier_observer()
    }
}
impl UpstreamOpener for Observer {
    fn protocol_original_observer(&self) -> Option<PendingCarrierObserver> {
        self.anchor.get().cloned()
    }
    fn retain_protocol_original(
        &self,
        binding: &UpstreamBinding,
        original: zero_platform_tokio::controlled_carrier::ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        self.receive_protocol(binding, original)
    }
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
        if self
            .anchor
            .get()
            .is_none_or(|anchor| binding != anchor.binding())
            || self.caller.check_open().is_err()
        {
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
        if self
            .anchor
            .get()
            .is_none_or(|anchor| binding != anchor.binding())
        {
            return Err(invalid(Reason::StalePlan, "pending caller binding changed"));
        }
        self.caller.check_open()?;
        if let Some(admitted) = &self.admitted {
            admitted.check_active().map_err(|source| {
                invalid(Reason::Revoked, "pending caller Engine admission ended")
                    .with_source(source)
            })?;
        }
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
        if let Caller::Probe(call) = &self.caller {
            for role in self.roles.iter() {
                plan.role_slot(*role)
                    .ok_or_else(|| invalid(Reason::StalePlan, "probe selected role retired"))?
                    .check_probe_caller(call)?;
            }
        }
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
        self.caller.retain(adoption)
    }
}
