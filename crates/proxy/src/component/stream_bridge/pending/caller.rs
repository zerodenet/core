//! Delegate only to this actual invocation's original finite owner.
use super::*;
#[derive(Clone)]
pub(super) enum Caller {
    Accepted(AcceptedCaller),
    Probe(zero_runtime::ProbeCall),
    #[cfg(feature = "raw-ip-runtime")]
    Raw(crate::runtime::inbound_operation::raw_ip::native_obligations::NativeRegistration),
}
impl Caller {
    pub(super) fn check_open(&self) -> Result<(), ChannelError> {
        match self {
            Self::Accepted(caller) => caller.check_open(),
            Self::Probe(caller) => caller.check_open(),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Raw(caller) => caller.check_open(),
        }
    }
    pub(super) fn retain(&self, adoption: CarrierAdoption) -> Result<(), ChannelError> {
        match self {
            Self::Accepted(caller) => caller.observe_pending_carrier(adoption),
            Self::Probe(caller) => caller.observe_pending_carrier(adoption),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Raw(caller) => caller.observe_pending_carrier(adoption),
        }
    }
}

impl Caller {
    pub(super) fn protocol_metadata(
        &self,
    ) -> Result<zero_platform_tokio::contract::component::PacketDriverMetadataView, ChannelError>
    {
        match self {
            Self::Accepted(caller) => caller.protocol_metadata().map(|(memory, _)| memory),
            Self::Probe(caller) => caller.protocol_metadata(),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Raw(caller) => caller.protocol_metadata(),
        }
    }
    pub(super) fn retain_protocol(
        &self,
        original: zero_platform_tokio::controlled_carrier::ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        match self {
            Self::Accepted(caller) => caller.observe_protocol_original(original),
            Self::Probe(caller) => caller.observe_protocol_original(original),
            #[cfg(feature = "raw-ip-runtime")]
            Self::Raw(caller) => caller.observe_protocol_original(original),
        }
    }
}

/// Metadata and the actual already-registered invocation only. No future/task,
/// caller Scope, native grant or new Root is created by this projection.
pub(crate) fn actual_protocol_caller(
    services: &TcpExecutionServices,
) -> Result<
    Option<(
        zero_platform_tokio::contract::component::PacketDriverMetadataView,
        zero_platform_tokio::contract::scope::TaskJoinWitness,
    )>,
    ChannelError,
> {
    let Some(invocation) = &services.invocation else {
        return Ok(None);
    };
    let caller = invocation.caller_accepted.clone().map(Caller::Accepted);
    #[cfg(feature = "raw-ip-runtime")]
    let caller = caller.or_else(|| invocation.caller_native.clone().map(Caller::Raw));
    let caller = caller
        .or_else(|| invocation.caller_probe.clone().map(Caller::Probe))
        .ok_or_else(|| {
            invalid(
                Reason::StalePlan,
                "protocol callback lacks actual invocation caller",
            )
        })?;
    caller.check_open()?;
    match (&caller, &invocation.identity) {
        (
            Caller::Probe(call),
            zero_platform_tokio::contract::context::RequestIdentity::Service(facts),
        ) if facts.matches_shared(&call.identity())
            && invocation.deadline == Some(call.deadline()) => {}
        (Caller::Probe(_), _) => {
            return Err(invalid(Reason::StalePlan, "protocol probe facts changed"))
        }
        (_, zero_platform_tokio::contract::context::RequestIdentity::Business(facts)) => {
            invocation
                .admitted
                .as_ref()
                .ok_or_else(|| invalid(Reason::Revoked, "protocol caller has no admission"))?
                .check_live(services.engine(), facts.session)
                .map_err(|source| {
                    invalid(Reason::Revoked, "protocol invocation admission ended")
                        .with_source(source)
                })?;
        }
        _ => {
            return Err(invalid(
                Reason::StalePlan,
                "protocol invocation identity changed",
            ))
        }
    }
    let witness = match &caller {
        Caller::Accepted(actual) => actual.task_join_witness(),
        Caller::Probe(actual) => actual.witness(),
        #[cfg(feature = "raw-ip-runtime")]
        Caller::Raw(actual) => Some(actual.current_protocol_caller()?),
    }
    .ok_or_else(|| invalid(Reason::Revoked, "protocol caller registration is absent"))?;
    if witness.is_joined()
        || !witness
            .native_task_id()
            .is_some_and(|id| Some(id) == tokio::task::try_id())
    {
        return Err(invalid(
            Reason::StalePlan,
            "protocol callback belongs to another real task",
        ));
    }
    Ok(Some((caller.protocol_metadata()?, witness)))
}
