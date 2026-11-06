//! Inline per-connection donor setup executes in the same real admitted task.
//! Cached transport setup still uses its own original Aux through service_transport.
use super::*;
use zero_platform_tokio::contract::network::PendingCarrierObserver;
struct InlineCaller {
    original: PacketOpener,
    call: CarrierContext,
    waiting: Option<PendingCarrierObserver>,
}
impl InlineCaller {
    fn check(
        &self,
        binding: &UpstreamBinding,
        call: &CarrierContext,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        if binding != &self.original.binding
            || call.session.is_some()
            || call.deadline != self.call.deadline
            || call.cancel.is_cancelled()
            || self.call.cancel.is_cancelled()
        {
            return Err(invalid(
                Reason::StalePlan,
                "inline Packet Service invocation changed",
            ));
        }
        self.original.packet_caller(
            binding,
            &self.original.invocation.identity,
            &CarrierContext {
                session: self.original.invocation.identity.session(),
                deadline: self.original.invocation.deadline,
                cancel: self.original.invocation.cancellation(),
            },
        )
    }
}
impl PacketOpener {
    pub(super) fn select_inline_service(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        Box::pin(async move {
            let binding = binding.ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "inline Packet Service requires its selected binding",
                )
            })?;
            if binding != self.binding || !binding.prefix.is_empty() || call.session.is_some() {
                return Err(invalid(
                    Reason::StalePlan,
                    "inline Packet Service changed its actual root",
                ));
            }
            let inline = Arc::new(InlineCaller {
                original: self.clone(),
                call: call.clone(),
                waiting,
            });
            inline.check(&binding, &call)?;
            let caller = UpstreamTransport::new(binding.clone(), inline);
            let pending = caller.pending_carrier_observer();
            Ok(UpstreamTransport::new(
                binding.clone(),
                Arc::new(ServicePacketOpener {
                    services: self.services.clone(),
                    binding,
                    call,
                    pending: Some(pending),
                    _caller_owner: Some(caller),
                }),
            ))
        })
    }
}
impl UpstreamOpener for InlineCaller {
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &RequestIdentity,
        call: &CarrierContext,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        if !matches!(identity,RequestIdentity::Service(facts) if facts.origin==zero_core::request::ServiceOrigin::Runtime && facts.purpose==zero_core::request::ServicePurpose::BackgroundControl)
        {
            return Err(invalid(
                Reason::StalePlan,
                "inline caller is not the actual per-connection Service projection",
            ));
        }
        self.check(binding, call)
    }
    fn has_pending_carrier_observer(&self) -> bool {
        self.waiting.is_some()
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        self.check(binding, &self.call).ok()?;
        self.waiting.as_ref()?.capacity()
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.check(binding, &self.call)?;
        self.waiting
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "inline caller has no foreign-original ledger",
                )
            })?
            .retain(adoption)
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::contract::stream::BoxStream, ChannelError>> {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "caller projection grants no native Stream",
            ))
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
            Err(invalid(
                Reason::Unsupported,
                "caller projection grants no native Datagram",
            ))
        })
    }
}
