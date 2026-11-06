//! Same-slot taskless opener; native and logical completion stay separate.
use super::*;
pub(super) struct Opener {
    pub(super) upstream: UpstreamTransport,
    pub(super) endpoint: Destination,
    pub(super) process: Option<zero_core::component::ComponentInstanceId>,
    pub(super) transfer: Arc<Transfer>,
}
impl UpstreamOpener for Opener {
    fn has_pending_carrier_observer(&self) -> bool {
        self.upstream.observed_pending_carrier().is_some()
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        if binding != self.upstream.binding() {
            return None;
        }
        self.upstream.pending_carrier_capacity()
    }
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        if binding != self.upstream.binding() {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "Packet caller binding changed",
            ));
        }
        self.upstream.packet_caller(identity, call)
    }
    fn resolve_packet_target(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        identity: zero_platform_tokio::contract::context::RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<std::net::SocketAddr>, ChannelError>> {
        if binding != self.upstream.binding() {
            return Box::pin(async {
                Err(crate::component::instance::invalid(
                    Reason::StalePlan,
                    "packet target binding changed",
                ))
            });
        }
        self.upstream
            .resolve_packet_target(destination, identity, call)
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if binding != self.upstream.binding() {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "nested pending original binding changed",
            ));
        }
        self.upstream.retain_pending_carrier(adoption)
    }
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        mut call: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            let mut original = self.transfer.owner.lock().await;
            if &binding != self.upstream.binding()
                || endpoint != self.endpoint
                || call.session.is_some() != self.transfer.caller.is_some()
                || original.is_some()
            {
                return Err(crate::component::instance::invalid(
                    Reason::StalePlan,
                    "protocol requested a foreign or duplicate carrier",
                ));
            }
            if self.transfer.caller.is_none() {
                call.cancel = self.transfer.stop.clone();
            }
            let grant = self.transfer.grant.lock().unwrap().take().ok_or_else(|| {
                crate::component::instance::invalid(
                    Reason::StalePlan,
                    "protocol already consumed its actual carrier grant",
                )
            })?;
            let business = self.transfer.caller.is_some();
            let view = self.transfer.view.lock().unwrap().take().ok_or_else(|| {
                crate::component::instance::invalid(
                    Reason::StalePlan,
                    "original unique taskless view already transferred",
                )
            })?;
            let io = if let Some(process) = self.process {
                self.upstream
                    .acquire_process_endpoint_stream(
                        zero_platform_tokio::contract::network::ProcessEndpointStreamRequest {
                            process,
                            process_ordinal: 0,
                            endpoint,
                            context: call,
                            grant,
                        },
                    )
                    .await?
            } else if business || binding.prefix.is_empty() {
                self.upstream
                    .acquire_controlled_stream(endpoint, call, grant)
                    .await?
            } else {
                let loan = self
                    .upstream
                    .borrow_stream(endpoint.clone(), call.clone())
                    .await?;
                grant.adopt_stream_loan(&binding, endpoint, &call, loan)?
            };
            if business {
                // Native permission has bound this exact admitted caller to the
                // slot. Keep its real IO and unique view; no transferred-origin
                // or Service loan is manufactured for a Business connection.
                return Ok(Box::pin(super::business::BusinessIo::new(io, view)) as BoxStream);
            }
            let loan = io.into_stream_loan(view)?;
            let StreamLoan { io, owner } = loan;
            // Keep the original control in the persistent actual owner before
            // returning the view to protocol TLS/framing/handshake code.
            *original = Some(owner);
            Ok(Box::pin(io) as BoxStream)
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
            Err(crate::component::instance::invalid(
                Reason::Unsupported,
                "a Stream carrier has no unrelated Datagram authority",
            ))
        })
    }
    fn carrier_dns_generation(&self) -> Option<zero_platform_tokio::contract::dns::DnsGeneration> {
        self.upstream.carrier_dns_generation()
    }
}
