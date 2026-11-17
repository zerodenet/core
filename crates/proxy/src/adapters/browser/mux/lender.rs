//! The actual Browser Source lends its original to the existing MUX driver.
use crate::component::background::BackgroundCarrier;
use std::sync::Arc;
use zero_core::{component::ComponentInstanceId, request::Destination};
use zero_platform_tokio::{
    contract::{
        network::{
            CarrierContext, PendingCarrierObserver, UpstreamBinding, UpstreamOpener,
            UpstreamTransport,
        },
        stream::BoxStream,
        BoxFuture, ChannelError, Impact, Reason, Stage,
    },
    controlled_carrier::{loan::StreamLoan, CarrierAdoption},
};
use zero_transport::browser_dialer::component::raw::RawCarrier;

pub(crate) fn transport(
    provider: Arc<BackgroundCarrier>,
    initial: UpstreamBinding,
    expected: ComponentInstanceId,
    source: RawCarrier,
) -> UpstreamTransport {
    let base = UpstreamTransport::new(initial, provider.clone());
    UpstreamTransport::new(
        base.binding().clone(),
        Arc::new(Root {
            base,
            provider,
            expected,
            source,
        }),
    )
}

struct Root {
    base: UpstreamTransport,
    provider: Arc<BackgroundCarrier>,
    expected: ComponentInstanceId,
    source: RawCarrier,
}
impl Root {
    fn select(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        Box::pin(async move {
            let base = match &waiting {
                Some(waiting) => {
                    self.base
                        .select_background_carrier_observed(binding, call.clone(), waiting.clone())
                        .await?
                }
                None => {
                    self.base
                        .select_background_carrier(binding, call.clone())
                        .await?
                }
            };
            Ok(UpstreamTransport::new(
                base.binding().clone(),
                Arc::new(Selected {
                    base,
                    expected: self.expected,
                    source: self.source.clone(),
                    waiting,
                    call,
                }),
            ))
        })
    }
}
impl UpstreamOpener for Root {
    fn validate_pending_protocol_original(
        &self,
        binding: &UpstreamBinding,
        original: &zero_platform_tokio::controlled_carrier::ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        self.base
            .validate_pending_protocol_original(binding, original)
    }

    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        if binding.prefix.is_empty() {
            self.provider
                .declared_pending_capacity_with_dependency(binding, self.expected)
                .ok()
        } else {
            self.base.pending_carrier_capacity_for(binding)
        }
    }
    fn validate_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        original: &CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.base.validate_pending_carrier(binding, original)
    }
    fn carrier_dns_generation(&self) -> Option<zero_platform_tokio::contract::dns::DnsGeneration> {
        self.base.carrier_dns_generation()
    }
    fn select_background_carrier(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        self.select(binding, call, None)
    }
    fn select_background_carrier_observed(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: PendingCarrierObserver,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        self.select(binding, call, Some(waiting))
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { Err(invalid()) })
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
        Box::pin(async { Err(invalid()) })
    }
}
struct Selected {
    base: UpstreamTransport,
    expected: ComponentInstanceId,
    source: RawCarrier,
    waiting: Option<PendingCarrierObserver>,
    call: CarrierContext,
}
impl UpstreamOpener for Selected {
    fn borrow_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<StreamLoan, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if binding != *self.base.binding()
                || endpoint != *self.source.endpoint()
                || call.session.is_some()
                || call.deadline != self.call.deadline
                || call.cancel.is_cancelled()
                || self.call.cancel.is_cancelled()
            {
                return Err(invalid());
            }
            if binding.prefix.is_empty() {
                self.source
                    .open_carrier(self.expected, binding, call, self.waiting.clone())
                    .await
            } else {
                // A browser cannot dial through a preceding proxy. Preserve
                // that real prefix and let the protocol apply its native WS/TLS
                // profile to this exact original, in the same owning pool.
                self.base.borrow_stream(endpoint, call).await
            }
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { Err(invalid()) })
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
        Box::pin(async { Err(invalid()) })
    }
}
fn invalid() -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component)
}
