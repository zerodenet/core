//! The existing cached gRPC pool lends its real body to the one MUX driver.
use crate::component::background::{pool as background_pool, BackgroundCarrier};
use std::sync::Arc;
use zero_core::{
    component::{ComponentId, ComponentInstanceId},
    request::Destination,
};
use zero_platform_tokio::{
    contract::{
        component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
        lifecycle::BuildError,
        network::{
            CarrierContext, PendingCarrierObserver, UpstreamBinding, UpstreamOpener,
            UpstreamTransport,
        },
        stream::BoxStream,
        BoxFuture, ChannelError, Impact, Reason, Stage,
    },
    controlled_carrier::{loan::StreamLoan, CarrierAdoption},
};
use zero_transport::grpc::{Demand, NativePoolOptions};

#[derive(Clone)]
pub(in crate::adapters::vless::component) enum Source {
    Carrier(::vless::component::quic::TransportCarrier),
    Grpc(Arc<NativePoolOptions>),
    Preconnect {
        pool: ::vless::transport::PreconnectPool,
        endpoint: Destination,
    },
}
impl Source {
    fn endpoint(&self) -> &Destination {
        match self {
            Self::Grpc(options) => options.endpoint(),
            Self::Carrier(pool) => pool.endpoint(),
            Self::Preconnect { endpoint, .. } => endpoint,
        }
    }
    async fn open(
        &self,
        expected: ComponentInstanceId,
        binding: UpstreamBinding,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> Result<StreamLoan, ChannelError> {
        match self {
            Self::Carrier(pool) => pool.open_carrier(expected, binding, call, waiting).await,
            Self::Grpc(options) => options
                .open_observed(
                    expected,
                    Demand {
                        binding,
                        deadline: call.deadline,
                        cancel: call.cancel,
                    },
                    waiting,
                )
                .await?
                .into_stream_loan(),
            Self::Preconnect { pool, .. } => {
                pool.open_carrier(expected, &binding, call, waiting).await
            }
        }
    }
}
pub(in crate::adapters::vless::component) fn transport(
    provider: Arc<BackgroundCarrier>,
    initial: UpstreamBinding,
    expected: ComponentInstanceId,
    source: Source,
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

pub(super) struct Factory {
    pub(super) background: Arc<background_pool::Factory>,
    pub(super) dependency: ComponentId,
    pub(super) source: Source,
}
impl ComponentFactory for Factory {
    type Options = super::pool::Options;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        super::pool::Factory {
            background: self.background.clone(),
        }
        .describe(options)
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        let expected = context
            .dependency(self.dependency)
            .filter(|actual| actual.component == self.dependency && *actual != instance)
            .ok_or_else(invalid)?;
        let provider = Arc::new(BackgroundCarrier {
            network: self.background.network.clone(),
            instance,
            initial: options.pool.pool_options().binding().clone(),
            endpoint: options.pool.pool_options().endpoint().clone(),
            wire_endpoint: options.wire_endpoint.clone(),
            tag: options.tag.clone(),
        });
        context.background_carrier = Some(transport(
            provider,
            options.pool.pool_options().binding().clone(),
            expected,
            self.source.clone(),
        ));
        ::vless::component::mux::OwnedPoolFactory.build_with_ech_resolver(
            options.pool.clone(),
            context,
            crate::component::background::grpc::tls_resolver(
                self.background.network.clone(),
                instance,
            ),
        )
    }
}
struct Root {
    base: UpstreamTransport,
    provider: Arc<BackgroundCarrier>,
    expected: ComponentInstanceId,
    source: Source,
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
        self.provider
            .declared_pending_capacity_with_dependency(binding, self.expected)
            .ok()
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
    source: Source,
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
            self.source
                .open(self.expected, binding, call, self.waiting.clone())
                .await
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
