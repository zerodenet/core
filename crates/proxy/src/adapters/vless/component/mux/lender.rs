//! The existing cached gRPC pool lends its real body to the one MUX driver.
use crate::component::background::{pool, BackgroundCarrier};
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

pub(super) struct Factory {
    pub(super) background: Arc<pool::Factory>,
    pub(super) dependency: ComponentId,
    pub(super) grpc: Arc<NativePoolOptions>,
}
impl ComponentFactory for Factory {
    type Options = pool::Options;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        self.background.describe(options)
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
            initial: options.pool.binding().clone(),
            endpoint: options.pool.endpoint().clone(),
            wire_endpoint: options.wire_endpoint.clone(),
            tag: options.tag.clone(),
        });
        let base = UpstreamTransport::new(options.pool.binding().clone(), provider.clone());
        context.background_carrier = Some(UpstreamTransport::new(
            base.binding().clone(),
            Arc::new(Root {
                base,
                provider,
                expected,
                grpc: self.grpc.clone(),
            }),
        ));
        zero_transport::pooled_driver::PoolFactory.build(
            Arc::new((*options.pool).clone().with_scoped_preparation()),
            context,
        )
    }
}
struct Root {
    base: UpstreamTransport,
    provider: Arc<BackgroundCarrier>,
    expected: ComponentInstanceId,
    grpc: Arc<NativePoolOptions>,
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
                    grpc: self.grpc.clone(),
                    waiting,
                    call,
                }),
            ))
        })
    }
}
impl UpstreamOpener for Root {
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
    grpc: Arc<NativePoolOptions>,
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
                || endpoint != *self.grpc.endpoint()
                || call.session.is_some()
                || call.deadline != self.call.deadline
                || call.cancel.is_cancelled()
                || self.call.cancel.is_cancelled()
            {
                return Err(invalid());
            }
            let stream = self
                .grpc
                .open_observed(
                    self.expected,
                    Demand {
                        binding,
                        deadline: call.deadline,
                        cancel: call.cancel,
                    },
                    self.waiting.clone(),
                )
                .await?;
            stream.into_stream_loan()
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
