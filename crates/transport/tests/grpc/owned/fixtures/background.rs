//! Exact root-only library provider; the underlying original still grants I/O.
use super::*;
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, PendingCarrierObserver, UpstreamOpener},
    stream::BoxStream,
};
pub(super) struct BackgroundRoot(pub(super) UpstreamTransport);
impl BackgroundRoot {
    fn select(
        &self,
        selected: Option<UpstreamBinding>,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        let result = if selected.as_ref() == Some(self.0.binding()) {
            Ok(self.0.clone())
        } else {
            Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ))
        };
        Box::pin(async move { result })
    }
}
impl UpstreamOpener for BackgroundRoot {
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        // No foreign prefix exists in this actual fixed root declaration.
        (binding == self.0.binding() && binding.prefix.is_empty()).then_some(0)
    }
    fn select_background_carrier(
        &self,
        selected: Option<UpstreamBinding>,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        self.select(selected)
    }
    fn select_background_carrier_observed(
        &self,
        selected: Option<UpstreamBinding>,
        _: CarrierContext,
        _: PendingCarrierObserver,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        self.select(selected)
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async {
            panic!("background original must use the actual controlled native grant")
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async { panic!("H2 original must not dial a bare Datagram") })
    }
}
