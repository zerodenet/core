//! An already selected route owns the original capture until one replay.
use zero_core::InboundFallbackReplay;
use zero_platform_tokio::{
    contract::{
        inbound::{AcceptedFallback, FallbackStreams},
        stream::BoxStream,
        BoxFuture, ChannelError, Impact, Reason, Stage,
    },
    TcpRelayStream,
};
pub(crate) fn accepted<R>(replay: R) -> Result<Box<dyn AcceptedFallback>, ChannelError>
where
    R: InboundFallbackReplay + Send + 'static,
    R::Stream: zero_platform_tokio::ClientStream + 'static,
{
    let route = replay.selected_route().ok_or_else(|| {
        ChannelError::new(Stage::Handshake, Reason::InvalidConfig, Impact::Session)
    })?;
    Ok(Box::new(Replay { route, replay }))
}
struct Replay<R> {
    route: zero_traits::FallbackRoute,
    replay: R,
}
impl<R> AcceptedFallback for Replay<R>
where
    R: InboundFallbackReplay + Send + 'static,
    R::Stream: zero_platform_tokio::ClientStream + 'static,
{
    fn route(&self) -> &zero_traits::FallbackRoute {
        &self.route
    }
    fn replay(
        self: Box<Self>,
        upstream: BoxStream,
    ) -> BoxFuture<'static, Result<FallbackStreams, ChannelError>> {
        Box::pin(async move {
            let mut upstream =
                TcpRelayStream::new(crate::component::carrier::Carrier::new(upstream));
            let inbound = self
                .replay
                .replay_to(&mut upstream)
                .await
                .map_err(super::datagram::failure)?;
            Ok(FallbackStreams {
                inbound: Box::pin(inbound),
                upstream: Box::pin(upstream),
            })
        })
    }
}
