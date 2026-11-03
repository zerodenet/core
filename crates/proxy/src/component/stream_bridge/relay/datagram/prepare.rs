//! Inline protocol handshakes over the one real native body; no detached worker.
use super::*;
use crate::protocol_registry::UpstreamConnectServices;
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, UpstreamOpener},
        stream::{BoxStream, ConnectRequest, DynStreamOutbound},
        BoxFuture, ChannelError, Reason,
    },
    controlled_carrier::CarrierStreamPreparation,
};
#[derive(Clone)]
pub(super) struct Hop {
    pub role: Arc<dyn DynStreamOutbound>,
    pub health: Option<Arc<crate::component::stream_bridge::relay::health::FirstHopHealth>>,
    pub observed: Arc<std::sync::atomic::AtomicBool>,
    pub endpoint: Destination,
    pub target: Destination,
    pub binding: UpstreamBinding,
    pub observer: Option<Arc<dyn zero_traits::IoObserver>>,
}
pub(super) fn preparation(
    hops: Vec<Hop>,
    identity: zero_platform_tokio::contract::context::RequestIdentity,
    call: CarrierContext,
    network: UpstreamConnectServices,
) -> CarrierStreamPreparation {
    Arc::new(move |mut body| {
        let (hops, identity, call, network) = (
            hops.clone(),
            identity.clone(),
            call.clone(),
            network.clone(),
        );
        Box::pin(async move {
            for hop in hops {
                if network.network_generation() != hop.binding.network_generation.0
                    || call.cancel.is_cancelled()
                {
                    return Err(invalid(
                        Reason::StalePlan,
                        "relay changed before prefix handshake",
                    ));
                }
                let context = OpenContext {
                    identity: identity.clone(),
                    deadline: call.deadline,
                    cancel: call.cancel.clone(),
                    observer: None,
                    upstream: UpstreamTransport::new(
                        hop.binding.clone(),
                        Arc::new(Body {
                            io: tokio::sync::Mutex::new(Some(body)),
                            endpoint: hop.endpoint,
                            binding: hop.binding,
                            session: call.session,
                            network: network.clone(),
                        }),
                    ),
                };
                let attempt = hop
                    .role
                    .connect(
                        ConnectRequest {
                            destination: hop.target,
                        },
                        context,
                    )
                    .await;
                if let Some(health) = &hop.health {
                    hop.observed
                        .store(true, std::sync::atomic::Ordering::Release);
                    health.observe(&attempt);
                }
                let connected = attempt.map_err(|mut error| {
                    error.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
                    error.retry = zero_platform_tokio::contract::RetryClass::Never;
                    error
                })?;
                body = if let Some(observer) = hop.observer {
                    Box::pin(zero_transport::observed::ObservedStream::new(
                        connected.io,
                        observer,
                    )) as BoxStream
                } else {
                    connected.io
                };
            }
            Ok(body)
        })
    })
}
struct Body {
    io: tokio::sync::Mutex<Option<BoxStream>>,
    endpoint: Destination,
    binding: UpstreamBinding,
    session: Option<zero_core::component::SessionId>,
    network: UpstreamConnectServices,
}
impl UpstreamOpener for Body {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let valid = binding == &self.binding
            && destination == self.endpoint
            && call.session == self.session
            && self.network.network_generation() == binding.network_generation.0;
        Box::pin(async move {
            if !valid || call.cancel.is_cancelled() {
                return Err(invalid(
                    Reason::StalePlan,
                    "relay body cannot bypass its selected hop",
                ));
            }
            self.io
                .lock()
                .await
                .take()
                .ok_or_else(|| invalid(Reason::Capacity, "relay body already consumed"))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "stream prefix grants no datagram socket",
            ))
        })
    }
}
