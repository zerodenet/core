use std::io;
use std::pin::Pin;

use zero_core::{InboundFallbackReplay, InboundRouteAccept};
use zero_engine::EngineError;
use zero_platform_tokio::TcpRelayStream;
use zero_traits::InboundFallbackProfile;

use crate::protocol_registry::TcpRuntimeServices;
use crate::transport::{relay_bidirectional_metered, ClientStream, MeteredStream};

#[derive(Debug, Clone)]
pub(crate) struct InboundFallbackTarget {
    route: zero_traits::FallbackRoute,
}

impl InboundFallbackTarget {
    pub(crate) fn from_profile<T>(profile: &T) -> Self
    where
        T: InboundFallbackProfile + ?Sized,
    {
        Self {
            route: zero_traits::FallbackRoute {
                endpoint: zero_traits::FallbackEndpoint::Tcp {
                    server: profile.server().to_owned(),
                    port: profile.port(),
                },
                proxy_protocol: 0,
                source: None,
                destination: None,
            },
        }
    }
}

pub(crate) struct PreparedInboundFallback<R> {
    pub(crate) target: InboundFallbackTarget,
    pub(crate) replay: R,
}

pub(crate) type PreparedInboundRouteAccept<R, F> =
    InboundRouteAccept<R, PreparedInboundFallback<F>>;

pub(crate) fn prepare_inbound_route_accept<R, F: InboundFallbackReplay>(
    result: InboundRouteAccept<R, F>,
    fallback: Option<InboundFallbackTarget>,
) -> Result<PreparedInboundRouteAccept<R, F>, EngineError> {
    match result {
        InboundRouteAccept::Route(route) => Ok(InboundRouteAccept::Route(route)),
        InboundRouteAccept::Control(control) => Ok(InboundRouteAccept::Control(control)),
        InboundRouteAccept::Fallback(replay) => {
            let target = replay
                .selected_route()
                .map(|route| InboundFallbackTarget { route })
                .or(fallback)
                .ok_or_else(|| {
                    EngineError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "protocol requested fallback without a prepared target",
                    ))
                })?;
            Ok(InboundRouteAccept::Fallback(PreparedInboundFallback {
                target,
                replay,
            }))
        }
    }
}

pub(crate) async fn relay_recorded_fallback<S, FReplay>(
    services: TcpRuntimeServices,
    fallback: InboundFallbackTarget,
    replay_to_upstream: FReplay,
) -> Result<(), EngineError>
where
    S: ClientStream,
    FReplay: for<'a> FnOnce(
        &'a mut TcpRelayStream,
    ) -> Pin<
        Box<dyn core::future::Future<Output = Result<S, std::io::Error>> + Send + 'a>,
    >,
{
    let mut upstream = match &fallback.route.endpoint {
        zero_traits::FallbackEndpoint::Tcp { server, port } => TcpRelayStream::from(
            services
                .connect_upstream_owned(server.clone(), *port)
                .await?,
        ),
        zero_traits::FallbackEndpoint::Unix { path } => {
            #[cfg(unix)]
            {
                TcpRelayStream::new(tokio::net::UnixStream::connect(path).await?)
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                return Err(EngineError::Io(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Unix fallback is unavailable on this platform",
                )));
            }
        }
    };
    let prefix = zero_transport::proxy_protocol::encode(
        fallback.route.proxy_protocol,
        fallback.route.source,
        fallback.route.destination,
    )?;
    zero_traits::AsyncSocket::write_all(&mut upstream, &prefix).await?;

    let client_stream = replay_to_upstream(&mut upstream).await?;

    let metered_client = MeteredStream::new(client_stream);
    let metered_upstream = MeteredStream::new(upstream);
    match relay_bidirectional_metered(metered_client, metered_upstream, |_| {}, |_| {}).await {
        Ok(_) => Ok(()),
        Err(error)
            if error.kind() == io::ErrorKind::NotConnected
                || error.kind() == io::ErrorKind::BrokenPipe =>
        {
            Ok(())
        }
        Err(error) => Err(EngineError::Io(error)),
    }
}

pub(crate) async fn relay_recorded_fallback_replay<R>(
    services: TcpRuntimeServices,
    fallback: InboundFallbackTarget,
    replay: R,
) -> Result<(), EngineError>
where
    R: InboundFallbackReplay + 'static,
    R::Stream: ClientStream,
{
    relay_recorded_fallback(services, fallback, move |upstream| {
        Box::pin(async move { replay.replay_to(upstream).await })
    })
    .await
}
