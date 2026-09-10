use std::pin::Pin;

use zero_core::InboundDatagramMultiplexer;
use zero_engine::EngineError;

use super::PreparedInboundListenerOperation;
use crate::protocol_registry::BoundInbound;
use crate::runtime::route_runtime::{InboundListenerRuntime, InboundRouteRuntime};

#[async_trait::async_trait]
pub(crate) trait AuthenticatedQuicInboundProfile: Clone + Send + Sync + 'static {
    type Connection: InboundDatagramMultiplexer<
        Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite,
        Error: Into<EngineError>,
    >;

    async fn accept_authenticated_connection(
        &self,
        connection: quinn::Connection,
    ) -> Result<Self::Connection, EngineError>;
}

pub(crate) struct AuthenticatedQuicInboundListenerOperation<P> {
    pub(crate) protocol_name: &'static str,
    pub(crate) profile: P,
}

impl<P> PreparedInboundListenerOperation for AuthenticatedQuicInboundListenerOperation<P>
where
    P: AuthenticatedQuicInboundProfile,
{
    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), EngineError>> + Send + 'static>> {
        Box::pin(async move {
            let listener = match bound {
                #[cfg(feature = "datagram-route-runtime")]
                BoundInbound::Datagram(_) => {
                    return Err(EngineError::Io(std::io::Error::other(
                        "unexpected datagram listener",
                    )))
                }
                BoundInbound::Quic(listener) => listener,
                #[cfg(feature = "inbound-listener-group-runtime")]
                BoundInbound::Group(_) => {
                    return Err(EngineError::Io(std::io::Error::other(
                        "nested listener group mismatch",
                    )))
                }
                #[cfg(feature = "managed-datagram-runtime")]
                BoundInbound::TcpAndDatagram(..) => {
                    return Err(EngineError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "QUIC operation received TCP/datagram listener",
                    )))
                }
                BoundInbound::Tcp(_) => {
                    return Err(EngineError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "authenticated QUIC inbound received a TCP listener",
                    )))
                }
            };
            let profile = self.profile;
            let protocol_name = self.protocol_name;
            crate::runtime::listener_loop::run_quic_listener_loop(
                crate::runtime::listener_loop::QuicListenerLoopRequest {
                    runtime_factory: runtime.route_factory(),
                    protocol_name,
                    listener,
                    shutdown,
                    handler: move |runtime, connection| {
                        let profile = profile.clone();
                        async move {
                            if let Err(error) = run_authenticated_quic_connection(
                                profile,
                                runtime,
                                connection,
                            )
                            .await
                            {
                                tracing::error!(%error, protocol = protocol_name, "inbound QUIC connection failed");
                            }
                        }
                    },
                },
            )
            .await
        })
    }
}

async fn run_authenticated_quic_connection<P>(
    profile: P,
    runtime: InboundRouteRuntime,
    connection: quinn::Connection,
) -> Result<(), EngineError>
where
    P: AuthenticatedQuicInboundProfile,
{
    let connection = profile.accept_authenticated_connection(connection).await?;
    super::multiplex::run_datagram_multiplexer(runtime, connection).await
}
