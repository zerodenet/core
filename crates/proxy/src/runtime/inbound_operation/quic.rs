use std::pin::Pin;
use std::sync::Arc;

use zero_core::{InboundClientResponse, InboundDatagramUdpRelay, Session};
use zero_engine::EngineError;
use zero_traits::AsyncSocket;

use super::{InboundConnectionContext, PreparedInboundListenerOperation};
use crate::protocol_registry::BoundInbound;
use crate::runtime::route_runtime::{InboundListenerRuntime, InboundRouteRuntime};

#[async_trait::async_trait]
pub(crate) trait AuthenticatedQuicInboundProfile: Clone + Send + Sync + 'static {
    type Connection: AuthenticatedQuicInboundConnection;

    async fn accept_authenticated_connection(
        &self,
        connection: quinn::Connection,
    ) -> Result<Self::Connection, EngineError>;
}

#[async_trait::async_trait]
pub(crate) trait AuthenticatedQuicInboundConnection: Send + Sync + 'static {
    type Stream: AsyncSocket
        + tokio::io::AsyncRead
        + tokio::io::AsyncWrite
        + Unpin
        + Send
        + Sync
        + 'static;
    type ResponseProtocol: InboundClientResponse<Self::Stream> + Send + Sync + Copy + 'static;
    type UdpRelay: InboundDatagramUdpRelay<Arc<quinn::Connection>> + Send + 'static;

    fn auth(&self) -> Option<&zero_core::SessionAuth>;
    fn close(&self, reason: &str);

    fn datagram_source(&self) -> Arc<quinn::Connection>;
    fn udp_relay(&self) -> Self::UdpRelay;
    fn response_protocol(&self) -> Self::ResponseProtocol;

    async fn accept_next_tcp_stream(&self) -> Result<Option<(Session, Self::Stream)>, EngineError>;
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
                BoundInbound::Quic(listener) => listener,
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
    let device_registration = match runtime.acquire_principal_device(connection.auth()) {
        Ok(registration) => registration,
        Err(error) => {
            connection.close("device_limit");
            return Err(error);
        }
    };
    let (principal_cancel_tx, mut principal_cancel_rx) =
        tokio::sync::mpsc::unbounded_channel::<String>();
    let principal_registration = connection
        .auth()
        .and_then(|auth| auth.principal_key.as_deref())
        .map(|principal_key| {
            runtime.register_principal_cancellation(principal_key, move |reason| {
                let _ = principal_cancel_tx.send(reason);
            })
        });
    let mut tasks = tokio::task::JoinSet::new();
    let udp_source = connection.datagram_source();
    let udp_relay = connection.udp_relay();
    let udp_runtime = runtime.udp_runtime();
    let udp_tag = runtime.inbound_tag().to_owned();
    tasks.spawn(async move {
        crate::runtime::datagram_udp::run_protocol_datagram_udp_relay(
            udp_runtime,
            udp_source,
            udp_relay,
            &udp_tag,
            false,
        )
        .await
    });

    loop {
        tokio::select! {
            accepted = connection.accept_next_tcp_stream() => {
                let Some((session, stream)) = accepted? else {
                    break;
                };
                let context = InboundConnectionContext::new(runtime.clone());
                let response = connection.response_protocol();
                tasks.spawn(async move {
                    context.serve_with_client_response(session, stream, response).await
                });
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => tracing::warn!(%error, "inbound QUIC stream task failed"),
                    Some(Err(error)) if !error.is_cancelled() => {
                        tracing::error!(%error, "inbound QUIC stream task panicked");
                    }
                    Some(Err(_)) | None => {}
                }
            }
            Some(reason) = principal_cancel_rx.recv() => {
                connection.close(&reason);
                break;
            }
        }
    }

    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    drop(principal_registration);
    drop(device_registration);
    Ok(())
}
