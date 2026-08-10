use std::future::Future;
use std::io;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use zero_engine::EngineError;

use crate::runtime::route_runtime::{InboundRouteRuntime, InboundRouteRuntimeFactory};

pub(crate) struct QuicListenerLoopRequest<H> {
    pub(crate) runtime_factory: InboundRouteRuntimeFactory,
    pub(crate) protocol_name: &'static str,
    pub(crate) listener: crate::transport::QuicInbound,
    pub(crate) shutdown: watch::Receiver<bool>,
    pub(crate) handler: H,
}

pub(crate) async fn run_quic_listener_loop<H, Fut>(
    request: QuicListenerLoopRequest<H>,
) -> Result<(), EngineError>
where
    H: Fn(InboundRouteRuntime, quinn::Connection) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let QuicListenerLoopRequest {
        runtime_factory,
        protocol_name,
        listener,
        mut shutdown,
        handler,
    } = request;
    let mut connections = JoinSet::new();

    info!(
        inbound_tag = %runtime_factory.inbound_tag(),
        protocol = protocol_name,
        transport = "quic",
        "inbound listener ready"
    );

    let stop_reason = loop {
        tokio::select! {
            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if *shutdown.borrow() => break "shutdown_signal",
                    Ok(()) => {}
                    Err(error) => {
                        warn!(
                            inbound_tag = %runtime_factory.inbound_tag(),
                            protocol = protocol_name,
                            transport = "quic",
                            reason = "shutdown_channel_closed",
                            error = %error,
                            "inbound listener shutdown channel closed"
                        );
                        break "shutdown_channel_closed";
                    }
                }
            }
            incoming = listener.accept_incoming() => {
                let Some(incoming) = incoming else {
                    error!(
                        inbound_tag = %runtime_factory.inbound_tag(),
                        protocol = protocol_name,
                        transport = "quic",
                        reason = "listener_endpoint_closed",
                        "inbound listener endpoint closed unexpectedly"
                    );
                    connections.abort_all();
                    while connections.join_next().await.is_some() {}
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        format!(
                            "{protocol_name} QUIC inbound endpoint closed for {}",
                            runtime_factory.inbound_tag()
                        ),
                    )
                    .into());
                };
                let remote_address = incoming.remote_address();
                let runtime = runtime_factory.for_connection(None);
                let handler = handler.clone();
                let inbound_tag = runtime_factory.inbound_tag().to_owned();
                connections.spawn(async move {
                    match crate::transport::QuicInbound::establish_incoming(incoming).await {
                        Ok(connection) => handler(runtime, connection).await,
                        Err(connection_error) => error!(
                            inbound_tag = %inbound_tag,
                            protocol = protocol_name,
                            transport = "quic",
                            remote_address = %remote_address,
                            reason = "connection_accept_error",
                            error = %connection_error,
                            "inbound QUIC connection handshake failed"
                        ),
                    }
                });
            }
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result {
                    if !error.is_cancelled() {
                        error!(
                            inbound_tag = %runtime_factory.inbound_tag(),
                            error = %error,
                            protocol = protocol_name,
                            transport = "quic",
                            reason = "connection_task_panic",
                            "inbound connection task panicked"
                        );
                    }
                }
            }
        }
    };

    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            if !error.is_cancelled() {
                error!(
                    inbound_tag = %runtime_factory.inbound_tag(),
                    error = %error,
                    protocol = protocol_name,
                    transport = "quic",
                    reason = "connection_task_panic_during_shutdown",
                    "inbound connection task panicked during shutdown"
                );
            }
        }
    }

    info!(
        inbound_tag = %runtime_factory.inbound_tag(),
        protocol = protocol_name,
        transport = "quic",
        reason = stop_reason,
        "inbound listener stopped"
    );
    Ok(())
}
