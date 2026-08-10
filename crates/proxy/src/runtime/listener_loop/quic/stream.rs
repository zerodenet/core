use std::future::Future;
use std::io;

use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use zero_engine::EngineError;

use crate::runtime::route_runtime::{InboundRouteRuntime, InboundRouteRuntimeFactory};

pub(crate) struct QuicStreamListenerLoopRequest<H> {
    pub(crate) runtime_factory: InboundRouteRuntimeFactory,
    pub(crate) protocol_name: &'static str,
    pub(crate) listener: crate::transport::QuicInbound,
    pub(crate) shutdown: watch::Receiver<bool>,
    pub(crate) handler: H,
}

pub(crate) async fn run_quic_stream_listener_loop<H, Fut>(
    request: QuicStreamListenerLoopRequest<H>,
) -> Result<(), EngineError>
where
    H: Fn(InboundRouteRuntime, crate::transport::QuicStream) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let QuicStreamListenerLoopRequest {
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
            accept_result = listener.accept() => {
                match accept_result {
                    Ok(stream) => {
                        let runtime = runtime_factory.for_connection(None);
                        let handler = handler.clone();
                        connections.spawn(handler(runtime, stream));
                    }
                    Err(error) => {
                        error!(
                            inbound_tag = %runtime_factory.inbound_tag(),
                            error = %error,
                            protocol = protocol_name,
                            transport = "quic",
                            reason = "accept_error",
                            "inbound accept error"
                        );
                        connections.abort_all();
                        while let Some(result) = connections.join_next().await {
                            if let Err(join_error) = result {
                                if !join_error.is_cancelled() {
                                    error!(
                                        inbound_tag = %runtime_factory.inbound_tag(),
                                        error = %join_error,
                                        protocol = protocol_name,
                                        transport = "quic",
                                        reason = "connection_task_panic_during_failed_listener_cleanup",
                                        "inbound connection task panicked during failed listener cleanup"
                                    );
                                }
                            }
                        }
                        return Err(io::Error::other(format!(
                            "{protocol_name} QUIC inbound accept failed for {}: {error}",
                            runtime_factory.inbound_tag()
                        ))
                        .into());
                    }
                }
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
