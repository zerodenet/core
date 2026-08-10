use std::future::Future;

use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use zero_stack::SystemTcpStack;
use zero_traits::TcpStack;

use crate::runtime::route_runtime::{InboundRouteRuntime, InboundRouteRuntimeFactory};

pub(crate) struct SystemTcpStackLoopRequest<H> {
    pub(crate) runtime_factory: InboundRouteRuntimeFactory,
    pub(crate) stack: SystemTcpStack,
    pub(crate) shutdown: watch::Receiver<bool>,
    pub(crate) handler: H,
}

pub(crate) async fn run_system_tcp_stack_loop<H, Fut>(request: SystemTcpStackLoopRequest<H>)
where
    H: Fn(InboundRouteRuntime, TcpStream, zero_traits::SocketAddress) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let SystemTcpStackLoopRequest {
        runtime_factory,
        stack,
        mut shutdown,
        handler,
    } = request;
    let mut connections = JoinSet::new();

    let stop_reason = loop {
        tokio::select! {
            biased;

            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if *shutdown.borrow() => {
                        info!(
                            inbound_tag = %runtime_factory.inbound_tag(),
                            reason = "shutdown_signal",
                            "system inbound shutdown"
                        );
                        break "shutdown_signal";
                    }
                    Ok(()) => {}
                    Err(error) => {
                        warn!(
                            inbound_tag = %runtime_factory.inbound_tag(),
                            reason = "shutdown_channel_closed",
                            error = %error,
                            "system inbound shutdown channel closed"
                        );
                        break "shutdown_channel_closed";
                    }
                }
            }

            accepted = stack.accept() => {
                match accepted {
                    Some((stream, source, destination)) => {
                        let runtime = runtime_factory.for_connection(
                            Some(zero_platform_tokio::socket_address_to_socket_addr(source)),
                        );
                        let handler = handler.clone();
                        connections.spawn(handler(runtime, stream, destination));
                    }
                    None => {
                        warn!(
                            inbound_tag = %runtime_factory.inbound_tag(),
                            reason = "accept_source_closed",
                            "system inbound accept source closed"
                        );
                        break "accept_source_closed";
                    }
                }
            }

            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result {
                    if !error.is_cancelled() {
                        error!(
                            inbound_tag = %runtime_factory.inbound_tag(),
                            reason = "connection_task_panic",
                            error = %error,
                            "system connection task panicked"
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
                    reason = "connection_task_panic_during_shutdown",
                    error = %error,
                    "system connection task panicked during shutdown"
                );
            }
        }
    }

    info!(
        inbound_tag = %runtime_factory.inbound_tag(),
        reason = stop_reason,
        "system inbound stopped"
    );
}
