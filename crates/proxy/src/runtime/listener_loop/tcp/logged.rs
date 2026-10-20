use std::future::Future;

use zero_engine::EngineError;

use crate::runtime::route_runtime::InboundRouteRuntime;

use super::connection::{run_tcp_listener_loop, TcpListenerLoopRequest};

pub(crate) struct LoggedTcpSocketListenerRequest<R, D> {
    pub(crate) runtime_factory: crate::runtime::route_runtime::InboundRouteRuntimeFactory,
    pub(crate) protocol_name: &'static str,
    pub(crate) error_protocol_name: &'static str,
    pub(crate) request: R,
    pub(crate) listener: zero_platform_tokio::TokioListener,
    pub(crate) shutdown: tokio::sync::watch::Receiver<bool>,
    pub(crate) dispatch: D,
}

pub(crate) async fn run_logged_tcp_socket_listener_loop<R, D, Fut>(
    request: LoggedTcpSocketListenerRequest<R, D>,
) -> Result<(), EngineError>
where
    R: Clone + Send + Sync + 'static,
    D: Fn(InboundRouteRuntime, R, zero_platform_tokio::TokioSocket) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
{
    let LoggedTcpSocketListenerRequest {
        runtime_factory,
        protocol_name,
        error_protocol_name,
        request,
        listener,
        shutdown,
        dispatch,
    } = request;

    let owned = listener.accepted_task_receiver().is_some();
    let handler = move |runtime: InboundRouteRuntime, stream: zero_platform_tokio::TokioSocket| {
        let request = request.clone();
        let dispatch = dispatch.clone();
        async move {
            let log_tag = runtime.inbound_tag().to_owned();
            let source_addr = runtime.source_addr();
            let result = dispatch(runtime, request, stream).await;
            if let Err(ref error) = result {
                crate::logging::log_listener_connection_error(
                    crate::logging::INBOUND_ACCEPT_ROUTE_STAGE,
                    error_protocol_name,
                    log_tag.as_str(),
                    &source_addr,
                    error,
                );
            }
            result
        }
    };
    if owned {
        // Generic protocol R may create transport/mux users before returning
        // its context. Only actual selected-user migration can clear this gap.
        super::owned::run(super::owned::OwnedLoop {
            request: TcpListenerLoopRequest {
                runtime_factory,
                protocol_name,
                listener,
                shutdown,
                handler,
            },
            users: super::owned::UsersPlan::Unaccounted,
        })
        .await
    } else {
        run_tcp_listener_loop(TcpListenerLoopRequest {
            runtime_factory,
            protocol_name,
            listener,
            shutdown,
            handler: move |runtime, socket| {
                let work = handler(runtime, socket);
                async move {
                    let _ = work.await;
                }
            },
        })
        .await
    }
}
