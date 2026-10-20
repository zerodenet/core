use std::future::Future;
use std::pin::Pin;

use zero_engine::EngineError;

use super::{InboundConnectionContext, PreparedInboundListenerOperation};
use crate::protocol_registry::BoundInbound;
use crate::runtime::route_runtime::InboundListenerRuntime;

pub(crate) struct TcpInboundListenerOperation<R, D> {
    pub(crate) protocol_name: &'static str,
    pub(crate) error_protocol_name: &'static str,
    pub(crate) request: R,
    pub(crate) dispatch: D,
}

impl<R, D, Fut> PreparedInboundListenerOperation for TcpInboundListenerOperation<R, D>
where
    R: Clone + Send + Sync + 'static,
    D: Fn(R, zero_platform_tokio::TokioSocket, InboundConnectionContext) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
{
    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        Box::pin(async move {
            let TcpInboundListenerOperation {
                protocol_name,
                error_protocol_name,
                request,
                dispatch,
            } = *self;
            crate::runtime::inbound_owners::InboundReady::poll_optional(
                runtime.readiness(),
                crate::runtime::listener_loop::run_logged_tcp_socket_listener_loop(
                    crate::runtime::listener_loop::LoggedTcpSocketListenerRequest {
                        runtime_factory: runtime.route_factory(),
                        protocol_name,
                        error_protocol_name,
                        request,
                        listener: bound.into_tcp(),
                        shutdown,
                        dispatch: move |runtime, request, socket| {
                            let dispatch = dispatch.clone();
                            async move {
                                dispatch(request, socket, InboundConnectionContext::new(runtime))
                                    .await
                            }
                        },
                    },
                ),
            )
            .await
        })
    }
}
