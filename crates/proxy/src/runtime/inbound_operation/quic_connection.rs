//! Connection-oriented carriers prepare a dispatcher; runtime owns listeners.
use super::{InboundConnectionContext, PreparedInboundListenerOperation};
use crate::{protocol_registry::BoundInbound, runtime::route_runtime::InboundListenerRuntime};
use std::{future::Future, pin::Pin};
use zero_engine::EngineError;
pub(crate) struct QuicConnectionInboundListenerOperation<R, D> {
    pub(crate) protocol_name: &'static str,
    pub(crate) request: R,
    pub(crate) dispatch: D,
}
impl<R, D, F> PreparedInboundListenerOperation for QuicConnectionInboundListenerOperation<R, D>
where
    R: Clone + Send + Sync + 'static,
    D: Fn(R, quinn::Connection, std::net::SocketAddr, InboundConnectionContext) -> F
        + Clone
        + Send
        + Sync
        + 'static,
    F: Future<Output = Result<(), EngineError>> + Send + 'static,
{
    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        Box::pin(async move {
            let BoundInbound::Quic(listener) = bound else {
                return Err(EngineError::Io(std::io::Error::other(
                    "QUIC connection operation requires a QUIC listener",
                )));
            };
            let local = listener.local_addr()?;
            let Self {
                protocol_name,
                request,
                dispatch,
            } = *self;
            crate::runtime::listener_loop::run_quic_listener_loop(crate::runtime::listener_loop::QuicListenerLoopRequest {
                runtime_factory:runtime.route_factory(), protocol_name,listener,shutdown,
                handler:move |runtime,connection| {
                    let request = request.clone(); let dispatch = dispatch.clone();
                    async move {
                        if let Err(error) = dispatch(request,connection,local,InboundConnectionContext::new(runtime)).await {
                            tracing::error!(%error,protocol = protocol_name,"inbound QUIC carrier failed");
                        }
                    }
                },
            }).await
        })
    }
}
