use std::future::Future;
use std::pin::Pin;

use zero_engine::EngineError;

use super::{InboundConnectionContext, PreparedInboundListenerOperation};
use crate::protocol_registry::BoundInbound;
use crate::runtime::route_runtime::InboundListenerRuntime;

pub(crate) struct TcpOrQuicInboundListenerOperation<R, TD, QD> {
    pub(crate) protocol_name: &'static str,
    pub(crate) error_protocol_name: &'static str,
    pub(crate) request: R,
    pub(crate) dispatch_tcp: TD,
    pub(crate) dispatch_quic: QD,
}

impl<R, TD, TFut, QD, QFut> PreparedInboundListenerOperation
    for TcpOrQuicInboundListenerOperation<R, TD, QD>
where
    R: Clone + Send + Sync + 'static,
    TD: Fn(R, zero_platform_tokio::TokioSocket, InboundConnectionContext) -> TFut
        + Clone
        + Send
        + Sync
        + 'static,
    TFut: Future<Output = Result<(), EngineError>> + Send + 'static,
    QD: Fn(R, crate::transport::QuicStream, InboundConnectionContext) -> QFut
        + Clone
        + Send
        + Sync
        + 'static,
    QFut: Future<Output = Result<(), EngineError>> + Send + 'static,
{
    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        Box::pin(async move {
            let TcpOrQuicInboundListenerOperation {
                protocol_name,
                error_protocol_name,
                request,
                dispatch_tcp,
                dispatch_quic,
            } = *self;
            let runtime_factory = runtime.route_factory();
            match bound {
                #[cfg(feature = "managed-datagram-runtime")]
                BoundInbound::TcpAndDatagram(..) => Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "TCP/QUIC operation received combined datagram listener",
                ))),
                BoundInbound::Tcp(listener) => {
                    crate::runtime::listener_loop::run_logged_tcp_socket_listener_loop(
                        crate::runtime::listener_loop::LoggedTcpSocketListenerRequest {
                            runtime_factory,
                            protocol_name,
                            error_protocol_name,
                            request,
                            listener,
                            shutdown,
                            dispatch: move |runtime, request, socket| {
                                let dispatch = dispatch_tcp.clone();
                                async move {
                                    dispatch(
                                        request,
                                        socket,
                                        InboundConnectionContext::new(runtime),
                                    )
                                    .await
                                }
                            },
                        },
                    )
                    .await
                }
                BoundInbound::Quic(listener) => {
                    crate::runtime::listener_loop::run_logged_quic_stream_listener_loop(
                        crate::runtime::listener_loop::LoggedQuicStreamListenerRequest {
                            runtime_factory,
                            protocol_name,
                            error_protocol_name,
                            request,
                            listener,
                            shutdown,
                            dispatch: move |runtime, request, stream| {
                                let dispatch = dispatch_quic.clone();
                                async move {
                                    dispatch(
                                        request,
                                        stream,
                                        InboundConnectionContext::new(runtime),
                                    )
                                    .await
                                }
                            },
                        },
                    )
                    .await
                }
            }
        })
    }
}
