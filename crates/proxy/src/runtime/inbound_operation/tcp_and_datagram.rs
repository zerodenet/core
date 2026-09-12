use std::future::Future;
use std::pin::Pin;

use zero_engine::EngineError;

use super::{PreparedInboundListenerOperation, TcpInboundListenerOperation};
use crate::protocol_registry::BoundInbound;
use crate::runtime::route_runtime::InboundListenerRuntime;

struct AbortTask(tokio::task::AbortHandle);
impl Drop for AbortTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) struct TcpAndDatagramInboundListenerOperation<R, D, U> {
    pub(crate) carrier: Option<Box<dyn zero_transport::inbound_carrier::InboundCarrierPlan>>,
    pub(crate) protocol_name: &'static str,
    pub(crate) error_protocol_name: &'static str,
    pub(crate) listen_address: String,
    pub(crate) listen_port: u16,
    pub(crate) tcp_request: R,
    pub(crate) tcp_dispatch: D,
    pub(crate) udp_relay: U,
}

impl<R, D, Fut, U> PreparedInboundListenerOperation
    for TcpAndDatagramInboundListenerOperation<R, D, U>
where
    R: Clone + Send + Sync + 'static,
    D: Fn(R, zero_platform_tokio::TokioSocket, super::InboundConnectionContext) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
    U: zero_core::InboundDatagramUdpRelay<std::sync::Arc<tokio::net::UdpSocket>> + Send + 'static,
{
    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        Box::pin(async move {
            let TcpAndDatagramInboundListenerOperation {
                carrier,
                protocol_name,
                error_protocol_name,
                listen_address,
                listen_port,
                tcp_request,
                tcp_dispatch,
                udp_relay,
            } = *self;
            let (bound, udp_socket, mut completion) = if let Some(carrier) = carrier {
                let carrier = carrier.activate(bound.into_tcp()).await?;
                (
                    BoundInbound::Tcp(carrier.listener),
                    Some(carrier.datagram),
                    carrier.completion,
                )
            } else {
                let socket =
                    tokio::net::UdpSocket::bind(format!("{listen_address}:{listen_port}")).await?;
                (
                    bound,
                    Some(std::sync::Arc::new(socket)),
                    Box::pin(std::future::pending())
                        as zero_transport::inbound_carrier::CarrierFuture<()>,
                )
            };
            let udp_task = udp_socket.as_ref().map(|socket| {
                let udp_runtime = runtime.udp_runtime();
                let inbound_tag = runtime.inbound_tag().to_owned();
                let socket = socket.clone();
                tokio::spawn(async move {
                    crate::runtime::datagram_udp::run_protocol_datagram_udp_relay(
                        udp_runtime,
                        socket,
                        udp_relay,
                        &inbound_tag,
                        false,
                    )
                    .await
                })
            });

            let _abort = udp_task.as_ref().map(|task| AbortTask(task.abort_handle()));
            let tcp = Box::new(TcpInboundListenerOperation {
                protocol_name,
                error_protocol_name,
                request: tcp_request,
                dispatch: tcp_dispatch,
            })
            .execute(runtime, bound, shutdown);
            let result = tokio::select! {
                result = tcp => result,
                result = &mut completion => result.map_err(EngineError::from),
            };

            if let Some(task) = udp_task {
                task.abort();
                let _ = task.await;
            }
            result
        })
    }
}
