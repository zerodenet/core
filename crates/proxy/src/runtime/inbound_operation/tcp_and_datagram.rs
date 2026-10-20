use std::future::Future;
use std::pin::Pin;

use zero_engine::EngineError;

use super::{PreparedInboundListenerOperation, TcpInboundListenerOperation};
use crate::protocol_registry::BoundInbound;
use crate::runtime::route_runtime::InboundListenerRuntime;

pub(crate) struct TcpAndDatagramInboundListenerOperation<R, D, U> {
    pub(crate) host: Option<crate::runtime::inbound_owners::InboundTaskAccess>,
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
    fn internal_tasks(&self) -> usize {
        1
    }
    fn install_resources(
        &mut self,
        context: &crate::runtime::inbound_owners::InboundInstallContext,
    ) -> Result<(), EngineError> {
        if let Some(carrier) = &self.carrier {
            context.register(carrier.owner())?;
        }
        self.host = Some(context.tasks());
        Ok(())
    }
    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        Box::pin(async move {
            let TcpAndDatagramInboundListenerOperation {
                carrier,
                host,
                protocol_name,
                error_protocol_name,
                listen_address,
                listen_port,
                tcp_request,
                tcp_dispatch,
                udp_relay,
            } = *self;
            let (bound, udp_socket, mut completion) = if let Some(carrier) = carrier {
                let mut activation_shutdown = shutdown.clone();
                let carrier = tokio::select! { biased;
                    _ = async {
                        if *activation_shutdown.borrow() { return; }
                        let _ = activation_shutdown.wait_for(|stop| *stop).await;
                    } => return Ok(()),
                    activated = carrier.activate(bound.into_tcp()) => activated?,
                };
                (
                    BoundInbound::Tcp(carrier.listener),
                    Some(carrier.datagram),
                    carrier.completion,
                )
            } else {
                let socket =
                    crate::protocol_registry::bind_datagram_listener(&listen_address, listen_port)
                        .await?;
                (
                    bound,
                    Some(std::sync::Arc::new(socket)),
                    Box::pin(std::future::pending())
                        as zero_transport::inbound_carrier::CarrierFuture<()>,
                )
            };
            let host = host.ok_or_else(|| {
                EngineError::Io(std::io::Error::other(
                    "inbound resource owner was not installed",
                ))
            })?;
            if host.cancellation().is_cancelled() || *shutdown.borrow() {
                return Ok(());
            }
            if let Some(socket) = udp_socket.as_ref() {
                let udp_runtime = runtime.udp_runtime();
                let inbound_tag = runtime.inbound_tag().to_owned();
                let socket = socket.clone();
                let cancel = host.cancellation();
                host.spawn(async move {
                    tokio::select! { biased;
                    _ = cancel.cancelled() => Ok(()),
                    result = crate::runtime::datagram_udp::run_protocol_datagram_udp_relay(
                        udp_runtime,
                        socket,
                        udp_relay,
                        &inbound_tag,
                        false,
                    )
                    => result,
                    }
                })?;
            }
            let mut tcp = Box::new(TcpInboundListenerOperation {
                protocol_name,
                error_protocol_name,
                request: tcp_request,
                dispatch: tcp_dispatch,
            })
            .execute(runtime, bound, shutdown);
            let (mut result, tcp_finished) = tokio::select! {
                result = &mut tcp => (result, true),
                result = &mut completion => (result.map_err(EngineError::from), false),
            };

            // A failed carrier stops acceptance, but the real TCP driver must
            // still join its accepted connections before its Scope task exits.
            host.request_stop();
            if !tcp_finished {
                if let Err(error) = tcp.await {
                    if result.is_ok() {
                        result = Err(error);
                    } else {
                        tracing::warn!(%error, "TCP cleanup also failed after carrier exit");
                    }
                }
            }
            // The persistent host owns and joins the actual UDP driver.
            result
        })
    }
}
