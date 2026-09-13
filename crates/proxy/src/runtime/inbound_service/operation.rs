use crate::{
    protocol_registry::UpstreamConnectServices,
    runtime::{
        mux_session::{run_protocol_mux_session, MuxSessionLoop},
        mux_tcp::run_protocol_mux_tcp_task_with_sniffing,
        mux_udp::run_protocol_mux_udp_task_with_sniffing,
        route_runtime::{InboundRouteRuntime, SharedIngressRuntimeServices},
        sniff::SniffingPolicy,
    },
};
use std::{future::Future, pin::Pin};
use zero_core::{InboundMuxServer, InboundMuxTcpRelay, InboundMuxUdpRelay};
use zero_engine::EngineError;

pub(crate) struct ServiceContext {
    pub(crate) upstream: UpstreamConnectServices,
    pub(crate) ingress: SharedIngressRuntimeServices,
}
pub(crate) trait PreparedInboundServiceOperation: Send {
    fn run(
        self: Box<Self>,
        context: ServiceContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>>;
}
pub(crate) trait DialedMuxSource: Send + Sync + 'static {
    type Stream: Send + 'static;
    type Server: InboundMuxServer<Self::Stream> + 'static;
    fn next_connection(
        &self,
        upstream: UpstreamConnectServices,
    ) -> impl Future<Output = Result<(Self::Stream, Self::Server), EngineError>> + Send;
}
pub(crate) struct MuxServiceOperation<S> {
    pub(crate) source: S,
    pub(crate) tag: String,
    pub(crate) protocol: &'static str,
    pub(crate) sniffing: Option<SniffingPolicy>,
}
impl<S> PreparedInboundServiceOperation for MuxServiceOperation<S>
where
    S: DialedMuxSource,
    <S::Server as InboundMuxServer<S::Stream>>::TcpRelay: InboundMuxTcpRelay + 'static,
    <S::Server as InboundMuxServer<S::Stream>>::UdpRelay: InboundMuxUdpRelay + 'static,
{
    fn run(
        self: Box<Self>,
        context: ServiceContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>> {
        Box::pin(async move {
            let mut workers = tokio::task::JoinSet::new();
            loop {
                // Keep the demand/handshake future alive while a previous worker
                // finishes. Dropping a partially accepted connection loses data.
                let next = self.source.next_connection(context.upstream.clone());
                tokio::pin!(next);
                let opened = loop {
                    tokio::select! {
                        opened = &mut next => break opened,
                        result = workers.join_next(), if !workers.is_empty() => {
                            if let Some(Err(error)) = result { return Err(std::io::Error::other(error).into()); }
                        }
                    }
                };
                let (stream, server) = match opened {
                    Ok(opened) => opened,
                    Err(error) => {
                        tracing::warn!(inbound_tag = %self.tag, protocol = self.protocol, %error, "background ingress connection failed");
                        continue;
                    }
                };
                let runtime =
                    InboundRouteRuntime::new(context.ingress.clone(), self.tag.clone(), None)
                        .into_mux_substream_runtime();
                let tag = self.tag.clone();
                let protocol = self.protocol;
                let sniffing = self.sniffing.clone();
                workers.spawn(async move {
                    let tcp_sniffing = sniffing.clone();
                    let result = run_protocol_mux_session(runtime, stream, server, MuxSessionLoop {
                        inbound_tag: tag.clone(), protocol, panic_message: "background MUX route task panicked", abort_on_end: true,
                    }, move |runtime, session, relay| -> Pin<Box<dyn Future<Output = ()> + Send>> {
                        let sniffing = tcp_sniffing.clone();
                        Box::pin(async move {
                            run_protocol_mux_tcp_task_with_sniffing(
                                runtime,
                                session,
                                relay,
                                protocol,
                                sniffing,
                            )
                            .await;
                        })
                    },
                    move |runtime, relay| -> Pin<Box<dyn Future<Output = ()> + Send>> {
                        let sniffing = sniffing.clone();
                        Box::pin(async move {
                            run_protocol_mux_udp_task_with_sniffing(
                                runtime, relay, protocol, sniffing,
                            )
                            .await;
                        })
                    }).await;
                    if let Err(error) = result {
                        tracing::debug!(inbound_tag = %tag, protocol, %error, "background ingress worker ended");
                    }
                });
            }
        })
    }
}
