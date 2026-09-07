//! Direct inbound - fixed-target forwarder.
//!
//! Listens on a port, accepts raw TCP connections with no protocol
//! handshake, and forwards all traffic through the kernel pipeline
//! to a configured outbound (node or group).

use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::EngineError;

use crate::runtime::inbound_operation::{
    InboundConnectionContext, PreparedInboundListenerOperation, TcpInboundListenerOperation,
};
use crate::runtime::tcp_ingress::NoClientResponseStreamProtocol;
use crate::transport::TcpRelayStream;

#[cfg(feature = "managed-datagram-runtime")]
mod datagram;

#[derive(Debug)]
pub(crate) struct DirectInboundListenerOperation {
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) udp: bool,
    pub(crate) target: Option<Address>,
    pub(crate) port: Option<u16>,
}

impl PreparedInboundListenerOperation for DirectInboundListenerOperation {
    fn execute(
        self: Box<Self>,
        runtime: crate::runtime::route_runtime::InboundListenerRuntime,
        bound: crate::protocol_registry::BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), EngineError>> + Send + 'static>,
    > {
        let operation = TcpInboundListenerOperation {
            protocol_name: "direct",
            error_protocol_name: "direct",
            request: (self.target, self.port),
            dispatch: |(target, port): (Option<Address>, Option<u16>),
                       socket,
                       context: InboundConnectionContext| async move {
                let Some(target) = target else {
                    return Ok(());
                };
                let session = Session::new(
                    0,
                    target,
                    port.unwrap_or(443),
                    Network::Tcp,
                    ProtocolType::UNKNOWN,
                );
                context
                    .serve(
                        session,
                        TcpRelayStream::from(socket),
                        NoClientResponseStreamProtocol::new(),
                    )
                    .await
            },
        };
        #[cfg(feature = "managed-datagram-runtime")]
        if self.udp {
            let target = operation.request.0.clone();
            let port = self.port.unwrap_or(443);
            let combined =
                crate::runtime::inbound_operation::TcpAndPeerDatagramInboundListenerOperation {
                    tcp: operation,
                    relay_factory: move |peer, packets| {
                        datagram::DirectDatagramRelay::new(target.clone(), port, peer, packets)
                    },
                };
            return Box::new(combined).execute(runtime, bound, shutdown);
        }
        Box::new(operation).execute(runtime, bound, shutdown)
    }
}
