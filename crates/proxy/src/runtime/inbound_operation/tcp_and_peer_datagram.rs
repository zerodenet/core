//! TCP listener plus bounded, independently routed UDP peer associations.
use std::{collections::HashMap, future::Future, net::SocketAddr, pin::Pin, sync::Arc};
use tokio::{net::UdpSocket, sync::mpsc};
use zero_core::InboundDatagramUdpRelay;
use zero_engine::EngineError;
mod peers;
#[cfg(test)]
mod tests;

use super::{
    InboundConnectionContext, PreparedInboundListenerOperation, TcpInboundListenerOperation,
};
use crate::{protocol_registry::BoundInbound, runtime::route_runtime::InboundListenerRuntime};

pub(crate) struct TcpAndPeerDatagramInboundListenerOperation<R, D, F> {
    pub(crate) tcp: TcpInboundListenerOperation<R, D>,
    pub(crate) relay_factory: F,
    pub(crate) peers: Arc<crate::runtime::inbound_owners::InboundPeerTasks>,
}

impl<R, D, F, Fut, U> PreparedInboundListenerOperation
    for TcpAndPeerDatagramInboundListenerOperation<R, D, F>
where
    R: Clone + Send + Sync + 'static,
    D: Fn(R, zero_platform_tokio::TokioSocket, InboundConnectionContext) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
    F: Fn(SocketAddr, mpsc::Receiver<Vec<u8>>) -> U + Send + 'static,
    U: InboundDatagramUdpRelay<Arc<UdpSocket>> + Send + 'static,
{
    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        Box::pin(async move {
            let BoundInbound::TcpAndDatagram(tcp, socket) = bound else {
                return Err(EngineError::Io(std::io::Error::other(
                    "expected TCP and UDP listeners",
                )));
            };
            let Self {
                tcp: tcp_operation,
                relay_factory,
                peers,
            } = *self;
            let host = runtime
                .host_control()
                .cloned()
                .ok_or_else(|| std::io::Error::other("peer listener owner was not installed"))?;
            let mut ready_branches = runtime.split_readiness(2)?.into_iter();
            let tcp_runtime = ready_branches.next().expect("TCP readiness branch");
            let udp_runtime_state = ready_branches.next().expect("UDP readiness branch");
            let udp_runtime = udp_runtime_state.udp_runtime();
            let ingress = udp_runtime.peer_ingress()?;
            let inbound_tag = runtime.inbound_tag().to_owned();
            let tcp_loop = Box::new(tcp_operation).execute(
                tcp_runtime,
                BoundInbound::Tcp(tcp),
                shutdown.clone(),
            );
            let peer_socket = socket.clone();
            let peer_resources = peers.clone();
            let udp_loop = crate::runtime::inbound_owners::InboundReady::poll_optional(
                udp_runtime_state.readiness(),
                peers::run_bound(
                    socket,
                    shutdown,
                    host.clone(),
                    peers,
                    move |peer, packets, binding| {
                        let relay = (relay_factory)(peer, packets);
                        let context = peer_resources.resource_context(binding, ingress);
                        let (slot, driver) =
                        crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot::channel();
                        let runtime = udp_runtime
                            .clone()
                            .with_peer_resources(context)
                            .with_direct_peer_driver(slot);
                        let socket = peer_socket.clone();
                        let tag = inbound_tag.clone();
                        async move {
                            crate::runtime::udp_socket::endpoint::peer::job::run_peer_with_driver(
                                crate::runtime::datagram_udp::run_protocol_datagram_udp_relay(
                                    runtime, socket, relay, &tag, true,
                                ),
                                driver,
                            )
                            .await
                        }
                    },
                ),
            );
            run_both(tcp_loop, udp_loop, host).await
        })
    }
}

async fn run_both(
    tcp: impl Future<Output = Result<(), EngineError>>,
    udp: impl Future<Output = Result<(), EngineError>>,
    host: crate::runtime::inbound_owners::InboundTaskAccess,
) -> Result<(), EngineError> {
    let udp_host = host.clone();
    let (tcp, udp) = tokio::join!(
        async {
            let result = tcp.await;
            host.request_stop();
            result
        },
        async {
            let result = udp.await;
            udp_host.request_stop();
            result
        },
    );
    // Both real loops have completed their cleanup before returning a failure.
    // Retain the unexpected peer failure source even when TCP also failed.
    match (tcp, udp) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(tcp), Err(udp)) => Err(std::io::Error::other(ListenerFailures { tcp, udp }).into()),
    }
}
#[derive(Debug)]
struct ListenerFailures {
    tcp: EngineError,
    udp: EngineError,
}
impl std::fmt::Display for ListenerFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TCP failed: {}; UDP failed: {}", self.tcp, self.udp)
    }
}
impl std::error::Error for ListenerFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.udp)
    }
}
