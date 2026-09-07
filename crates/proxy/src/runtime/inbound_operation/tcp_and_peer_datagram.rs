//! TCP listener plus bounded, independently routed UDP peer associations.
use std::{collections::HashMap, future::Future, net::SocketAddr, pin::Pin, sync::Arc};
use tokio::{net::UdpSocket, sync::mpsc, task::JoinSet};
use zero_core::InboundDatagramUdpRelay;
use zero_engine::EngineError;

use super::{
    InboundConnectionContext, PreparedInboundListenerOperation, TcpInboundListenerOperation,
};
use crate::{protocol_registry::BoundInbound, runtime::route_runtime::InboundListenerRuntime};

pub(crate) struct TcpAndPeerDatagramInboundListenerOperation<R, D, F> {
    pub(crate) tcp: TcpInboundListenerOperation<R, D>,
    pub(crate) relay_factory: F,
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
            } = *self;
            let udp_runtime = runtime.udp_runtime();
            let inbound_tag = runtime.inbound_tag().to_owned();
            let mut udp_shutdown = shutdown.clone();
            let tcp_loop =
                Box::new(tcp_operation).execute(runtime, BoundInbound::Tcp(tcp), shutdown);
            let udp_loop = async move {
                let mut peers: HashMap<SocketAddr, mpsc::Sender<Vec<u8>>> = HashMap::new();
                let mut tasks = JoinSet::new();
                let mut buffer = vec![0; 65535];
                loop {
                    tokio::select! {
                        changed = udp_shutdown.changed() => {
                            if changed.is_err() || *udp_shutdown.borrow() { break; }
                        }
                        recv = socket.recv_from(&mut buffer) => {
                            let (size, peer) = recv.map_err(EngineError::Io)?;
                            peers.retain(|_, sender| !sender.is_closed());
                            if !peers.contains_key(&peer) {
                                if peers.len() >= 4096 { continue; }
                                let (sender, packets) = mpsc::channel(32);
                                let relay = (relay_factory)(peer, packets);
                                let runtime = udp_runtime.clone();
                                let socket = socket.clone();
                                let tag = inbound_tag.clone();
                                tasks.spawn(async move {
                                    crate::runtime::datagram_udp::run_protocol_datagram_udp_relay(
                                        runtime, socket, relay, &tag, true,
                                    ).await
                                });
                                peers.insert(peer, sender);
                            }
                            // Bound memory and drop excess packets instead of blocking other peers.
                            let _ = peers[&peer].try_send(buffer[..size].to_vec());
                        }
                        Some(result) = tasks.join_next() => {
                            match result {
                                Ok(Ok(())) => {},
                                Ok(Err(error)) => tracing::debug!(%error, "UDP peer association ended"),
                                Err(error) => tracing::warn!(%error, "UDP peer association task failed"),
                            }
                            peers.retain(|_, sender| !sender.is_closed());
                        }
                    }
                }
                tasks.shutdown().await;
                Ok::<(), EngineError>(())
            };
            // Normal shutdown waits for peer tasks to release the listener socket.
            tokio::try_join!(tcp_loop, udp_loop)?;
            Ok(())
        })
    }
}
