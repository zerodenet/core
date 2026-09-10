//! Runtime-owned datagram peer dispatch, task fan-out and listener shutdown.
use super::{InboundConnectionContext, PreparedInboundListenerOperation};
use crate::{protocol_registry::BoundInbound, runtime::route_runtime::InboundListenerRuntime};
use std::{collections::HashMap, future::Future, net::SocketAddr, pin::Pin, sync::Arc};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, watch},
    task::JoinSet,
};
use zero_engine::EngineError;
pub(crate) struct PeerRouteInboundListenerOperation<R, D> {
    pub(crate) request: R,
    pub(crate) max_packet_size: usize,
    pub(crate) pending_packets: usize,
    pub(crate) dispatch: D,
}
impl<R, D, Fut> PreparedInboundListenerOperation for PeerRouteInboundListenerOperation<R, D>
where
    R: Clone + Send + Sync + 'static,
    D: Fn(R, Arc<UdpSocket>, SocketAddr, mpsc::Receiver<Vec<u8>>, InboundConnectionContext) -> Fut
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
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        Box::pin(async move {
            let BoundInbound::Datagram(socket) = bound else {
                return Err(EngineError::Io(std::io::Error::other(
                    "expected datagram listener",
                )));
            };
            let mut peers: HashMap<SocketAddr, mpsc::Sender<Vec<u8>>> = HashMap::new();
            let mut tasks = JoinSet::new();
            let max_packet_size = self.max_packet_size.clamp(1, 65534);
            let mut buffer = vec![0; max_packet_size + 1];
            loop {
                tokio::select! {
                    changed=shutdown.changed()=>{if changed.is_err() || *shutdown.borrow() {break;}}
                    recv=socket.recv_from(&mut buffer)=>{
                        let (n,peer)=recv?;
                        if n > max_packet_size {continue;}
                        peers.retain(|_,sender|!sender.is_closed());
                        if !peers.contains_key(&peer) {
                            if peers.len()>=128 {continue;}
                            let (sender,packets)=mpsc::channel(self.pending_packets.clamp(1, 1024));
                            let request=self.request.clone();let dispatch=self.dispatch.clone();let socket=socket.clone();
                            let context=InboundConnectionContext::new(runtime.route_factory().for_connection(Some(peer)));
                            tasks.spawn(async move {dispatch(request,socket,peer,packets,context).await});
                            peers.insert(peer,sender);
                        }
                        // Loss is handled by the owning reliable packet protocol.
                        let _=peers[&peer].try_send(buffer[..n].to_vec());
                    }
                    Some(result)=tasks.join_next()=>{
                        match result {
                            Ok(Ok(()))=>{},
                            Ok(Err(error))=>tracing::debug!(%error,"datagram route peer ended"),
                            Err(error)=>tracing::warn!(%error,"datagram route peer task failed"),
                        }
                        peers.retain(|_,sender|!sender.is_closed());
                    }
                }
            }
            tasks.shutdown().await;
            Ok(())
        })
    }
}
