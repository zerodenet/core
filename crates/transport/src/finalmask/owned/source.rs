//! Virtual packet peers over one actual masked Metadata original. The source
//! retains receive continuation; peer drivers live in the accepted host Scope.
use super::*;
use std::{collections::HashMap, net::SocketAddr, num::NonZeroUsize};
use tokio::{
    sync::{mpsc, Mutex},
    time::Instant,
};
use zero_platform_tokio::{
    contract::{
        component::*, context::*, inbound::*, lifecycle::*, scope::TaskRegistrar,
        CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
    },
    controlled_datagram::metadata::users::{self, drivers, LogicalUserPermit},
    PacketSocket,
};
mod lifecycle;
mod peer;
pub use peer::Peer;
type Graph = users::MetadataUserGraph<(DriverKind, u8), Option<Arc<io::Error>>>;
type Receiver = drivers::DriverExitReceiver<(DriverKind, u8), Option<Arc<io::Error>>>;
type Permit = LogicalUserPermit<drivers::Admission<(DriverKind, u8), Option<Arc<io::Error>>>>;
struct State {
    native: Option<crate::quic::owned::HostedMetadataSocket>,
    graph: Option<Graph>,
    receiver: Option<Receiver>,
    completion: Option<DriverCompletion>,
    unstarted: Option<users::UnstartedMetadataCompletion>,
    rejected: Option<GrantedMetadataListener>,
    stops: Vec<(u8, CancellationToken)>,
    socket: Option<PacketSocket>,
    peers: HashMap<SocketAddr, mpsc::Sender<Vec<u8>>>,
    buffer: Vec<u8>,
}
type DriverCompletion = users::MetadataUsersCompletion<(DriverKind, u8), Option<Arc<io::Error>>>;

pub struct Lifecycle {
    state: Arc<Mutex<State>>,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
    request: MetadataDatagramRequest,
    masks: Vec<Mask>,
    cancel: CancellationToken,
    peers: NonZeroUsize,
}
pub struct Source<Make> {
    state: Arc<Mutex<State>>,
    cancel: CancellationToken,
    make: Make,
    peers: NonZeroUsize,
    packets: NonZeroUsize,
}
#[expect(
    clippy::too_many_arguments,
    reason = "existing concrete source keeps original resources, task scope and handshake inputs explicit"
)]
pub fn source<H: InboundHandshake, Make: Fn(Peer) -> H + Send + 'static>(
    request: MetadataDatagramRequest,
    masks: Vec<Mask>,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
    cancel: CancellationToken,
    peers: NonZeroUsize,
    packets: NonZeroUsize,
    make: Make,
) -> (Lifecycle, Source<Make>) {
    let state = Arc::new(Mutex::new(State {
        native: None,
        graph: None,
        receiver: None,
        completion: None,
        unstarted: None,
        rejected: None,
        stops: Vec::new(),
        socket: None,
        peers: HashMap::new(),
        buffer: vec![0; 65536],
    }));
    let cancel = cancel.child_token();
    (
        Lifecycle {
            state: state.clone(),
            resources,
            tasks,
            request,
            masks,
            cancel: cancel.clone(),
            peers,
        },
        Source {
            state,
            cancel,
            make,
            peers,
            packets,
        },
    )
}
impl<H: InboundHandshake, Make: Fn(Peer) -> H + Send + 'static> InboundSource for Source<Make> {
    type Handshake = PendingInbound;
    async fn next(
        &mut self,
        context: AcceptContext,
    ) -> Result<Option<PendingInbound>, ChannelError> {
        let mut state = self.state.lock().await;
        let socket = state
            .socket
            .clone()
            .ok_or_else(|| failure(Reason::Revoked))?;
        loop {
            let State {
                receiver, buffer, ..
            } = &mut *state;
            let received = tokio::select! {biased;
                _=self.cancel.cancelled()=>return Ok(None),
                _=context.cancel.cancelled()=>return Err(failure(Reason::Cancelled)),
                _=async {match context.deadline {Some(t)=>tokio::time::sleep_until(t).await,None=>std::future::pending().await}}=>return Err(failure(Reason::Timeout)),
                delivery=receiver.as_mut().unwrap().receive_next()=>{
                    let delivery=delivery?;
                    self.cancel.cancel();
                    return Err(match delivery.joined.result() {
                        Ok(())=>failure(Reason::Io),
                        Err(source)=>failure(Reason::Io).with_source(source.clone()),
                    });
                },
                packet=socket.recv_from(buffer)=>packet.map_err(|e|failure(Reason::Io).with_source(e))?,
            };
            let (len, remote) = received;
            state.peers.retain(|_, sender| !sender.is_closed());
            let packet = state.buffer[..len].to_vec();
            if let Some(sender) = state.peers.get(&remote) {
                let _ = sender.try_send(packet);
                continue;
            }
            if state.peers.len() >= self.peers.get() {
                continue;
            }
            let permit = state.graph.as_ref().unwrap().users().acquire()?;
            let (sender, packets) = mpsc::channel(self.packets.get());
            sender.try_send(packet).expect("new bounded peer queue");
            state.peers.insert(remote, sender);
            let local = socket
                .local_addr()
                .map_err(|e| failure(Reason::Io).with_source(e))?;
            let facts = InboundConnectionFacts {
                original_source: Some(remote),
                effective_source: Some(remote),
                local_endpoint: Some(local),
                sni: None,
            };
            let peer = Peer::new(socket.clone(), remote, packets, permit, facts.clone());
            return Ok(Some(
                PendingInbound::new((self.make)(peer)).with_carrier_facts(facts),
            ));
        }
    }
}
fn failure(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
}

#[derive(Debug)]
struct OriginalIo(Arc<io::Error>);
impl std::fmt::Display for OriginalIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for OriginalIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
