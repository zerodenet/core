//! Virtual packet peers over one actual masked Metadata original. The source
//! retains receive continuation; peer drivers live in the accepted host Scope.
use super::*;
use std::{net::SocketAddr, num::NonZeroUsize};
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
    last_delivery: Option<drivers::DriverDelivery<Option<Arc<io::Error>>>>,
    completion: Option<DriverCompletion>,
    unstarted: Option<users::UnstartedMetadataCompletion>,
    rejected: Option<GrantedMetadataListener>,
    stops: Vec<(u8, CancellationToken)>,
    socket: Option<PacketSocket>,
    peers: super::super::rows::Rows<SocketAddr, mpsc::Sender<bytes::Bytes>>,
    buffer: Option<super::super::memory::Scratch>,
    memory: Option<super::super::memory::Memory>,
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
        last_delivery: None,
        completion: None,
        unstarted: None,
        rejected: None,
        stops: Vec::new(),
        socket: None,
        peers: super::super::rows::Rows::new(peers.get()),
        buffer: None,
        memory: None,
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
        loop {
            let mut state = self.state.lock().await;
            let socket = state
                .socket
                .clone()
                .ok_or_else(|| failure(Reason::Revoked))?;
            let State {
                receiver,
                last_delivery,
                buffer,
                ..
            } = &mut *state;
            let received = tokio::select! {biased;
                _=self.cancel.cancelled()=>return Ok(None),
                _=context.cancel.cancelled()=>return Err(failure(Reason::Cancelled)),
                _=async {match context.deadline {Some(t)=>tokio::time::sleep_until(t).await,None=>std::future::pending().await}}=>return Err(failure(Reason::Timeout)),
                delivery=receiver.as_mut().unwrap().receive_next()=>{
                    *last_delivery=Some(delivery?);
                    let delivery=last_delivery.as_ref().unwrap();
                    receiver.as_mut().unwrap().release_body(delivery)?;
                    self.cancel.cancel();
                    return Err(match delivery.joined.result() {
                        Ok(())=>failure(Reason::Io),
                        Err(source)=>failure(Reason::Io).with_source(source.clone()),
                    });
                },
                packet=socket.recv_from(&mut buffer.as_mut().unwrap().bytes)=>packet.map_err(|e|failure(Reason::Io).with_source(e))?,
            };
            let (len, remote) = received;
            let notifications = state
                .memory
                .as_ref()
                .unwrap()
                .metadata()
                .map(|view| view.notifications());
            // This non-suspending step releases its actual owner lock before
            // dropping an unsent frame or invoking deferred Root callbacks.
            let mut state = ReadyStep {
                state,
                pending: None,
                _notifications: notifications,
            };
            state.peers.retain(|_, sender| !sender.is_closed());
            state.pending = Some(
                match state
                    .memory
                    .as_ref()
                    .unwrap()
                    .copy(&state.buffer.as_ref().unwrap().bytes[..len], remote)
                {
                    Ok(packet) => bytes::Bytes::from_owner(packet),
                    Err(source) if source.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(source) => return Err(failure(Reason::Capacity).with_source(source)),
                },
            );
            if let Some(sender) = state.peers.get(&remote).cloned() {
                let packet = state.pending.take().unwrap();
                drop(state);
                let _ = sender.try_send(packet);
                continue;
            }
            if state.peers.len() >= self.peers.get() {
                continue;
            }
            if state.peers.reserve(&remote).is_err() {
                continue;
            }
            let permit = state.graph.as_ref().unwrap().users().acquire()?;
            let local = socket
                .local_addr()
                .map_err(|e| failure(Reason::Io).with_source(e))?;
            let capacity = self.packets.get().min(
                state
                    .memory
                    .as_ref()
                    .unwrap()
                    .book()
                    .unwrap()
                    .limits()
                    .max_queue_packets,
            );
            let (sender, packets) = mpsc::channel(capacity);
            sender
                .try_send(state.pending.take().unwrap())
                .expect("new bounded peer queue");
            state.peers.insert_reserved(remote, sender);
            drop(state);
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
// Fields drop in declaration order. Never cross an await with this guard.
struct ReadyStep<'a, N> {
    state: tokio::sync::MutexGuard<'a, State>,
    pending: Option<bytes::Bytes>,
    _notifications: N,
}
impl<N> std::ops::Deref for ReadyStep<'_, N> {
    type Target = State;
    fn deref(&self) -> &State {
        &self.state
    }
}
impl<N> std::ops::DerefMut for ReadyStep<'_, N> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.state
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
