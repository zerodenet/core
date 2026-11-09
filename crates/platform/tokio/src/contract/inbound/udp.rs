//! Bounded peer source over the host's actual UDP listener. No task or bind loop SPI.
mod peer;
use super::super::{
    component::{DatagramPeerLimits, ResourceAccess},
    context::InboundConnectionFacts,
    lifecycle::{CloseReport, ResourceLifecycle},
    CancellationToken, CloseReason, Impact, Reason, Stage,
};
use super::*;
use crate::contract::component::GrantedDatagramListener;
pub use peer::{
    DatagramPeer, DatagramPeerAdmission, DatagramPeerRetirement, PeerControl, PeerRx, PeerTx,
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, Mutex},
    time::Instant,
};
use zero_core::request::Destination;
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum PeerKey {
    Endpoint(SocketAddr),
    AuthenticatedAssociation(u64),
}
struct PeerPacket {
    payload: Vec<u8>,
    event: Option<InboundDatagramEvent>,
}
struct State {
    io: Option<GrantedDatagramListener>,
    peers: HashMap<PeerKey, (mpsc::Sender<PeerPacket>, DatagramPeerRetirement)>,
    buffer: Vec<u8>,
}
pub struct UdpSourceLifecycle {
    state: Arc<Mutex<State>>,
    cancel: CancellationToken,
    resources: Arc<dyn ResourceAccess>,
    address: Destination,
    started: bool,
}
pub struct UdpSource<
    Make,
    Decode = fn(&[u8], SocketAddr) -> Result<Option<InboundDatagramEvent>, ChannelError>,
> {
    state: Arc<Mutex<State>>,
    cancel: CancellationToken,
    limits: DatagramPeerLimits,
    idle: Duration,
    make: Make,
    decode: Decode,
}
pub fn source<H, Make>(
    address: Destination,
    resources: Arc<dyn ResourceAccess>,
    cancel: CancellationToken,
    limits: DatagramPeerLimits,
    idle: Duration,
    make: Make,
) -> (UdpSourceLifecycle, UdpSource<Make>)
where
    H: InboundHandshake,
    Make: Fn(DatagramPeer) -> H + Send + Sync + 'static,
{
    source_events(
        address,
        resources,
        cancel,
        limits,
        idle,
        make,
        raw_packet as fn(&[u8], SocketAddr) -> Result<Option<InboundDatagramEvent>, ChannelError>,
    )
}
fn raw_packet(_: &[u8], _: SocketAddr) -> Result<Option<InboundDatagramEvent>, ChannelError> {
    Ok(None)
}
/// Decode at the owning protocol boundary before admission. Authenticated
/// association facts allow address roaming without a second socket/task owner.
/// No protocol name, config or crypto is interpreted by this source.
pub fn source_events<H, Make, Decode>(
    address: Destination,
    resources: Arc<dyn ResourceAccess>,
    cancel: CancellationToken,
    limits: DatagramPeerLimits,
    idle: Duration,
    make: Make,
    decode: Decode,
) -> (UdpSourceLifecycle, UdpSource<Make, Decode>)
where
    H: InboundHandshake,
    Make: Fn(DatagramPeer) -> H + Send + Sync + 'static,
    Decode: Fn(&[u8], SocketAddr) -> Result<Option<InboundDatagramEvent>, ChannelError>
        + Send
        + Sync
        + 'static,
{
    let state = Arc::new(Mutex::new(State {
        io: None,
        peers: HashMap::new(),
        buffer: vec![0; 65536],
    }));
    let cancel = cancel.child_token();
    (
        UdpSourceLifecycle {
            state: state.clone(),
            cancel: cancel.clone(),
            resources,
            address,
            started: false,
        },
        UdpSource {
            state,
            cancel,
            limits,
            idle,
            make,
            decode,
        },
    )
}
impl ResourceLifecycle for UdpSourceLifecycle {
    async fn start(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        if self.started || self.cancel.is_cancelled() {
            return Err(error(Reason::Revoked));
        }
        self.started = true;
        let grant = super::super::context::with_deadline(
            Some(deadline),
            self.resources.listen_datagram(self.address.clone()),
        )
        .await
        .map_err(|source| error(Reason::Timeout).with_source(source))??;
        self.state.lock().await.io = Some(grant);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.cancel.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel.cancel();
        let mut state = super::super::context::with_deadline(Some(deadline), self.state.lock())
            .await
            .map_err(|source| error(Reason::Timeout).with_source(source))?;
        state.peers.clear();
        state.io.take();
        // Views returned only. The host still consumes the original native receipt.
        Ok(CloseReport::default())
    }
}
impl<H, Make, Decode> InboundSource for UdpSource<Make, Decode>
where
    H: InboundHandshake,
    Make: Fn(DatagramPeer) -> H + Send + Sync + 'static,
    Decode: Fn(&[u8], SocketAddr) -> Result<Option<InboundDatagramEvent>, ChannelError>
        + Send
        + Sync
        + 'static,
{
    type Handshake = PendingInbound;
    async fn next(
        &mut self,
        context: AcceptContext,
    ) -> Result<Option<PendingInbound>, ChannelError> {
        let work = async {
            let mut state = self.state.lock().await;
            loop {
                let State { io, peers, buffer } = &mut *state;
                let io = io.as_mut().ok_or_else(|| error(Reason::Revoked))?;
                let (size, remote) = io
                    .rx
                    .recv_from_addr(buffer)
                    .await
                    .map_err(|source| error(Reason::Io).with_source(source))?;
                let event = match (self.decode)(&buffer[..size], remote) {
                    Ok(event) => event,
                    Err(error) if error.impact == Impact::Datagram => continue,
                    Err(error) => return Err(error),
                };
                let key = match &event {
                    Some(InboundDatagramEvent::AuthenticatedPacket {
                        dispatch, sender, ..
                    }) if *sender == remote => PeerKey::AuthenticatedAssociation(
                        dispatch
                            .client_session_id()
                            .ok_or_else(|| error(Reason::Protocol))?,
                    ),
                    Some(InboundDatagramEvent::AuthenticatedPacket { .. }) => {
                        return Err(error(Reason::Protocol))
                    }
                    _ => PeerKey::Endpoint(remote),
                };
                let packet = PeerPacket {
                    payload: if event.is_some() {
                        Vec::new()
                    } else {
                        buffer[..size].to_vec()
                    },
                    event,
                };
                peers.retain(|_, (_, retirement)| !retirement.is_released());
                if let Some((sender, _)) = peers.get(&key) {
                    // A closed receiver does not certify original peer cleanup.
                    let _ = sender.try_send(packet);
                    continue;
                }
                if peers.len() >= self.limits.max_peers.get() {
                    continue;
                }
                let (sender, packets) = mpsc::channel(self.limits.packets_per_peer.get());
                sender
                    .try_send(packet)
                    .expect("first packet in empty bounded queue");
                let facts = InboundConnectionFacts {
                    original_source: Some(remote),
                    effective_source: Some(remote),
                    local_endpoint: Some(
                        io.rx
                            .local_addr()
                            .map_err(|source| error(Reason::Io).with_source(source))?,
                    ),
                    sni: None,
                };
                let (socket, retirement) = peer::make(
                    remote,
                    io.tx.clone(),
                    packets,
                    self.cancel.child_token(),
                    self.idle,
                    match key {
                        PeerKey::AuthenticatedAssociation(id) => Some(id),
                        PeerKey::Endpoint(_) => None,
                    },
                );
                let admission = DatagramPeerAdmission::new(
                    socket.control(),
                    io.tx.identity(),
                    facts,
                    self.limits,
                    retirement.clone(),
                );
                peers.insert(key, (sender, retirement));
                let mut pending = PendingInbound::new((self.make)(socket));
                pending.peer = Some(admission);
                return Ok(Some(pending));
            }
        };
        tokio::select! { biased;
            _ = context.cancel.cancelled() => Err(error(Reason::Cancelled)),
            _ = self.cancel.cancelled() => Ok(None),
            result = super::super::context::with_deadline(context.deadline, work) => {
                result.map_err(|source| error(Reason::Timeout).with_source(source))?
            }
        }
    }
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Receive, reason, Impact::Component)
}
