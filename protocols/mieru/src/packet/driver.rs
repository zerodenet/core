mod receive;
mod send;
#[cfg(test)]
mod tests;
use super::{PacketCodec, ReliableSession};
use crate::{
    client::OpenRequest,
    inbound::multiplex::{reader::Reader, stream::Status, writer::Command},
};
use std::{collections::BTreeMap, io, net::SocketAddr, sync::Arc};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    time::{Duration, Instant},
};

pub(crate) enum PacketIo {
    Client {
        carrier: Arc<dyn crate::client::ClientDatagramCarrier>,
    },
    Server {
        socket: Arc<UdpSocket>,
        peer: SocketAddr,
        packets: mpsc::Receiver<Vec<u8>>,
    },
}
impl PacketIo {
    async fn receive(&mut self) -> io::Result<Vec<u8>> {
        match self {
            Self::Server { packets, .. } => packets
                .recv()
                .await
                .ok_or_else(|| io::ErrorKind::BrokenPipe.into()),
            Self::Client { carrier } => {
                let mut buffer = vec![0; 1501];
                let read = carrier.receive(&mut buffer).await?;
                buffer.truncate(read);
                Ok(buffer)
            }
        }
    }
    async fn send(&self, data: &[u8]) -> io::Result<()> {
        match self {
            Self::Client { carrier } => carrier.send(data).await,
            Self::Server { socket, peer, .. } => {
                socket.send_to(data, *peer).await?;
                Ok(())
            }
        }
    }
}
struct Barrier {
    through: u32,
    close: bool,
    done: oneshot::Sender<io::Result<()>>,
}
struct State {
    reliable: ReliableSession,
    barriers: Vec<Barrier>,
    closing: Option<Instant>,
}
#[derive(Clone, Copy)]
enum Retirement {
    Local,
    PeerClosed,
    LocalCloseAcknowledged,
    Failed(io::ErrorKind),
}
impl Retirement {
    fn status_error(self) -> Option<io::ErrorKind> {
        match self {
            Self::Failed(kind) => Some(kind),
            Self::Local | Self::PeerClosed | Self::LocalCloseAcknowledged => None,
        }
    }
}
impl State {
    fn advance_barriers(&mut self) -> io::Result<()> {
        let mut waiting = Vec::new();
        for barrier in self.barriers.drain(..) {
            if self.reliable.is_flushed(barrier.through) {
                if barrier.close {
                    if self.closing.is_none() {
                        self.reliable
                            .queue_control(crate::metadata::CLOSE_SESSION_REQUEST)?;
                        self.closing = Some(Instant::now());
                    }
                    // Shutdown includes the close handshake. Data ACKs release
                    // an ordinary flush, but do not complete a close barrier.
                    waiting.push(barrier);
                } else {
                    let _ = barrier.done.send(Ok(()));
                }
            } else {
                waiting.push(barrier);
            }
        }
        self.barriers = waiting;
        Ok(())
    }

    fn complete_barriers(&mut self, retirement: Retirement) {
        for barrier in self.barriers.drain(..) {
            let flushed = self.reliable.is_flushed(barrier.through);
            let success = if barrier.close {
                flushed
                    && match retirement {
                        Retirement::PeerClosed => true,
                        Retirement::LocalCloseAcknowledged => self.closing.is_some(),
                        Retirement::Local | Retirement::Failed(_) => false,
                    }
            } else {
                flushed
            };
            let result = success.then_some(()).ok_or_else(|| {
                io::Error::from(
                    retirement
                        .status_error()
                        .unwrap_or(io::ErrorKind::BrokenPipe),
                )
            });
            let _ = barrier.done.send(result);
        }
    }
}
struct Tombstone {
    since: Instant,
    sequence: u32,
}
pub(crate) struct Driver {
    pub(crate) book: Reader,
    pub(crate) codec: PacketCodec,
    pub(crate) io: PacketIo,
    pub(crate) client: bool,
    states: BTreeMap<u32, State>,
    retired: BTreeMap<u32, Tombstone>,
    id: u32,
    idle: Instant,
}
impl Driver {
    pub(crate) fn new(book: Reader, codec: PacketCodec, io: PacketIo, client: bool) -> Self {
        Self {
            book,
            codec,
            io,
            client,
            states: BTreeMap::new(),
            retired: BTreeMap::new(),
            id: rand::random::<u32>() & 0x7fffffff,
            idle: Instant::now(),
        }
    }
    fn start(&mut self, id: u32) -> io::Result<()> {
        if id == 0 || self.states.len() >= 64 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.states.insert(
            id,
            State {
                reliable: ReliableSession::with_fragment_size(
                    id,
                    self.client,
                    self.codec.fragment_size(),
                ),
                barriers: Vec::new(),
                closing: None,
            },
        );
        Ok(())
    }
    fn retire(&mut self, id: u32, retirement: Retirement) {
        if let Some(entry) = self.book.sessions.remove(&id) {
            entry.status.close(retirement.status_error());
        }
        if let Some(mut state) = self.states.remove(&id) {
            self.retired.insert(
                id,
                Tombstone {
                    since: Instant::now(),
                    sequence: state.reliable.next_send,
                },
            );
            state.complete_barriers(retirement);
        }
    }
}
pub(crate) async fn run(
    mut driver: Driver,
    mut commands: mpsc::Receiver<Command>,
    mut opens: mpsc::Receiver<OpenRequest>,
    mut dropped: mpsc::UnboundedReceiver<(u32, Arc<Status>)>,
) -> io::Result<()> {
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            packet=driver.io.receive()=> {
                let packet=packet?;
                if let Ok(segment)=driver.codec.decode(&packet) {
                    if !driver.client && segment.session_meta.as_ref().is_some_and(|m| m.protocol_type == crate::metadata::OPEN_SESSION_REQUEST) && !driver.codec.accept_open(&packet) { continue; }
                    driver.receive(segment).await?;
                }
            }
            request=opens.recv(), if driver.client=> {
                let Some(request)=request else {return Ok(());};
                if driver.states.len()>=64 {let _=request.send(Err(io::ErrorKind::WouldBlock.into()));continue;}
                driver.id=driver.id.checked_add(1).ok_or_else(||io::Error::other("mieru session IDs exhausted"))?;
                let id=driver.id;driver.start(id)?;
                driver.states.get_mut(&id).unwrap().reliable.queue_open()?;
                let stream=driver.book.create(id,Vec::new())?;
                let _=request.send(Ok(stream));
            }
            command=commands.recv(), if driver.states.values().map(|s|s.reliable.queued_len()).sum::<usize>()<960=> {
                let Some(command)=command else {return Ok(());};driver.command(command)?;
            }
            Some((id,status))=dropped.recv()=> {
                if driver.book.sessions.get(&id).is_some_and(|e|Arc::ptr_eq(&e.status,&status)) {
                    if let Some(state)=driver.states.get_mut(&id) {
                        if state.closing.is_none() {state.reliable.queue_control(crate::metadata::CLOSE_SESSION_REQUEST)?;state.closing=Some(Instant::now());}
                    }
                }
            }
            _=tick.tick()=> {driver.flush().await?;}
        }
        if driver.states.is_empty() {
            if driver.idle.elapsed() > Duration::from_secs(60) {
                return Ok(());
            }
        } else {
            driver.idle = Instant::now();
        }
    }
}
