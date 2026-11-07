//! Bounded socket queues for stateful datagram tunnels; no endpoint task cycles.
use crate::datagram_driver::{DriverStop, PreparedIoDriver};
use quinn::{
    udp::{RecvMeta, Transmit},
    AsyncUdpSocket, UdpPoller,
};
use std::{
    future::Future,
    io::{self, IoSliceMut},
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;
pub(crate) struct Packet {
    pub bytes: Vec<u8>,
    pub peer: SocketAddr,
    pub metadata: Option<WireMetadata>,
    // Backing fields above must be destroyed before this original frame credit.
    pub _credit: Option<zero_platform_tokio::controlled_carrier::CarrierBufferFrameCredit>,
}
impl Drop for Packet {
    fn drop(&mut self) {
        // Also prohibit implicit field moves which could leave a live backing
        // after its original ticket has returned to the working lane.
        drop(std::mem::take(&mut self.bytes));
    }
}
impl Packet {
    // Existing unbudgeted server queues transfer a Vec. Budgeted native/loan
    // packets keep this owning wrapper through send/dequeue instead.
    pub fn into_unbudgeted_bytes(mut self) -> Vec<u8> {
        assert!(
            self._credit.is_none(),
            "original frame backing must keep its ticket"
        );
        std::mem::take(&mut self.bytes)
    }
}
#[derive(Clone, Copy)]
pub(crate) struct WireMetadata {
    pub ecn: Option<quinn::udp::EcnCodepoint>,
    pub source_ip: Option<IpAddr>,
    pub destination_ip: Option<IpAddr>,
    pub segment_size: Option<usize>,
}
#[derive(Default)]
pub(crate) struct NativeCapabilities {
    pub current: Option<zero_platform_tokio::controlled_datagram::metadata::MetadataCapabilities>,
    pub maximum_transmit_segments: usize,
}
pub(crate) type NativeLink = Arc<Mutex<NativeCapabilities>>;
pub(crate) struct Channels {
    pub incoming: mpsc::Sender<Packet>,
    pub outgoing: mpsc::Receiver<Packet>,
    pub native: Option<NativeLink>,
    pub memory: Option<memory::Memory>,
}
type Prepare = Arc<dyn Fn(&Transmit<'_>) -> io::Result<Vec<u8>> + Send + Sync>;
pub(crate) struct Socket {
    local: SocketAddr,
    prepare: Prepare,
    outgoing: mpsc::Sender<Packet>,
    incoming: Mutex<mpsc::Receiver<Packet>>,
    task: DriverStop,
    native: Option<NativeLink>,
    memory: Option<Arc<memory::Shared>>,
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.task.request();
    }
}
impl std::fmt::Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DatagramTunnelSocket")
    }
}
pub(crate) fn spawn<F, Fut>(
    inner: Arc<dyn AsyncUdpSocket>,
    prepare: Prepare,
    run: F,
) -> io::Result<Arc<dyn AsyncUdpSocket>>
where
    F: FnOnce(Arc<dyn AsyncUdpSocket>, Channels) -> Fut,
    Fut: Future<Output = io::Result<()>> + Send + 'static,
{
    let local = inner.local_addr()?;
    spawn_at(inner, local, prepare, run)
}
pub(crate) fn spawn_at<F, Fut>(
    inner: Arc<dyn AsyncUdpSocket>,
    local: SocketAddr,
    prepare: Prepare,
    run: F,
) -> io::Result<Arc<dyn AsyncUdpSocket>>
where
    F: FnOnce(Arc<dyn AsyncUdpSocket>, Channels) -> Fut,
    Fut: Future<Output = io::Result<()>> + Send + 'static,
{
    spawn_with(local, prepare, move |channels| run(inner, channels))
}
pub(crate) fn spawn_with<F, Fut>(
    local: SocketAddr,
    prepare: Prepare,
    run: F,
) -> io::Result<Arc<dyn AsyncUdpSocket>>
where
    F: FnOnce(Channels) -> Fut,
    Fut: Future<Output = io::Result<()>> + Send + 'static,
{
    let (mut socket, driver) = prepare_with(local, prepare, run)?;
    let task = tokio::spawn(async move {
        if let Err(error) = driver.future.await {
            tracing::debug!(%error,"datagram tunnel stopped");
        }
    })
    .abort_handle();
    Arc::get_mut(&mut socket)
        .expect("unpublished inert socket")
        .task = DriverStop::Legacy(task);
    Ok(socket)
}

pub(crate) fn prepare_channels(local: SocketAddr, prepare: Prepare) -> (Arc<Socket>, Channels) {
    prepare_channels_inner(local, prepare, None)
}
pub(crate) fn prepare_budgeted_channels(
    local: SocketAddr,
    logical: SocketAddr,
    memory: memory::Memory,
) -> (Arc<Socket>, Channels) {
    let shared = memory.shared.clone();
    prepare_channels_inner(
        local,
        Arc::new(move |packet| {
            if packet.destination != logical {
                return Err(io::ErrorKind::InvalidInput.into());
            }
            shared.encode(packet)
        }),
        Some(memory),
    )
}
fn prepare_channels_inner(
    local: SocketAddr,
    prepare: Prepare,
    memory: Option<memory::Memory>,
) -> (Arc<Socket>, Channels) {
    let capacity = memory
        .as_ref()
        .map_or(256, |memory| memory.shared.book.limits().max_queue_packets);
    let (tx, outgoing) = mpsc::channel(capacity);
    let (incoming, rx) = mpsc::channel(capacity);
    let native = Arc::new(Mutex::new(NativeCapabilities {
        current: None,
        maximum_transmit_segments: 1,
    }));
    let socket = Arc::new(Socket {
        local,
        prepare,
        outgoing: tx,
        incoming: Mutex::new(rx),
        task: DriverStop::Hosted(zero_platform_tokio::contract::CancellationToken::new()),
        native: Some(native.clone()),
        memory: memory.as_ref().map(|memory| memory.shared.clone()),
    });
    (
        socket,
        Channels {
            incoming,
            outgoing,
            native: Some(native),
            memory,
        },
    )
}
pub(crate) fn prepare_with<F, Fut>(
    local: SocketAddr,
    prepare: Prepare,
    run: F,
) -> io::Result<(Arc<Socket>, PreparedIoDriver)>
where
    F: FnOnce(Channels) -> Fut,
    Fut: Future<Output = io::Result<()>> + Send + 'static,
{
    let (tx, outgoing) = mpsc::channel(256);
    let (incoming, rx) = mpsc::channel(256);
    let driver = PreparedIoDriver::new(run(Channels {
        incoming,
        outgoing,
        native: None,
        memory: None,
    }));
    let socket = Arc::new(Socket {
        local,
        prepare,
        outgoing: tx,
        incoming: Mutex::new(rx),
        task: DriverStop::Hosted(driver.cancel.clone()),
        native: None,
        memory: None,
    });
    Ok((socket, driver))
}
impl AsyncUdpSocket for Socket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(Poller {
            outgoing: PollSender::new(self.outgoing.clone()),
            memory: self.memory.clone(),
            waiting: Mutex::new(None),
            waiting_bytes: 0,
        })
    }
    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        let maximum = self
            .native
            .as_ref()
            .map(|link| link.lock().unwrap().maximum_transmit_segments.max(1));
        let maximum = if self.memory.as_ref().is_some_and(|memory| memory.masked()) {
            Some(1)
        } else {
            maximum
        };
        let valid = match (maximum, transmit.segment_size) {
            (Some(maximum), Some(size)) => {
                size > 0 && size <= 65507 && transmit.contents.len().div_ceil(size) <= maximum
            }
            (_, None) => transmit.contents.len() <= 65507,
            (None, Some(_)) => false,
        };
        if !valid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP queue requires a bounded transmit",
            ));
        }
        let permit = self.outgoing.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => io::ErrorKind::WouldBlock,
            mpsc::error::TrySendError::Closed(_) => io::ErrorKind::BrokenPipe,
        })?;
        // Prepay before the codec or original byte copy can allocate.
        let credit = self
            .memory
            .as_ref()
            .map(|memory| memory.reserve_transmit(transmit.contents.len()))
            .transpose()?;
        let bytes = (self.prepare)(transmit)?;
        if self.memory.as_ref().is_some_and(|memory| memory.masked())
            && bytes.len() > memory::MAX_DATAGRAM_BYTES
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        permit.send(Packet {
            bytes,
            peer: transmit.destination,
            metadata: self.native.as_ref().map(|_| WireMetadata {
                ecn: transmit.ecn,
                source_ip: transmit.src_ip,
                destination_ip: None,
                segment_size: transmit.segment_size,
            }),
            _credit: credit,
        });
        Ok(())
    }
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }
        for _ in 0..32 {
            // A frame refund can wake an arbitrary original writer. Never
            // destroy its backing/credit under this queue receiver mutex.
            let packet = {
                let mut receiver = self.incoming.lock().unwrap();
                std::task::ready!(receiver.poll_recv(cx))
            };
            let Some(packet) = packet else {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            };
            let decoded = match &self.memory {
                Some(memory) => match memory.decode(&packet.bytes) {
                    Ok(decoded) => decoded,
                    Err(_) => continue,
                },
                None => None,
            };
            let bytes = decoded.as_deref().unwrap_or(&packet.bytes);
            if bytes.len() > bufs[0].len() {
                continue;
            }
            bufs[0][..bytes.len()].copy_from_slice(bytes);
            meta[0] = RecvMeta {
                addr: packet.peer,
                len: bytes.len(),
                stride: bytes.len(),
                ecn: packet.metadata.and_then(|metadata| metadata.ecn),
                dst_ip: packet.metadata.and_then(|metadata| metadata.destination_ip),
            };
            return Poll::Ready(Ok(1));
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }
    fn max_transmit_segments(&self) -> usize {
        if self.memory.as_ref().is_some_and(|memory| memory.masked()) {
            return 1;
        }
        self.native
            .as_ref()
            .and_then(|link| link.lock().unwrap().current)
            .map_or(1, |caps| caps.transmit_segments.max(1))
    }
    fn max_receive_segments(&self) -> usize {
        // The actual native worker consumes GRO and retains each complete
        // datagram with its original ECN/destination metadata in this FIFO.
        1
    }
    fn may_fragment(&self) -> bool {
        self.native
            .as_ref()
            .and_then(|link| link.lock().unwrap().current)
            .is_none_or(|caps| caps.may_fragment)
    }
}
struct Poller {
    outgoing: PollSender<Packet>,
    memory: Option<Arc<memory::Shared>>,
    waiting: Mutex<
        Option<
            Pin<
                Box<
                    dyn Future<
                            Output = io::Result<
                                zero_platform_tokio::controlled_carrier::CarrierBufferFrameCredit,
                            >,
                        > + Send,
                >,
            >,
        >,
    >,
    waiting_bytes: usize,
}
impl std::fmt::Debug for Poller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DatagramTunnelPoller")
    }
}
impl UdpPoller for Poller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        std::task::ready!(this.outgoing.poll_reserve(cx))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        this.outgoing.abort_send();
        let Some(memory) = this.memory.clone() else {
            return Poll::Ready(Ok(()));
        };
        if memory
            .ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            return Poll::Ready(Ok(()));
        }
        let bytes = memory.needed.load(std::sync::atomic::Ordering::Acquire);
        // Exclusive poller access; no mutex guard surrounds a foreign poll.
        let waiting = this
            .waiting
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if waiting.is_none() || this.waiting_bytes != bytes {
            *waiting = Some(Box::pin(memory.transmit.reserve(bytes)));
            this.waiting_bytes = bytes;
        }
        let credit = std::task::ready!(waiting.as_mut().unwrap().as_mut().poll(cx))?;
        *waiting = None;
        let unused = {
            let mut ready = memory
                .ready
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if ready.is_none() {
                *ready = Some(credit);
                None
            } else {
                Some(credit)
            }
        };
        drop(unused);
        Poll::Ready(Ok(()))
    }
}

mod packet_io;
pub(crate) use packet_io::{receive, receive_budgeted, receive_native, send};
pub(crate) mod memory;

#[cfg(test)]
mod memory_tests;
