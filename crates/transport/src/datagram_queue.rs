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
}
type Prepare = Arc<dyn Fn(&Transmit<'_>) -> io::Result<Vec<u8>> + Send + Sync>;
pub(crate) struct Socket {
    local: SocketAddr,
    prepare: Prepare,
    outgoing: mpsc::Sender<Packet>,
    incoming: Mutex<mpsc::Receiver<Packet>>,
    task: DriverStop,
    native: Option<NativeLink>,
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
    let (tx, outgoing) = mpsc::channel(256);
    let (incoming, rx) = mpsc::channel(256);
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
    });
    (
        socket,
        Channels {
            incoming,
            outgoing,
            native: Some(native),
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
    }));
    let socket = Arc::new(Socket {
        local,
        prepare,
        outgoing: tx,
        incoming: Mutex::new(rx),
        task: DriverStop::Hosted(driver.cancel.clone()),
        native: None,
    });
    Ok((socket, driver))
}
impl AsyncUdpSocket for Socket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(Poller(PollSender::new(self.outgoing.clone())))
    }
    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        let maximum = self
            .native
            .as_ref()
            .map(|link| link.lock().unwrap().maximum_transmit_segments.max(1));
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
        let bytes = (self.prepare)(transmit)?;
        permit.send(Packet {
            bytes,
            peer: transmit.destination,
            metadata: self.native.as_ref().map(|_| WireMetadata {
                ecn: transmit.ecn,
                source_ip: transmit.src_ip,
                destination_ip: None,
                segment_size: transmit.segment_size,
            }),
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
        let mut receiver = self.incoming.lock().unwrap();
        for _ in 0..32 {
            let Some(packet) = std::task::ready!(receiver.poll_recv(cx)) else {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            };
            if packet.bytes.len() > bufs[0].len() {
                continue;
            }
            bufs[0][..packet.bytes.len()].copy_from_slice(&packet.bytes);
            meta[0] = RecvMeta {
                addr: packet.peer,
                len: packet.bytes.len(),
                stride: packet.bytes.len(),
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
struct Poller(PollSender<Packet>);
impl std::fmt::Debug for Poller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DatagramTunnelPoller")
    }
}
impl UdpPoller for Poller {
    fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::task::ready!(self.0.poll_reserve(cx))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        self.0.abort_send();
        Poll::Ready(Ok(()))
    }
}

mod packet_io;
pub(crate) use packet_io::{receive, receive_native, send};
