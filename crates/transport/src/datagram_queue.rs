//! Bounded socket queues for stateful datagram tunnels; no endpoint task cycles.
use quinn::{
    udp::{RecvMeta, Transmit},
    AsyncUdpSocket, UdpPoller,
};
use std::{
    future::Future,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;
pub(crate) struct Packet {
    pub bytes: Vec<u8>,
    pub peer: SocketAddr,
}
pub(crate) struct Channels {
    pub incoming: mpsc::Sender<Packet>,
    pub outgoing: mpsc::Receiver<Packet>,
}
type Prepare = Arc<dyn Fn(&Transmit<'_>) -> io::Result<Vec<u8>> + Send + Sync>;
struct Socket {
    local: SocketAddr,
    prepare: Prepare,
    outgoing: mpsc::Sender<Packet>,
    incoming: Mutex<mpsc::Receiver<Packet>>,
    task: tokio::task::AbortHandle,
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.task.abort();
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
    let (tx, outgoing) = mpsc::channel(256);
    let (incoming, rx) = mpsc::channel(256);
    let future = run(Channels { incoming, outgoing });
    let task = tokio::spawn(async move {
        if let Err(error) = future.await {
            tracing::debug!(%error,"datagram tunnel stopped");
        }
    })
    .abort_handle();
    Ok(Arc::new(Socket {
        local,
        prepare,
        outgoing: tx,
        incoming: Mutex::new(rx),
        task,
    }))
}
impl AsyncUdpSocket for Socket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(Poller(PollSender::new(self.outgoing.clone())))
    }
    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        if transmit.segment_size.is_some() || transmit.contents.len() > 65507 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tunnel requires a bounded individual datagram",
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
                ecn: None,
                dst_ip: None,
            };
            return Poll::Ready(Ok(1));
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
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
pub(crate) async fn receive(socket: &dyn AsyncUdpSocket) -> io::Result<Vec<Packet>> {
    let mut bytes = vec![0; 65536];
    let mut meta = [RecvMeta::default()];
    let count = std::future::poll_fn(|cx| {
        socket.poll_recv(cx, &mut [IoSliceMut::new(&mut bytes)], &mut meta)
    })
    .await?;
    if count == 0 || meta[0].stride == 0 || meta[0].len > bytes.len() {
        return Ok(Vec::new());
    }
    Ok(bytes[..meta[0].len]
        .chunks(meta[0].stride)
        .map(|p| Packet {
            bytes: p.to_vec(),
            peer: meta[0].addr,
        })
        .collect())
}
pub(crate) async fn send(socket: &Arc<dyn AsyncUdpSocket>, packet: Packet) -> io::Result<()> {
    let transmit = Transmit {
        contents: &packet.bytes,
        destination: packet.peer,
        ecn: None,
        segment_size: None,
        src_ip: None,
    };
    let mut poller = socket.clone().create_io_poller();
    loop {
        match socket.try_send(&transmit) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::future::poll_fn(|cx| poller.as_mut().poll_writable(cx)).await?
            }
            result => return result,
        }
    }
}
