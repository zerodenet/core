//! Protocol-owned shared client sessions. The owning adapter supplies carriers.
mod datagram;
mod pool;
mod tcp;

use crate::inbound::multiplex::stream::MieruLogicalStream;
pub use datagram::ClientDatagramCarrier;
pub(crate) use datagram::UdpClientDatagramCarrier;
pub use pool::{ClientPool, ClientPoolPolicy, ClientPoolSnapshot, PoolKey};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};

pub(crate) type OpenRequest = oneshot::Sender<io::Result<MieruLogicalStream>>;
pub struct ClientConnection {
    first: Mutex<Option<MieruLogicalStream>>,
    opens: mpsc::Sender<OpenRequest>,
    task: tokio::task::JoinHandle<()>,
    active_streams: AtomicUsize,
    pending_opens: AtomicUsize,
    max_streams: usize,
    created_at: Instant,
}

struct ConnectionLease {
    connection: Arc<ClientConnection>,
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        self.connection
            .active_streams
            .fetch_sub(1, Ordering::AcqRel);
    }
}

struct PendingOpen<'a>(&'a AtomicUsize);

impl Drop for PendingOpen<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
impl Drop for ClientConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl ClientConnection {
    pub fn is_closed(&self) -> bool {
        self.task.is_finished() || self.opens.is_closed()
    }
    pub(crate) fn active_streams(&self) -> usize {
        self.active_streams.load(Ordering::Acquire)
    }
    pub(crate) fn pending_opens(&self) -> usize {
        self.pending_opens.load(Ordering::Acquire)
    }
    pub(crate) fn load(&self) -> usize {
        self.active_streams().saturating_add(self.pending_opens())
    }
    pub(crate) fn max_streams(&self) -> usize {
        self.max_streams
    }
    pub(crate) fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }
    pub async fn open(self: &Arc<Self>) -> io::Result<MieruLogicalStream> {
        self.pending_opens.fetch_add(1, Ordering::AcqRel);
        let pending = PendingOpen(&self.pending_opens);
        let mut stream = self.next().await?;
        drop(pending);
        self.active_streams.fetch_add(1, Ordering::AcqRel);
        stream.keep_alive(Arc::new(ConnectionLease {
            connection: self.clone(),
        }));
        Ok(stream)
    }
    async fn next(&self) -> io::Result<MieruLogicalStream> {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let first = self.first.lock().unwrap().take();
            let stream = if let Some(stream) = first {
                stream
            } else {
                let (send, receive) = oneshot::channel();
                self.opens
                    .send(send)
                    .await
                    .map_err(|_| io::ErrorKind::BrokenPipe)?;
                receive.await.map_err(|_| io::ErrorKind::BrokenPipe)??
            };
            stream.status.wait_open().await?;
            Ok(stream)
        })
        .await
        .map_err(|_| io::ErrorKind::TimedOut)?
    }
}

impl ClientConnection {
    pub async fn udp(
        socket: Arc<tokio::net::UdpSocket>,
        peer: std::net::SocketAddr,
        username: &str,
        password: &str,
    ) -> io::Result<Arc<Self>> {
        Self::udp_with_options(socket, peer, username, password, &Default::default()).await
    }
    pub async fn udp_with_options(
        socket: Arc<tokio::net::UdpSocket>,
        peer: std::net::SocketAddr,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
    ) -> io::Result<Arc<Self>> {
        Self::datagram_with_options(
            Arc::new(UdpClientDatagramCarrier::new(socket, peer)),
            peer.is_ipv6(),
            username,
            password,
            options,
        )
        .await
    }

    pub async fn datagram_with_options(
        carrier: Arc<dyn ClientDatagramCarrier>,
        conservative_ipv6_budget: bool,
        username: &str,
        password: &str,
        options: &mieru_config::MieruTransportOptions,
    ) -> io::Result<Arc<Self>> {
        options.validate().map_err(io::Error::other)?;
        use crate::{
            inbound::multiplex::reader::Reader,
            packet::{Driver, PacketCodec, PacketIo},
        };
        let (ready, _unused) = mpsc::channel(1);
        let (outgoing, commands) = mpsc::channel(64);
        let (dropped, drop_events) = mpsc::unbounded_channel();
        let (opens, requests) = mpsc::channel(64);
        let key = crate::crypto::derive_key(
            username,
            password,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(io::Error::other)?
                .as_secs(),
        );
        let book = Reader::new(ready, outgoing, dropped, options.receive.clone());
        let driver = Driver::new(
            book,
            PacketCodec::configured(key, username, options, conservative_ipv6_budget)?,
            PacketIo::Client { carrier },
            true,
        );
        let task = tokio::spawn(async move {
            let _ = crate::packet::run(driver, commands, requests, drop_events).await;
        });
        let connection = Arc::new(Self {
            first: Mutex::new(None),
            opens,
            task,
            active_streams: AtomicUsize::new(0),
            pending_opens: AtomicUsize::new(0),
            max_streams: 64,
            created_at: Instant::now(),
        });
        let stream = connection.next().await?;
        *connection.first.lock().unwrap() = Some(stream);
        Ok(connection)
    }
}
