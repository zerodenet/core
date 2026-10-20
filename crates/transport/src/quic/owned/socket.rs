use std::{
    fmt, io,
    sync::{Arc, Mutex},
};
use zero_platform_tokio::controlled_datagram::metadata::{
    GrantedMetadataDatagram, MetadataDatagramControl, MetadataDatagramRx, MetadataDatagramTx,
    MetadataOwner, MetadataOwnerReport,
};
mod poller;
mod receive;
mod send;
use send::NativeDrops;

/// Actual native ownership, separate from endpoint/connection task and user
/// ownership. No Drop path reports native closure.
pub struct MetadataSocketOwner {
    owner: MetadataOwner,
    control: MetadataDatagramControl,
    socket: Arc<MetadataSocket>,
}
/// Failed composition returns both original owners/views to its caller.
pub struct RejectedMetadataSocket {
    pub error: io::Error,
    pub owner: MetadataOwner,
    pub grant: GrantedMetadataDatagram,
}
impl MetadataSocketOwner {
    pub fn new(
        owner: MetadataOwner,
        grant: GrantedMetadataDatagram,
    ) -> Result<Self, Box<RejectedMetadataSocket>> {
        if !owner.owns_grant(&grant) {
            return Err(Box::new(RejectedMetadataSocket {
                error: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "metadata views belong to another original owner",
                ),
                owner,
                grant,
            }));
        }
        let GrantedMetadataDatagram { rx, tx, control } = grant;
        Ok(Self {
            owner,
            control,
            socket: Arc::new(MetadataSocket {
                receiver: Mutex::new(Some(rx)),
                sender: tx,
                drops: Mutex::new(NativeDrops::default()),
            }),
        })
    }
    /// Only an abstract metadata socket escapes, never the original FD, RX or
    /// native control. A host must separately own every Quinn driver and user.
    pub fn socket(&self) -> Arc<dyn quinn::AsyncUdpSocket> {
        self.socket.clone()
    }
    pub fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<()> {
        self.control.set_observer(observer)
    }
    pub fn revoke_io(&self) {
        self.control.revoke();
    }
    /// Actual native receipts only. This does not prove any Quinn task/user exit.
    pub fn close_io(&mut self) -> MetadataOwnerReport {
        self.owner.close_all()
    }
    /// Bounded diagnostics for the pinned helper's nonfatal native-send policy.
    /// This is not a complete history of error sources or submitted bytes.
    pub fn native_send_drops(&self) -> NativeSendDrops {
        let state = self.socket.drops.lock().unwrap();
        NativeSendDrops {
            count: state.count,
            overflowed: state.overflowed,
            first: state.first.clone(),
        }
    }
}
/// The original first native-send source and an aggregate diagnostic count.
#[derive(Clone, Debug)]
pub struct NativeSendDrops {
    pub count: u64,
    pub overflowed: bool,
    pub first: Option<Arc<io::Error>>,
}
struct MetadataSocket {
    receiver: Mutex<Option<MetadataDatagramRx>>,
    sender: MetadataDatagramTx,
    drops: Mutex<NativeDrops>,
}
impl fmt::Debug for MetadataSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataSocket").finish_non_exhaustive()
    }
}
impl fmt::Debug for RejectedMetadataSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RejectedMetadataSocket")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl quinn::AsyncUdpSocket for MetadataSocket {
    fn create_io_poller(self: Arc<Self>) -> std::pin::Pin<Box<dyn quinn::UdpPoller>> {
        Box::pin(poller::Poller::new(self.sender.create_write_poller()))
    }
    fn try_send(&self, transmit: &quinn::udp::Transmit<'_>) -> io::Result<()> {
        self.send(transmit)
    }
    fn poll_recv(
        &self,
        cx: &mut std::task::Context<'_>,
        buffers: &mut [io::IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> std::task::Poll<io::Result<usize>> {
        self.receive(cx, buffers, meta)
    }
    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.sender.local_addr()
    }
    fn max_transmit_segments(&self) -> usize {
        self.sender.capabilities().transmit_segments
    }
    fn max_receive_segments(&self) -> usize {
        self.sender.capabilities().receive_segments
    }
    fn may_fragment(&self) -> bool {
        self.sender.capabilities().may_fragment
    }
}
