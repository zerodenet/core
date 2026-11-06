//! Active, in-memory UDP sockets over raw IP packets for a client-side tunnel.
//! This module does not create an OS socket or own a tunnel device.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

mod construction;
mod quiescence;
mod sender;
use sender::SocketState;
pub use sender::{ClientUdpCloseReceipt, ClientUdpSendError, ClientUdpSender};
mod maintenance;
mod observation;
mod queue;
mod receive;
use crate::packet::{self, Endpoint};
use crate::FragmentReassembler;
use observation::observe_discard;
pub use queue::ClientUdpReceiveQueueConfig;
use queue::{ByteBudget, QueueBudget, QueuedEvent};

const FIRST_EPHEMERAL_PORT: u16 = 49_152;
const EPHEMERAL_PORT_COUNT: usize = 16_384;
const MAX_SOCKETS: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientUdpStackError {
    MissingLocalAddress,
    DuplicateLocalAddress,
    InvalidMtu,
    InvalidReceiveQueue,
    UnknownLocalAddress,
    SocketLimit,
    AddressFamilyMismatch,
    PayloadTooLarge,
    OutboundClosed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientUdpDatagram {
    pub source: Endpoint,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientUdpEvent {
    Datagram(ClientUdpDatagram),
    IcmpError(packet::ParsedIcmpError),
}

struct SocketEntry {
    state: Arc<SocketState>,
    sender: mpsc::Sender<QueuedEvent>,
    budget: QueueBudget,
    last_destination: Option<Endpoint>,
    path_mtu: Option<usize>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}

struct Inner {
    local_addresses: Vec<IpAddr>,
    outbound: crate::packet_output::PacketSender,
    mtu: usize,
    next_fragment_id: AtomicU32,
    closed: AtomicBool,
    drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
    receive_queue: ClientUdpReceiveQueueConfig,
    receive_budget: Option<Arc<ByteBudget>>,
    sockets: Mutex<HashMap<Endpoint, SocketEntry>>,
    fragments: Mutex<FragmentReassembler>,
}

/// Client-side UDP stack. Encrypted tunnel integration supplies and consumes
/// complete raw IP packets through the bounded outbound channel and `feed`.
#[derive(Clone)]
pub struct ClientUdpStack {
    inner: Arc<Inner>,
}

/// A local UDP association. Dropping it immediately releases its port.
pub struct ClientUdpSocket {
    state: Arc<SocketState>,
    inner: Arc<Inner>,
    endpoint: Endpoint,
    receiver: mpsc::Receiver<QueuedEvent>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}

impl ClientUdpSocket {
    pub const fn local_endpoint(&self) -> Endpoint {
        self.endpoint
    }

    /// Send-only view of this exact logical socket registration. It cannot
    /// receive, remove the original entry, or manufacture a native close.
    pub fn sender(&self) -> ClientUdpSender {
        ClientUdpSender::new(
            self.inner.clone(),
            self.endpoint,
            self.observer.clone(),
            self.state.clone(),
        )
    }
    pub async fn send_to(
        &self,
        payload: &[u8],
        destination: Endpoint,
    ) -> Result<(), ClientUdpStackError> {
        self.sender()
            .send_to(payload, destination)
            .await
            .map_err(|failure| failure.kind)
    }
    /// Retire this same logical registration and drain its unique receive view.
    /// Pending output futures are fenced and must still be driven/returned;
    /// neither this logical receipt nor Drop proves a native or Scope close.
    pub fn close(&mut self) -> ClientUdpCloseReceipt {
        self.state.stop();
        let mut entries = self.inner.sockets.lock().unwrap_or_else(|e| e.into_inner());
        let retired = if entries
            .get(&self.endpoint)
            .is_some_and(|entry| Arc::ptr_eq(&entry.state, &self.state))
        {
            entries.remove(&self.endpoint)
        } else {
            None
        };
        drop(entries);
        drop(retired);
        self.discard_queued();
        ClientUdpCloseReceipt::new(self.state.clone(), self.endpoint)
    }
    pub async fn recv_event(&mut self) -> Option<ClientUdpEvent> {
        std::future::poll_fn(|cx| self.poll_recv_event(cx)).await
    }
    pub fn poll_recv_event(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<ClientUdpEvent>> {
        use std::task::Poll;
        if self.inner.closed.load(Ordering::Acquire) || self.state.closed() {
            self.discard_queued();
            return Poll::Ready(None);
        }
        match self.receiver.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(event) => {
                if self.inner.closed.load(Ordering::Acquire) || self.state.closed() {
                    if event.is_some() {
                        self.observe_closed_discard();
                    }
                    drop(event);
                    self.discard_queued();
                    Poll::Ready(None)
                } else {
                    Poll::Ready(event.map(QueuedEvent::into_event))
                }
            }
        }
    }
    fn observe_closed_discard(&self) {
        observe_discard(
            self.observer.as_ref(),
            self.inner.drop_observer.as_ref(),
            zero_traits::PacketDropReason::QueueClosed,
        );
    }
    fn discard_queued(&mut self) {
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {
            self.observe_closed_discard();
        }
    }

    /// Receive only payloads; callers that need network errors use
    /// `recv_event` so ICMP cannot be silently mistaken for a UDP response.
    pub async fn recv_from(&mut self) -> Option<ClientUdpDatagram> {
        loop {
            match self.recv_event().await? {
                ClientUdpEvent::Datagram(datagram) => return Some(datagram),
                ClientUdpEvent::IcmpError(_) => {}
            }
        }
    }
}

impl Drop for ClientUdpSocket {
    fn drop(&mut self) {
        // Compatibility callers request the same logical retirement. Only an
        // explicit controller retaining the receipt can confirm quiescence.
        let _ = self.close();
    }
}
