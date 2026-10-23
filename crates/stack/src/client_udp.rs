//! Active, in-memory UDP sockets over raw IP packets for a client-side tunnel.
//! This module does not create an OS socket or own a tunnel device.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

mod construction;
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
    inner: Arc<Inner>,
    endpoint: Endpoint,
    receiver: mpsc::Receiver<QueuedEvent>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}

impl ClientUdpSocket {
    pub const fn local_endpoint(&self) -> Endpoint {
        self.endpoint
    }

    pub async fn send_to(
        &self,
        payload: &[u8],
        destination: Endpoint,
    ) -> Result<(), ClientUdpStackError> {
        if self.endpoint.ip.is_ipv4() != destination.ip.is_ipv4() {
            return Err(ClientUdpStackError::AddressFamilyMismatch);
        }
        let maximum_payload = if destination.ip.is_ipv4() {
            65_507
        } else {
            65_527
        };
        if payload.len() > maximum_payload {
            return Err(ClientUdpStackError::PayloadTooLarge);
        }
        let mtu = if let Some(socket) = self
            .inner
            .sockets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&self.endpoint)
        {
            if socket.last_destination != Some(destination) {
                socket.path_mtu = None;
                socket.last_destination = Some(destination);
            }
            socket.path_mtu.unwrap_or(self.inner.mtu)
        } else {
            return Err(ClientUdpStackError::OutboundClosed);
        };
        let packet = packet::build_udp(
            self.endpoint.ip,
            destination.ip,
            self.endpoint.port,
            destination.port,
            payload,
        );
        let identification = self.inner.next_fragment_id.fetch_add(1, Ordering::Relaxed);
        let fragments = packet::fragment_ip_packet_owned(packet, mtu, identification);
        if fragments.is_empty() {
            return Err(ClientUdpStackError::InvalidMtu);
        }
        for fragment in fragments {
            self.inner
                .outbound
                .clone()
                .with_observer(self.observer.clone())
                .send(fragment)
                .await
                .map_err(|_| {
                    observe_discard(
                        self.observer.as_ref(),
                        self.inner.drop_observer.as_ref(),
                        zero_traits::PacketDropReason::QueueClosed,
                    );
                    ClientUdpStackError::OutboundClosed
                })?;
        }
        Ok(())
    }

    pub async fn recv_event(&mut self) -> Option<ClientUdpEvent> {
        if self.inner.closed.load(Ordering::Acquire) {
            self.discard_queued();
            return None;
        }
        let event = self.receiver.recv().await;
        if self.inner.closed.load(Ordering::Acquire) {
            if event.is_some() {
                self.observe_closed_discard();
            }
            drop(event);
            self.discard_queued();
            return None;
        }
        event.map(QueuedEvent::into_event)
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
        self.inner
            .sockets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.endpoint);
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {
            observe_discard(
                self.observer.as_ref(),
                self.inner.drop_observer.as_ref(),
                zero_traits::PacketDropReason::QueueClosed,
            );
        }
    }
}
