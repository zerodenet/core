//! Active, in-memory UDP sockets over raw IP packets for a client-side tunnel.
//! This module does not create an OS socket or own a tunnel device.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

mod observation;
mod receive;
use crate::packet::{self, Endpoint};
use crate::FragmentReassembler;
use observation::observe_discard;

const FIRST_EPHEMERAL_PORT: u16 = 49_152;
const EPHEMERAL_PORT_COUNT: usize = 16_384;
const MAX_SOCKETS: usize = 1_024;
const RECEIVE_QUEUE_CAPACITY: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientUdpStackError {
    MissingLocalAddress,
    DuplicateLocalAddress,
    InvalidMtu,
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
    sender: mpsc::Sender<ClientUdpEvent>,
    last_destination: Option<Endpoint>,
    path_mtu: Option<usize>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}

struct Inner {
    local_addresses: Vec<IpAddr>,
    outbound: crate::packet_output::PacketSender,
    mtu: usize,
    next_fragment_id: AtomicU32,
    drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
    sockets: Mutex<HashMap<Endpoint, SocketEntry>>,
    fragments: Mutex<FragmentReassembler>,
}

/// Client-side UDP stack. Encrypted tunnel integration supplies and consumes
/// complete raw IP packets through the bounded outbound channel and `feed`.
#[derive(Clone)]
pub struct ClientUdpStack {
    inner: Arc<Inner>,
}

impl ClientUdpStack {
    pub fn new(
        local_addresses: Vec<IpAddr>,
        outbound: tokio::sync::mpsc::Sender<Vec<u8>>,
        mtu: u16,
    ) -> Result<Self, ClientUdpStackError> {
        Self::new_with_output(local_addresses, outbound.into(), mtu, None)
    }
    pub fn new_observed(
        local_addresses: Vec<IpAddr>,
        outbound: tokio::sync::mpsc::Sender<crate::packet_output::ObservedPacket>,
        mtu: u16,
    ) -> Result<Self, ClientUdpStackError> {
        Self::new_with_output(local_addresses, outbound.into(), mtu, None)
    }
    pub fn new_observed_with_drops(
        local_addresses: Vec<IpAddr>,
        outbound: mpsc::Sender<crate::packet_output::ObservedPacket>,
        mtu: u16,
        drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<Self, ClientUdpStackError> {
        Self::new_with_output(local_addresses, outbound.into(), mtu, drop_observer)
    }
    fn new_with_output(
        local_addresses: Vec<IpAddr>,
        outbound: crate::packet_output::PacketSender,
        mtu: u16,
        drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<Self, ClientUdpStackError> {
        if local_addresses.is_empty() {
            return Err(ClientUdpStackError::MissingLocalAddress);
        }
        if local_addresses
            .iter()
            .enumerate()
            .any(|(index, address)| local_addresses[..index].contains(address))
        {
            return Err(ClientUdpStackError::DuplicateLocalAddress);
        }
        if mtu < 68 || (local_addresses.iter().any(IpAddr::is_ipv6) && mtu < 1_280) {
            return Err(ClientUdpStackError::InvalidMtu);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                local_addresses,
                outbound,
                mtu: usize::from(mtu),
                next_fragment_id: AtomicU32::new(1),
                drop_observer,
                sockets: Mutex::new(HashMap::new()),
                fragments: Mutex::new(FragmentReassembler::new()),
            }),
        })
    }

    /// Allocate a bounded receive queue and a randomized ephemeral source port.
    pub fn bind(&self, local: IpAddr) -> Result<ClientUdpSocket, ClientUdpStackError> {
        self.bind_observed(local, None)
    }

    pub fn bind_observed(
        &self,
        local: IpAddr,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<ClientUdpSocket, ClientUdpStackError> {
        if !self.inner.local_addresses.contains(&local) {
            return Err(ClientUdpStackError::UnknownLocalAddress);
        }
        let mut sockets = self.inner.sockets.lock().unwrap_or_else(|e| e.into_inner());
        if sockets.len() >= MAX_SOCKETS {
            return Err(ClientUdpStackError::SocketLimit);
        }
        let start = usize::from(rand::random_range(0..EPHEMERAL_PORT_COUNT as u16));
        for offset in 0..EPHEMERAL_PORT_COUNT {
            let index = (start + offset) % EPHEMERAL_PORT_COUNT;
            let endpoint = Endpoint {
                ip: local,
                port: FIRST_EPHEMERAL_PORT + index as u16,
            };
            if sockets.contains_key(&endpoint) {
                continue;
            }
            let (sender, receiver) = mpsc::channel(RECEIVE_QUEUE_CAPACITY);
            sockets.insert(
                endpoint,
                SocketEntry {
                    sender,
                    last_destination: None,
                    path_mtu: None,
                    observer: observer.clone(),
                },
            );
            return Ok(ClientUdpSocket {
                inner: Arc::clone(&self.inner),
                endpoint,
                receiver,
                observer,
            });
        }
        Err(ClientUdpStackError::SocketLimit)
    }
}

/// A local UDP association. Dropping it immediately releases its port.
pub struct ClientUdpSocket {
    inner: Arc<Inner>,
    endpoint: Endpoint,
    receiver: mpsc::Receiver<ClientUdpEvent>,
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
        let fragments = packet::fragment_ip_packet(&packet, mtu, identification);
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
        self.receiver.recv().await
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
