//! Active, in-memory UDP sockets over raw IP packets for a client-side tunnel.
//! This module does not create an OS socket or own a tunnel device.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::mpsc;

use crate::packet::{self, Endpoint};
use crate::{FragmentOutcome, FragmentReassembler};

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

struct Inner {
    local_addresses: Vec<IpAddr>,
    outbound: mpsc::Sender<Vec<u8>>,
    mtu: usize,
    next_fragment_id: AtomicU32,
    sockets: Mutex<HashMap<Endpoint, mpsc::Sender<ClientUdpDatagram>>>,
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
        outbound: mpsc::Sender<Vec<u8>>,
        mtu: u16,
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
                sockets: Mutex::new(HashMap::new()),
                fragments: Mutex::new(FragmentReassembler::new()),
            }),
        })
    }

    /// Allocate a bounded receive queue and a randomized ephemeral source port.
    pub fn bind(&self, local: IpAddr) -> Result<ClientUdpSocket, ClientUdpStackError> {
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
            sockets.insert(endpoint, sender);
            return Ok(ClientUdpSocket {
                inner: Arc::clone(&self.inner),
                endpoint,
                receiver,
            });
        }
        Err(ClientUdpStackError::SocketLimit)
    }

    /// Feed an authenticated, decrypted IP packet. The caller must check that
    /// the packet source belongs to the authenticated tunnel peer first.
    /// Fragment reassembly and socket queues are bounded; excess packets drop.
    pub fn feed(&self, raw_packet: &[u8]) -> bool {
        let packet = {
            let mut fragments = self
                .inner
                .fragments
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match fragments.process(raw_packet, Instant::now()) {
                FragmentOutcome::NotFragmented(packet) => packet.to_vec(),
                FragmentOutcome::Reassembled(packet) => packet,
                FragmentOutcome::Pending | FragmentOutcome::Rejected(_) => return false,
            }
        };
        let Some(datagram) = packet::parse_udp(&packet) else {
            return false;
        };
        let sockets = self.inner.sockets.lock().unwrap_or_else(|e| e.into_inner());
        sockets.get(&datagram.dst).is_some_and(|sender| {
            sender
                .try_send(ClientUdpDatagram {
                    source: datagram.src,
                    payload: datagram.payload.to_vec(),
                })
                .is_ok()
        })
    }
}

/// A local UDP association. Dropping it immediately releases its port.
pub struct ClientUdpSocket {
    inner: Arc<Inner>,
    endpoint: Endpoint,
    receiver: mpsc::Receiver<ClientUdpDatagram>,
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
        let packet = packet::build_udp(
            self.endpoint.ip,
            destination.ip,
            self.endpoint.port,
            destination.port,
            payload,
        );
        let identification = self.inner.next_fragment_id.fetch_add(1, Ordering::Relaxed);
        let fragments = packet::fragment_ip_packet(&packet, self.inner.mtu, identification);
        if fragments.is_empty() {
            return Err(ClientUdpStackError::InvalidMtu);
        }
        for fragment in fragments {
            self.inner
                .outbound
                .send(fragment)
                .await
                .map_err(|_| ClientUdpStackError::OutboundClosed)?;
        }
        Ok(())
    }

    pub async fn recv_from(&mut self) -> Option<ClientUdpDatagram> {
        self.receiver.recv().await
    }
}

impl Drop for ClientUdpSocket {
    fn drop(&mut self) {
        self.inner
            .sockets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.endpoint);
    }
}
