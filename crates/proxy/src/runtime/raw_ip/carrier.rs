//! Wire datagram I/O for a raw-IP peer device.

use std::{
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use zero_core::Address;
use zero_platform_tokio::TokioDatagramSocket;

use crate::protocol_registry::PacketPathExecutionServices;
use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;
use crate::runtime::udp_flow::packet_path::PacketPathCarrier;

#[async_trait]
pub(crate) trait RawIpWireCarrier: Send + Sync {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, Option<SocketAddr>)>;
    async fn send(&self, packet: &[u8], endpoint: SocketAddr) -> io::Result<()>;
}

pub(crate) struct DirectRawIpWireCarrier(pub(crate) TokioDatagramSocket);

#[async_trait]
impl RawIpWireCarrier for DirectRawIpWireCarrier {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, Option<SocketAddr>)> {
        self.0
            .recv_from_addr(buf)
            .await
            .map(|(size, source)| (size, Some(source)))
    }

    async fn send(&self, packet: &[u8], endpoint: SocketAddr) -> io::Result<()> {
        self.0.send_to_addr(packet, endpoint).await.map(|_| ())
    }
}

/// An upstream carrier serves one configured peer. Address-bearing relays
/// report the response source. Opaque transports must not present the
/// configured endpoint as an observed source.
pub(crate) struct ProxiedRawIpWireCarrier {
    state: Arc<ProxiedCarrierState>,
    sent_once: AtomicBool,
}

struct ProxiedCarrierState {
    path: tokio::sync::watch::Sender<Arc<dyn PacketPathCarrier>>,
    operation: Arc<dyn PreparedUdpPacketPathOperation>,
    services: PacketPathExecutionServices,
    rebuilding: AtomicBool,
    closing: tokio::sync::watch::Sender<bool>,
}

impl ProxiedRawIpWireCarrier {
    pub(crate) fn new(
        path: Arc<dyn PacketPathCarrier>,
        operation: Arc<dyn PreparedUdpPacketPathOperation>,
        services: PacketPathExecutionServices,
    ) -> Self {
        let (path, _) = tokio::sync::watch::channel(path);
        let (closing, _) = tokio::sync::watch::channel(false);
        Self {
            state: Arc::new(ProxiedCarrierState {
                path,
                operation,
                services,
                rebuilding: AtomicBool::new(false),
                closing,
            }),
            sent_once: AtomicBool::new(false),
        }
    }
}

impl Drop for ProxiedRawIpWireCarrier {
    fn drop(&mut self) {
        self.state.closing.send_replace(true);
    }
}

impl ProxiedCarrierState {
    fn rebuild_if_current(self: &Arc<Self>, failed: Arc<dyn PacketPathCarrier>) {
        if !Arc::ptr_eq(&failed, &*self.path.borrow())
            || self.rebuilding.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let state = Arc::clone(self);
        tokio::spawn(async move {
            state.rebuild(failed).await;
            state.rebuilding.store(false, Ordering::Release);
        });
    }

    async fn rebuild(&self, failed: Arc<dyn PacketPathCarrier>) {
        let mut closing = self.closing.subscribe();
        let mut backoff = Duration::from_secs(1);
        loop {
            if *closing.borrow() || !Arc::ptr_eq(&failed, &*self.path.borrow()) {
                return;
            }
            let built = tokio::select! {
                result = self.operation.build_carrier(self.services.clone()) => result,
                _ = closing.changed() => return,
            };
            match built {
                Ok(next) => {
                    self.path.send_replace(next);
                    return;
                }
                Err(error) => {
                    tracing::warn!(%error, "raw-IP outer packet path reconnect failed");
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {},
                _ = closing.changed() => return,
            }
            backoff = (backoff * 2).min(Duration::from_secs(15));
        }
    }
}

#[async_trait]
impl RawIpWireCarrier for ProxiedRawIpWireCarrier {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, Option<SocketAddr>)> {
        let mut changes = self.state.path.subscribe();
        loop {
            let path = changes.borrow_and_update().clone();
            match path.recv_from_with_source(buf).await {
                Ok((size, source)) => return Ok((size, source)),
                Err(error) => {
                    tracing::warn!(%error, "raw-IP outer packet path receive failed");
                    self.state.rebuild_if_current(path.clone());
                    while Arc::ptr_eq(&path, &*changes.borrow()) {
                        changes.changed().await.map_err(|_| {
                            io::Error::new(io::ErrorKind::BrokenPipe, "outer packet path closed")
                        })?;
                    }
                }
            }
        }
    }

    async fn send(&self, packet: &[u8], endpoint: SocketAddr) -> io::Result<()> {
        let target = match endpoint.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        let path = self.state.path.borrow().clone();
        match path.send_to(&target, endpoint.port(), packet).await {
            Ok(()) => {
                self.sent_once.store(true, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                tracing::warn!(%error, "raw-IP outer packet path send failed");
                self.state.rebuild_if_current(path);
                // The tunnel timer will retransmit its handshake or keepalive;
                // TCP has its own retransmission. Keep the shared device alive.
                if self.sent_once.load(Ordering::Acquire) {
                    Ok(())
                } else {
                    Err(io::Error::other(error))
                }
            }
        }
    }
}
