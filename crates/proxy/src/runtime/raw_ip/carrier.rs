//! Wire datagram I/O for a raw-IP peer device.

use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
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
use zero_engine::EngineError;

#[derive(Debug)]
struct BackgroundStopped;
impl std::fmt::Display for BackgroundStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("outer packet path owner stopped")
    }
}
impl std::error::Error for BackgroundStopped {}
pub(super) fn is_background_stopped(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<BackgroundStopped>())
}
fn background_stopped() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, BackgroundStopped)
}

pub(crate) type CarrierWorkerCompletion =
    Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>>;

#[async_trait]
pub(crate) trait RawIpWireCarrier: Send + Sync {
    /// Unknown legacy native ownership cannot authorize a replacement.
    fn native_owner(
        &self,
    ) -> Option<Arc<crate::runtime::udp_dispatch::packet_owner::PacketCarrierOwnerSlot>> {
        None
    }

    /// Actual original background handles consumed, independent of their fault.
    /// Unknown legacy implementations cannot certify an empty task graph.
    fn background_drained(&self) -> bool {
        false
    }
    /// Synchronously fence/cancel this carrier's original background work,
    /// returning a waiter for actual joins. None is an unmigrated task graph,
    /// never successful cleanup. A worker join proves no native I/O close.
    fn stop_background(&self) -> Option<CarrierWorkerCompletion> {
        None
    }

    async fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, Option<SocketAddr>)>;
    async fn send(&self, packet: &[u8], endpoint: SocketAddr) -> io::Result<()>;
}

pub(crate) struct DirectRawIpWireCarrier(pub(crate) TokioDatagramSocket);

#[async_trait]
impl RawIpWireCarrier for DirectRawIpWireCarrier {
    fn background_drained(&self) -> bool {
        true
    } // This concrete type creates none.
    fn stop_background(&self) -> Option<CarrierWorkerCompletion> {
        // This concrete view owns no background task at all. Its native socket
        // requires separate explicit close evidence after the I/O driver stops.
        Some(Box::pin(async { Ok(()) }))
    }

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
    closing: tokio::sync::watch::Sender<bool>,
    tasks: super::tasks::RetainedTasks<()>,
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
                closing,
                // The original design admits one reconnect worker; retain that
                // exact finite budget across active and ready-but-unjoined work.
                tasks: super::tasks::RetainedTasks::new("raw-IP outer carrier reconnect", 1),
            }),
            sent_once: AtomicBool::new(false),
        }
    }
}

impl Drop for ProxiedRawIpWireCarrier {
    fn drop(&mut self) {
        drop(self.stop_background());
    }
}

impl ProxiedCarrierState {
    fn rebuild_if_current(self: &Arc<Self>, failed: Arc<dyn PacketPathCarrier>) {
        if *self.closing.borrow() || !Arc::ptr_eq(&failed, &*self.path.borrow()) {
            return;
        }
        let Some(mut tasks) = self.tasks.try_lock() else {
            return;
        };
        self.tasks.reap_ready(&mut tasks);
        if !tasks.is_empty() {
            return;
        }
        let state = Arc::clone(self);
        if let Err(error) = tasks.spawn(async move {
            state.rebuild(failed).await;
        }) {
            tracing::warn!(%error, "raw-IP original reconnect task admission failed");
        }
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
    fn background_drained(&self) -> bool {
        self.state.tasks.is_drained()
    }
    fn native_owner(
        &self,
    ) -> Option<Arc<crate::runtime::udp_dispatch::packet_owner::PacketCarrierOwnerSlot>> {
        self.state.path.borrow().native_owner()
    }

    fn stop_background(&self) -> Option<CarrierWorkerCompletion> {
        self.state.closing.send_replace(true);
        self.state.tasks.request_abort();
        let tasks = self.state.tasks.clone();
        Some(Box::pin(async move {
            tasks
                .join_unbounded()
                .await
                .map(|_| ())
                .map_err(|source| EngineError::Io(io::Error::other(source)))
        }))
    }

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
                        if *self.state.closing.borrow() {
                            return Err(background_stopped());
                        }
                        tokio::select! {
                            changed = changes.changed() => { changed.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "outer packet path closed"))?; },
                            joined = self.state.tasks.wait_one() => {
                                let joined = joined.map_err(io::Error::other)?;
                                if !joined && Arc::ptr_eq(&path, &*changes.borrow()) {
                                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "outer packet path reconnect owner has no running worker"));
                                }
                            },
                        }
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

#[cfg(test)]
#[path = "carrier/tests.rs"]
mod tests;
