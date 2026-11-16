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

use crate::component::stream_bridge::datagram::packet_path::RolePacketPath;
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
pub(super) fn background_stopped() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, BackgroundStopped)
}

pub(crate) type CarrierWorkerCompletion =
    Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>>;

#[async_trait]
pub(crate) trait RawIpWireCarrier: Send + Sync {
    /// Synchronously fence/cancel this carrier's original background work,
    /// returning a waiter for actual joins. None is an unmigrated task graph,
    /// never successful cleanup. A worker join proves no native I/O close.
    fn stop_background(&self) -> Option<CarrierWorkerCompletion> {
        None
    }

    /// Original provider evidence is independent of worker joins.
    async fn close_native(&self) -> Result<(), EngineError> {
        Err(EngineError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "raw-IP carrier has no original native-close provider",
        )))
    }

    async fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, Option<SocketAddr>)>;
    async fn send(&self, packet: &[u8], endpoint: SocketAddr) -> io::Result<()>;
}

mod registered;
pub(crate) use registered::RegisteredProxiedRawIpWireCarrier;
mod hosted;
pub(crate) use hosted::HostedRawIpWireCarrier;
#[cfg(test)]
mod direct;
#[cfg(test)]
pub(crate) use direct::DirectRawIpWireCarrier;
