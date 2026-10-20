//! Owned native UDP ancillary I/O. This is a library boundary, not a runtime.
//! Every native original (including capability probes) has a persistent owner.
mod io;
mod observation;
mod owner;
mod poller;
mod probes;
#[cfg(test)]
mod tests;
mod types;
use super::*;
pub use poller::DatagramWritePoller;
use std::{net::SocketAddr, task::Poll};
pub use types::*;

/// All limits are caller-assigned. No process or protocol defaults are implied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataLimits {
    pub main_sockets: NonZeroUsize,
    pub probe_sockets: NonZeroUsize,
    pub probe_attempts_per_initialization: NonZeroUsize,
    pub concurrent_send_calls: NonZeroUsize,
    pub write_pollers: NonZeroUsize,
}
pub struct MetadataOwner {
    main: ControlledDatagramOwner,
    probes: probes::Probes,
    limits: MetadataLimits,
    views: BTreeMap<DatagramReservationId, Arc<Shared>>,
}
#[derive(Debug)]
pub struct MetadataOwnerReport {
    pub main: DatagramOwnerReport,
    pub probes: DatagramOwnerReport,
}
impl MetadataOwnerReport {
    pub fn is_complete(&self) -> bool {
        self.main.is_complete() && self.probes.is_complete()
    }
}
/// Unique receiver, clonable sender and unique native control, same original.
pub struct GrantedMetadataDatagram {
    pub rx: MetadataDatagramRx,
    pub tx: MetadataDatagramTx,
    pub control: MetadataDatagramControl,
}
pub struct MetadataDatagramRx {
    shared: Arc<Shared>,
    registration: poller::Registration,
}
#[derive(Clone)]
pub struct MetadataDatagramTx {
    shared: Arc<Shared>,
}
pub struct MetadataDatagramControl {
    shared: Arc<Shared>,
    observation_transferred: bool,
}
/// Unique observation authority over an already granted original. This carries
/// no native close/revoke authority and cannot create another receiver/poller.
pub struct MetadataDatagramObservation {
    shared: Arc<Shared>,
}
struct Shared {
    socket: ControlledDatagramSocket,
    state: quinn_udp::UdpSocketState,
    pollers: Arc<poller::Broker>,
    sends: tokio::sync::Semaphore,
    send_limit: NonZeroUsize,
}
impl Shared {
    fn check(&self) -> std_io::Result<()> {
        if self.socket.entry.retired.is_cancelled() {
            return Err(std_io::Error::new(
                std_io::ErrorKind::NotConnected,
                "metadata datagram original is retired",
            ));
        }
        Ok(())
    }
    fn capabilities(&self) -> MetadataCapabilities {
        MetadataCapabilities {
            transmit_segments: self.state.max_gso_segments(),
            receive_segments: self.state.gro_segments(),
            batch_size: quinn_udp::BATCH_SIZE,
            may_fragment: self.state.may_fragment(),
        }
    }
}
impl MetadataDatagramTx {
    /// Structural bound on whole synchronous send calls, including observation.
    pub fn max_concurrent_send_calls(&self) -> NonZeroUsize {
        self.shared.send_limit
    }
    pub fn local_addr(&self) -> std_io::Result<SocketAddr> {
        self.shared.socket.local_addr()
    }
    pub fn capabilities(&self) -> MetadataCapabilities {
        self.shared.capabilities()
    }
    pub fn create_write_poller(&self) -> std_io::Result<DatagramWritePoller> {
        self.shared.check()?;
        let registration = self.shared.pollers.register()?;
        Ok(DatagramWritePoller {
            shared: self.shared.clone(),
            registration,
        })
    }
}
impl MetadataDatagramRx {
    pub fn local_addr(&self) -> std_io::Result<SocketAddr> {
        self.shared.socket.local_addr()
    }
    pub fn capabilities(&self) -> MetadataCapabilities {
        self.shared.capabilities()
    }
}
impl MetadataDatagramControl {
    /// Transfer the sole mutable observation authority. The host keeps native
    /// control; subsequent transfers or observations through control are denied.
    pub fn take_observation(&mut self) -> std_io::Result<MetadataDatagramObservation> {
        self.shared.check()?;
        if self.observation_transferred {
            return Err(std_io::Error::new(
                std_io::ErrorKind::AlreadyExists,
                "metadata observation authority already transferred",
            ));
        }
        self.observation_transferred = true;
        Ok(MetadataDatagramObservation {
            shared: self.shared.clone(),
        })
    }
    /// Observe the actual native metadata boundary, including GRO segments and
    /// partial submissions. The original remains owned if a callback panics.
    pub fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> std_io::Result<()> {
        if self.observation_transferred {
            return Err(std_io::Error::new(
                std_io::ErrorKind::PermissionDenied,
                "metadata observation authority belongs to its granted view",
            ));
        }
        self.shared.check()?;
        let old = self.shared.socket.replace_observer(observer.clone())?;
        super::observation::boundary(&observer, old);
        Ok(())
    }
    pub fn reservation(&self) -> DatagramReservationId {
        self.shared.socket.reservation()
    }
    /// Stop all current and future native I/O. This is not a close receipt.
    pub fn revoke(&self) {
        self.shared.socket.entry.retired.cancel();
        self.shared.pollers.signal();
    }
    pub fn close(&mut self) -> Result<DatagramCloseReceipt, Arc<std_io::Error>> {
        self.revoke();
        self.shared.socket.close()
    }
}
impl MetadataDatagramObservation {
    /// Identity check only, not a native close or task completion receipt.
    pub fn matches(&self, rx: &MetadataDatagramRx, tx: &MetadataDatagramTx) -> bool {
        Arc::ptr_eq(&self.shared, &rx.shared) && Arc::ptr_eq(&self.shared, &tx.shared)
    }
    pub fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> std_io::Result<()> {
        self.shared.check()?;
        let old = self.shared.socket.replace_observer(observer.clone())?;
        super::observation::boundary(&observer, old);
        Ok(())
    }
}
impl fmt::Debug for MetadataOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataOwner")
            .field("main", &self.main)
            .field("probes", &self.probes)
            .finish_non_exhaustive()
    }
}
