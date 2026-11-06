//! Bidirectional host L3 I/O. The host supplies a dedicated configured device.
mod driver;
mod pause;
pub(crate) use pause::NativeDevicePaused;
mod legacy;
mod policy;
mod scoped;
mod session;
mod task;
use super::{PacketForwardObservation, PreparedPacketRouteOperation};
use crate::runtime::raw_ip::PacketReturns;
pub(crate) use policy::PreparedHostPacketOperation;
use std::{io, sync::Arc, time::Instant};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot, watch},
};
use zero_stack::packet;
use zero_traits::IoObserver;
struct LegacyWrite {
    return_channel: zero_stack::packet_output::PacketSender,
    packet: zero_traits::PacketBuffer,
    observer: Option<Arc<dyn IoObserver>>,
    ack: oneshot::Sender<(zero_traits::PacketBuffer, io::Result<()>)>,
}
impl LegacyWrite {
    fn discarded(&self) {
        if let Some(observer) = &self.observer {
            observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed);
        }
    }
}
enum Write {
    Legacy(LegacyWrite),
    Owned(session::OwnedWrite),
}
pub(crate) struct HostPacketDevice {
    read_pause: Option<Arc<pause::ReadPause>>,
    pub(crate) config: zero_config::DirectPacketDeviceConfig,
    writes: mpsc::Sender<Write>,
    returns: Arc<PacketReturns>,
    sessions: Arc<session::Sessions>,
    stop: watch::Sender<bool>,
    worker: task::DriverTask,
    pub(crate) mtu: usize,
    activate: watch::Sender<bool>,
    ready: watch::Receiver<bool>,
    native_provider:
        std::sync::OnceLock<zero_platform_tokio::controlled_carrier::PacketProviderRegistration>,
}
impl HostPacketDevice {
    #[cfg(test)]
    pub(crate) fn start<D: zero_tun::TunDevice + 'static>(
        device: D,
        config: zero_config::DirectPacketDeviceConfig,
        mtu: u16,
    ) -> Arc<Self> {
        Self::start_with_close(device, config, mtu, |_| None)
    }

    #[cfg(test)]
    fn start_with_close<D, C>(
        device: D,
        config: zero_config::DirectPacketDeviceConfig,
        mtu: u16,
        close: C,
    ) -> Arc<Self>
    where
        D: zero_tun::TunDevice + 'static,
        C: FnOnce(D) -> Option<Result<Arc<zero_tun::TunCloseReceipt>, Arc<io::Error>>>
            + Send
            + 'static,
    {
        Self::start_with_completion(device, config, mtu, move |device| {
            std::future::ready(close(device))
        })
    }

    #[cfg(test)]
    fn start_with_completion<D, C, F>(
        device: D,
        config: zero_config::DirectPacketDeviceConfig,
        mtu: u16,
        close: C,
    ) -> Arc<Self>
    where
        D: zero_tun::TunDevice + 'static,
        C: FnOnce(D) -> F + Send + 'static,
        F: std::future::Future<
                Output = Option<Result<Arc<zero_tun::TunCloseReceipt>, Arc<io::Error>>>,
            > + Send
            + 'static,
    {
        let (writes, rx) = mpsc::channel::<Write>(config.write_queue.get());
        let returns = Arc::new(PacketReturns::default());
        let sessions = session::Sessions::new(
            returns.clone(),
            config.router_addresses.clone(),
            usize::from(mtu),
        );
        let (stop, stopped) = watch::channel(false);
        let (activate, activated) = watch::channel(false);
        let (_ready, ready) = watch::channel(true);
        let packets = returns.clone();
        let logical = sessions.clone();
        let worker = task::DriverTask::new(async move {
            let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
            let read = root
                .native_grant(65_536)
                .expect("explicit independent fixture read owner");
            let (device, failure) =
                driver::run(device, read, rx, packets, logical, stopped, activated, None)
                    .await
                    .parts();
            task::DriverResult {
                failure,
                native: close(device).await,
            }
        });
        Arc::new(Self {
            read_pause: None,
            activate,
            ready,
            config,
            writes,
            returns,
            sessions,
            stop,
            worker,
            mtu: usize::from(mtu),
            native_provider: std::sync::OnceLock::new(),
        })
    }
    pub(crate) fn activate(&self) {
        self.activate.send_replace(true);
    }
    pub(crate) fn close(&self) {
        self.sessions.stop();
        self.stop.send_replace(true);
    }
    pub(crate) fn usable(&self) -> bool {
        *self.activate.borrow() && !self.worker.is_finished() && !*self.stop.borrow()
    }
    #[cfg(test)]
    pub(crate) async fn wait_workers_stopped(&self) -> io::Result<()> {
        self.worker.join().await.map(|_| ())
    }
    pub(crate) async fn wait_closed(&self) -> io::Result<()> {
        self.close();
        if let Some(original) = self.native_provider.get() {
            if let Some(driver) = self.worker.witness() {
                driver.wait().await;
            }
            let closed = original
                .closed()
                .filter(|closed| closed.matches(original))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "same actual native Packet provider has not closed",
                    )
                })?;
            let _ = closed;
            return Ok(());
        }
        self.worker.join_closed().await
    }
}
impl Drop for HostPacketDevice {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
#[path = "host/tests.rs"]
mod tests;
