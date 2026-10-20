//! Bidirectional host L3 I/O. The host supplies a dedicated configured device.
mod driver;
mod policy;
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
struct Write {
    return_channel: zero_stack::packet_output::PacketSender,
    packet: zero_traits::PacketBuffer,
    observer: Option<Arc<dyn IoObserver>>,
    ack: oneshot::Sender<(zero_traits::PacketBuffer, io::Result<()>)>,
}
impl Write {
    fn discarded(&self) {
        if let Some(observer) = &self.observer {
            observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed);
        }
    }
}
pub(crate) struct HostPacketDevice {
    pub(crate) config: zero_config::DirectPacketDeviceConfig,
    writes: mpsc::Sender<Write>,
    returns: Arc<PacketReturns>,
    stop: watch::Sender<bool>,
    worker: task::DriverTask,
    pub(crate) mtu: usize,
    activate: watch::Sender<bool>,
}
impl HostPacketDevice {
    #[cfg(any(test, target_os = "windows"))]
    pub(crate) fn start<D: zero_tun::TunDevice + 'static>(
        device: D,
        config: zero_config::DirectPacketDeviceConfig,
        mtu: u16,
    ) -> Arc<Self> {
        Self::start_with_close(device, config, mtu, |_| None)
    }

    pub(crate) fn start_owned<D: zero_tun::OwnedTunDevice + 'static>(
        device: D,
        config: zero_config::DirectPacketDeviceConfig,
        mtu: u16,
    ) -> Arc<Self> {
        let identity = device.owned_identity();
        Self::start_with_close(device, config, mtu, move |device| {
            let result = device.close_owned().and_then(|receipt| {
                if receipt.identity().same_owner(&identity) {
                    Ok(receipt)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "host packet close returned a different native owner",
                    ))
                }
            });
            Some(result.map_err(Arc::new))
        })
    }

    fn start_with_close<D, C>(
        device: D,
        config: zero_config::DirectPacketDeviceConfig,
        mtu: u16,
        close: C,
    ) -> Arc<Self>
    where
        D: zero_tun::TunDevice + 'static,
        C: FnOnce(D) -> Option<Result<zero_tun::TunCloseReceipt, Arc<io::Error>>> + Send + 'static,
    {
        let (writes, rx) = mpsc::channel::<Write>(128);
        let returns = Arc::new(PacketReturns::default());
        let (stop, stopped) = watch::channel(false);
        let (activate, activated) = watch::channel(false);
        let packets = returns.clone();
        let worker = task::DriverTask::new(async move {
            let (device, failure) = driver::run(device, rx, packets, stopped, activated)
                .await
                .parts();
            task::DriverResult {
                failure,
                native: close(device),
            }
        });
        Arc::new(Self {
            activate,
            config,
            writes,
            returns,
            stop,
            worker,
            mtu: usize::from(mtu),
        })
    }
    pub(crate) fn activate(&self) {
        self.activate.send_replace(true);
    }
    pub(crate) fn close(&self) {
        self.stop.send_replace(true);
    }
    pub(crate) fn usable(&self) -> bool {
        *self.activate.borrow() && !self.worker.is_finished() && !*self.stop.borrow()
    }
    /// Compatibility waiter: actual join, no inferred native close receipt.
    pub(crate) async fn wait_stopped(&self) {
        if let Err(error) = self.worker.join().await {
            tracing::warn!(%error, "host packet original driver joined with a retained failure");
        }
    }
}
impl Drop for HostPacketDevice {
    fn drop(&mut self) {
        self.close();
    }
}
#[async_trait::async_trait]
impl PreparedPacketRouteOperation for Arc<HostPacketDevice> {
    async fn forward(
        &self,
        packet: &mut zero_traits::PacketBuffer,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
        _: u64,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> io::Result<PacketForwardObservation> {
        let hop = packet::ip_hop_limit(packet);
        let result = self
            .forward_inner(packet, ingress_id, replies, observer)
            .await;
        if result.is_err() && packet::ip_hop_limit(packet) != hop {
            if let Some(hop) = hop {
                packet::restore_ip_hop(packet, hop);
            }
        }
        result
    }
}
impl HostPacketDevice {
    async fn forward_inner(
        &self,
        packet: &mut zero_traits::PacketBuffer,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> io::Result<PacketForwardObservation> {
        if !self.usable() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "host packet device stopped",
            ));
        }
        let source = packet::ip_source(packet)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid IP packet"))?;
        let router = self
            .config
            .router_addresses
            .iter()
            .find(|ip| ip.is_ipv4() == source.is_ipv4());
        if packet.len() > self.mtu && !packet::ipv4_fragmentation_allowed(packet) {
            return Ok(PacketForwardObservation::local(router.and_then(|router| {
                packet::build_icmp_mtu_error_response(packet, *router, self.mtu)
            })));
        }
        let conversation = packet::packet_conversation_key(packet);
        if packet::ip_hop_limit(packet).is_some_and(|limit| limit <= 1) {
            return Ok(PacketForwardObservation::local(router.and_then(|router| {
                packet::build_icmp_time_exceeded_response(packet, *router, self.mtu)
            })));
        }
        if !packet::advance_ip_hop(packet) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid IP packet",
            ));
        }
        let fragmented = packet.len() > self.mtu;
        let mut fragments = if fragmented {
            packet::fragment_forwarded_packet(packet, self.mtu)
                .into_iter()
                .map(Into::into)
                .collect::<Vec<zero_traits::PacketBuffer>>()
        } else {
            Vec::new()
        };
        if fragmented && fragments.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet cannot be fragmented",
            ));
        }
        let return_channel = replies.clone();
        self.returns.register_observed(
            source,
            ingress_id,
            replies,
            observer.clone(),
            conversation,
        )?;
        if !fragmented {
            fragments.push(std::mem::take(packet));
        }
        for outgoing in fragments.drain(..) {
            let (ack, done) = oneshot::channel();
            let permit = match self.writes.try_reserve() {
                Ok(permit) => permit,
                Err(_) => {
                    if !fragmented {
                        *packet = outgoing;
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "host packet queue full",
                    ));
                }
            };
            permit.send(Write {
                return_channel: return_channel.clone(),
                packet: outgoing,
                observer: observer.clone(),
                ack,
            });
            let (outgoing, result) = done.await.map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "host packet writer stopped")
            })?;
            if !fragmented {
                *packet = outgoing;
            }
            result?;
        }
        Ok(PacketForwardObservation::forwarded(None))
    }
}

#[cfg(test)]
#[path = "host/tests.rs"]
mod tests;
