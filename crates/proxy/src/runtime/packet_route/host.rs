//! Bidirectional host L3 I/O. The host supplies a dedicated configured device.
use super::{PacketForwardObservation, PreparedPacketRouteOperation};
use crate::runtime::raw_ip::PacketReturns;
use std::{io, sync::Arc, time::Instant};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot, watch},
};
use zero_stack::{packet, FragmentReassembler};
use zero_traits::IoObserver;
struct Write {
    return_channel: mpsc::Sender<Vec<u8>>,
    packet: Vec<u8>,
    observer: Option<Arc<dyn IoObserver>>,
    ack: oneshot::Sender<(Vec<u8>, io::Result<()>)>,
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
    finished: watch::Receiver<bool>,
    pub(crate) mtu: usize,
    activate: watch::Sender<bool>,
}
impl HostPacketDevice {
    pub(crate) fn start<D: zero_tun::TunDevice + 'static>(
        device: D,
        config: zero_config::DirectPacketDeviceConfig,
        mtu: u16,
    ) -> Arc<Self> {
        let (writes, mut rx) = mpsc::channel::<Write>(128);
        let returns = Arc::new(PacketReturns::default());
        let (stop, mut stopped) = watch::channel(false);
        let (done, finished) = watch::channel(false);
        let packets = returns.clone();
        let (activate, mut activated) = watch::channel(false);
        struct Receipt(watch::Sender<bool>);
        impl Drop for Receipt {
            fn drop(&mut self) {
                self.0.send_replace(true);
            }
        }
        let receipt = Receipt(done);
        tokio::spawn(async move {
            let _receipt = receipt;
            while !*activated.borrow_and_update() {
                tokio::select! { biased; _ = stopped.changed() => return, result = activated.changed() => { if result.is_err() { return; } } }
            }
            let (mut reader, mut writer) = tokio::io::split(device);
            let mut buffer = vec![0; 65_536];
            let mut fragments = FragmentReassembler::new();
            let mut timer = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = stopped.changed() => break,
                    result = reader.read(&mut buffer) => match result {
                        Ok(0) | Err(_) => break,
                        Ok(n) => match fragments.process(&buffer[..n], Instant::now()) {
                            zero_stack::FragmentOutcome::NotFragmented(packet) => { packets.deliver(packet); },
                            zero_stack::FragmentOutcome::Reassembled(packet) => { let _ = packets.deliver_owned(packet); },
                            zero_stack::FragmentOutcome::Pending | zero_stack::FragmentOutcome::Rejected(_) => packets.lose_receive_coverage(),
                        },
                    },
                    write = rx.recv() => {
                        let Some(write) = write else { break; };
                        if write.return_channel.is_closed() { write.discarded(); let _ = write.ack.send((write.packet, Err(io::Error::new(io::ErrorKind::Interrupted,"packet route closed")))); continue; }
                        let (result, stopped, discarded) = tokio::select! { biased;
                            _ = stopped.changed() => (Err(io::Error::new(io::ErrorKind::Interrupted, "host packet device stopped")), true, true),
                            _ = write.return_channel.closed() => (Err(io::Error::new(io::ErrorKind::Interrupted, "packet route closed")), false, true),
                            result = writer.write(&write.packet) => {
                                let result = result.and_then(|n| if n == write.packet.len() { Ok(()) } else { Err(io::Error::new(io::ErrorKind::WriteZero, "partial host packet write")) });
                                let failed = result.is_err(); (result, failed, false)
                            },
                        };
                        if discarded { write.discarded(); }
                        else if let Some(observer) = &write.observer { if result.is_ok() { observer.sent(write.packet.len()); } else { observer.error(); } }
                        let _ = write.ack.send((write.packet, result));
                        if stopped { break; }

                    },
                    _ = timer.tick() => packets.expire(),
                }
            }
            rx.close();
            while let Ok(write) = rx.try_recv() {
                write.discarded();
                let _ = write.ack.send((
                    write.packet,
                    Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "host packet writer stopped",
                    )),
                ));
            }
            packets.clear();
        });
        Arc::new(Self {
            activate,
            config,
            writes,
            returns,
            stop,
            finished,
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
        *self.activate.borrow() && !*self.finished.borrow() && !*self.stop.borrow()
    }
    pub(crate) async fn wait_stopped(&self) {
        let mut done = self.finished.clone();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                break;
            }
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
        packet: &mut Vec<u8>,
        ingress_id: u64,
        replies: mpsc::Sender<Vec<u8>>,
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
        packet: &mut Vec<u8>,
        ingress_id: u64,
        replies: mpsc::Sender<Vec<u8>>,
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
