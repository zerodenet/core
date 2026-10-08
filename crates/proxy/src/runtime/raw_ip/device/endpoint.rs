//! Active client stacks attached to a neutral bidirectional IP endpoint.

use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use tokio::sync::{mpsc, watch};
use zero_stack::{client_udp::ClientUdpStack, packet, ClientTcpStack};

use super::{DeviceHealth, ForwardedPackets, PacketReturns, SharedRawIpDevice};

/// An inner IP packet submitted to a bidirectional datagram endpoint.
pub(crate) struct EndpointPacket {
    pub(crate) peer: usize,
    pub(crate) packet: zero_traits::PacketBuffer,
    pub(crate) observer: Option<Arc<dyn zero_traits::IoObserver>>,
    /// Already queued packets must not survive stack revocation or re-enable.
    pub(crate) closed: Arc<AtomicBool>,
    pub(crate) return_channel: Option<zero_stack::packet_output::PacketSender>,
}

impl SharedRawIpDevice {
    pub(crate) fn observe_handshake(&self, age: Option<Duration>) {
        if let Some(age) = age {
            self.health
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .last_handshake = Instant::now().checked_sub(age);
        }
    }

    pub(crate) fn observe_authenticated_packet(&self) {
        self.health
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .last_authenticated_packet = Some(Instant::now());
    }

    pub(crate) fn start_on_endpoint(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        peer: usize,
        endpoint: watch::Receiver<mpsc::Sender<EndpointPacket>>,
    ) -> Result<Arc<Self>, zero_stack::client_udp::ClientUdpStackError> {
        let (outbound, raw_packets) =
            mpsc::channel::<zero_stack::packet_output::ObservedPacket>(128);
        let (forwarded_packets, forwarded_rx) = mpsc::channel(128);
        let udp = ClientUdpStack::new_observed(local_addresses.clone(), outbound.clone(), mtu)?;
        let tcp = Arc::new(
            ClientTcpStack::new_observed(local_addresses, outbound, mtu)
                .map_err(|_| zero_stack::client_udp::ClientUdpStackError::InvalidMtu)?,
        );
        let returns = Arc::new(PacketReturns::default());
        let closed = Arc::new(AtomicBool::new(false));
        let health = Arc::new(Mutex::new(DeviceHealth::default()));
        let (_ready_tx, ready) = watch::channel(Some(Ok(())));
        let (completed_tx, completed) = watch::channel(false);
        let completion = super::completion::Completion(completed_tx);
        let stack = EndpointStack {
            peer,
            endpoint,
            tcp: tcp.clone(),
            raw_packets,
            forwarded_packets: forwarded_rx,
            closed: closed.clone(),
            returns: returns.clone(),
        };
        let task = tokio::spawn(async move {
            let _completion = completion;
            run_endpoint_stack(stack).await;
        })
        .abort_handle();
        Ok(Arc::new(Self {
            incarnation: super::super::next_incarnation(),
            udp,
            tcp,
            forwarded_packets,
            returns,
            closed,
            health,
            retired: AtomicBool::new(false),
            ready,
            task,
            completed,
        }))
    }

    /// Consume decrypted packets belonging to an active outbound flow or
    /// explicitly registered Packet return route.
    pub(crate) async fn deliver_decrypted_owned(
        &self,
        packet: zero_traits::PacketBuffer,
        allow_native: bool,
    ) -> Result<(), zero_traits::PacketBuffer> {
        let packet = if allow_native {
            match self.returns.deliver_owned(packet) {
                Ok(()) => return Ok(()),
                Err(packet) => packet,
            }
        } else {
            if self.returns.deliver_correlated(&packet) {
                return Ok(());
            }
            packet
        };
        let delivered = match packet::ip_protocol(&packet) {
            Some(packet::IPPROTO_TCP) if self.tcp.has_connection(&packet).await => {
                self.tcp.feed(&packet).await;
                true
            }
            Some(packet::IPPROTO_UDP) => self.udp.feed_correlated(&packet),
            Some(packet::IPPROTO_ICMP) | Some(packet::IPPROTO_ICMPV6) => {
                let Some(error) = packet::parse_icmp_error(&packet) else {
                    return Err(packet);
                };
                match error.quoted_protocol {
                    packet::IPPROTO_TCP => {
                        self.tcp
                            .feed_icmp_error_observed(error, Some(packet.len()))
                            .await
                    }
                    packet::IPPROTO_UDP => self.udp.feed_correlated(&packet),
                    _ => false,
                }
            }
            _ => false,
        };
        if delivered {
            Ok(())
        } else {
            Err(packet)
        }
    }
}

struct EndpointStack {
    peer: usize,
    endpoint: watch::Receiver<mpsc::Sender<EndpointPacket>>,
    tcp: Arc<ClientTcpStack>,
    raw_packets: mpsc::Receiver<zero_stack::packet_output::ObservedPacket>,
    forwarded_packets: mpsc::Receiver<ForwardedPackets>,
    closed: Arc<AtomicBool>,
    returns: Arc<PacketReturns>,
}

async fn run_endpoint_stack(stack: EndpointStack) {
    let EndpointStack {
        peer,
        mut endpoint,
        tcp,
        mut raw_packets,
        mut forwarded_packets,
        closed,
        returns,
    } = stack;
    let mut sweep = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = sweep.tick() => returns.expire(),
            packet = raw_packets.recv() => {
                let Some(zero_stack::packet_output::ObservedPacket { packet, observer }) = packet else { break; };
                let packets = if packet::ip_protocol(&packet) == Some(packet::IPPROTO_TCP) {
                    tcp.fragment_outbound_packet_owned(packet).await
                } else { vec![packet] };
                for packet in packets {
                    if !send_endpoint_packet(&mut endpoint, EndpointPacket { peer, packet: packet.into(), observer: observer.clone(), closed: closed.clone(), return_channel: None }).await {
                        closed.store(true, Ordering::Release);
                        return;
                    }
                }
            }
            packets = forwarded_packets.recv() => {
                let Some(ForwardedPackets { packets, observer, return_channel }) = packets else { break; };
                for packet in packets {
                    if return_channel.is_closed() { if let Some(observer) = &observer { observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed); } continue; }
                    if !send_endpoint_packet(&mut endpoint, EndpointPacket { peer, packet, observer: observer.clone(), closed: closed.clone(), return_channel: Some(return_channel.clone()) }).await {
                        closed.store(true, Ordering::Release);
                        return;
                    }
                }
            }
        }
    }
    closed.store(true, Ordering::Release);
}

async fn send_endpoint_packet(
    endpoint: &mut watch::Receiver<mpsc::Sender<EndpointPacket>>,
    mut packet: EndpointPacket,
) -> bool {
    loop {
        let sender = endpoint.borrow().clone();
        match sender.send(packet).await {
            Ok(()) => return true,
            Err(error) => packet = error.0,
        }
        if endpoint.changed().await.is_err() {
            return false;
        }
    }
}
