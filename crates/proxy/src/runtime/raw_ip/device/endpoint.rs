//! Active client stacks attached to a neutral bidirectional IP endpoint.
mod driver;
mod readiness;
use driver::run_endpoint_stack;

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
    )
)]
mod owner;
mod recipe;
pub(crate) use owner::EndpointStackOwner;
pub(crate) use recipe::EndpointStackRecipe;

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

use super::{
    stack_tcp_error, stack_udp_error, DeviceHealth, ForwardedPackets, PacketReturns,
    SharedRawIpDevice,
};
use zero_engine::EngineError;

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

    #[cfg(test)]
    pub(crate) fn start_on_endpoint(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        peer: usize,
        endpoint: watch::Receiver<mpsc::Sender<EndpointPacket>>,
    ) -> Result<Arc<Self>, EngineError> {
        Self::start_on_endpoint_with_options(
            local_addresses,
            mtu,
            peer,
            endpoint,
            Default::default(),
        )
    }

    pub(crate) fn start_on_endpoint_with_options(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        peer: usize,
        endpoint: watch::Receiver<mpsc::Sender<EndpointPacket>>,
        options: super::super::RawIpStackOptions,
    ) -> Result<Arc<Self>, EngineError> {
        EndpointStackRecipe::prepare(local_addresses, mtu, peer, endpoint, options)
            .map_err(stack_udp_error)?
            .start()
    }

    fn start_endpoint_stack(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        peer: usize,
        endpoint: watch::Receiver<mpsc::Sender<EndpointPacket>>,
        options: super::super::RawIpStackOptions,
        task: Arc<super::workers::DeviceWorkers>,
    ) -> Result<Arc<Self>, EngineError> {
        let returns = PacketReturns::default();
        let returns = Arc::new(if task.requires_packet_memory() {
            returns.with_host_limits(&options)?
        } else {
            returns.with_limits(&options)?
        });
        let (outbound, raw_packets) =
            mpsc::channel::<zero_stack::packet_output::ObservedPacket>(128);
        let (forwarded_packets, forwarded_rx) = mpsc::channel(128);
        let outbound = options.packet_output(outbound, task.requires_packet_memory())?;

        let udp = ClientUdpStack::new_with_queue(
            local_addresses.clone(),
            outbound.clone(),
            mtu,
            Some(Arc::new(options.traffic)),
            options.udp_receive_queue,
        )
        .map_err(stack_udp_error)?;
        let tcp = Arc::new(
            ClientTcpStack::new_output_with_task_owner(
                local_addresses,
                outbound,
                mtu,
                task.clone(),
            )
            .map_err(stack_tcp_error)?,
        );
        let closed = Arc::new(AtomicBool::new(false));
        let health = Arc::new(Mutex::new(DeviceHealth::default()));
        let (ready_tx, ready) = watch::channel(None);
        let stack = EndpointStack {
            peer,
            endpoint,
            tcp: tcp.clone(),
            udp: udp.clone(),
            raw_packets,
            forwarded_packets: forwarded_rx,
            closed: closed.clone(),
            returns: returns.clone(),
        };
        task.spawn_driver(readiness::run(stack, ready_tx))?;
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
            carrier: None,
            packet_wire_memory: options.packet_wire_memory,
        }))
    }

    /// Same existing stack, with native-return backpressure retained in the
    /// caller's actual wire loop instead of discarding a full standard queue.
    pub(crate) async fn deliver_decrypted_owned_wait(
        &self,
        packet: zero_traits::PacketBuffer,
        allow_native: bool,
    ) -> Result<Option<zero_traits::PacketBuffer>, EngineError> {
        let packet = if allow_native {
            match self
                .returns
                .deliver_owned_wait(packet)
                .await
                .map_err(std::io::Error::other)?
            {
                None => return Ok(None),
                Some(packet) => packet,
            }
        } else {
            match self
                .returns
                .deliver_correlated_owned_wait(packet)
                .await
                .map_err(std::io::Error::other)?
            {
                None => return Ok(None),
                Some(packet) => packet,
            }
        };
        match self.deliver_decrypted_owned(packet, false).await {
            Ok(()) => Ok(None),
            Err(packet) => Ok(Some(packet)),
        }
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
            Some(packet::IPPROTO_TCP) => self.tcp.feed_correlated(&packet).await,
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
    udp: ClientUdpStack,
    raw_packets: mpsc::Receiver<zero_stack::packet_output::ObservedPacket>,
    forwarded_packets: mpsc::Receiver<ForwardedPackets>,
    closed: Arc<AtomicBool>,
    returns: Arc<PacketReturns>,
}
