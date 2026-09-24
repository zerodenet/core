//! Routed ICMP echo relay for authenticated raw-IP ingress.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::{mpsc, watch, Semaphore};
use zero_platform_tokio::{EgressInterface, IcmpSocket};
use zero_stack::packet;

use crate::runtime::route_runtime::InboundRouteRuntimeFactory;

const MAX_ECHO_PROBES: usize = 64;
const ECHO_TIMEOUT: Duration = Duration::from_secs(3);
static NEXT_PROBE_ID: AtomicU16 = AtomicU16::new(1);

pub(crate) struct IcmpEchoRelay {
    responses: mpsc::Sender<Vec<u8>>,
    route: InboundRouteRuntimeFactory,
    slots: Arc<Semaphore>,
    shutdown: watch::Receiver<bool>,
}

impl IcmpEchoRelay {
    pub(crate) fn accepts_direct_echo(packet: &[u8]) -> bool {
        packet::parse_icmp_echo_request(packet).is_some()
    }

    pub(crate) fn new(
        responses: mpsc::Sender<Vec<u8>>,
        route: InboundRouteRuntimeFactory,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            responses,
            route,
            slots: Arc::new(Semaphore::new(MAX_ECHO_PROBES)),
            shutdown,
        }
    }

    /// Returns true when the packet was an echo request, including requests
    /// explicitly rejected by routing or resource limits.
    pub(crate) fn dispatch_direct(&self, packet: &[u8], mtu: u16) -> bool {
        let Some(request) = packet::parse_icmp_echo_request(packet) else {
            return false;
        };
        let unicast = match request.destination {
            IpAddr::V4(ip) => !ip.is_unspecified() && !ip.is_multicast() && !ip.is_broadcast(),
            IpAddr::V6(ip) => !ip.is_unspecified() && !ip.is_multicast(),
        };
        if !unicast {
            reject(&self.responses, packet, mtu);
            return true;
        }
        let Ok(permit) = self.slots.clone().try_acquire_owned() else {
            reject(&self.responses, packet, mtu);
            return true;
        };
        let packet = packet.to_vec();
        let egress = self.route.icmp_egress(request.destination);
        let responses = self.responses.clone();
        let mut shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result = tokio::select! {
                result = probe(&packet, mtu, egress.as_ref()) => result,
                _ = shutdown.changed() => return,
            };
            match result {
                Ok(Some(reply)) => {
                    let _ = responses.try_send(reply);
                }
                Ok(None) => {} // An unanswered echo request times out normally.
                Err(error) => {
                    tracing::debug!(%error, "ICMP echo probe unavailable");
                    reject(&responses, &packet, mtu);
                }
            }
        });
        true
    }
}

async fn probe(
    packet: &[u8],
    mtu: u16,
    egress: Option<&EgressInterface>,
) -> std::io::Result<Option<Vec<u8>>> {
    let Some(request) = packet::parse_icmp_echo_request(packet) else {
        return Ok(None);
    };
    let bind = match request.destination {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    };
    let socket = IcmpSocket::bind(bind, egress)?;
    let local = socket.connect(request.destination).await?;
    let probe_id = NEXT_PROBE_ID.fetch_add(1, Ordering::Relaxed);
    let Some(message) = packet::build_icmp_echo_probe(&request, probe_id, local) else {
        return Ok(None);
    };
    socket.send(&message).await?;
    let deadline = tokio::time::Instant::now() + ECHO_TIMEOUT;
    let mut buffer = vec![0_u8; usize::from(mtu).max(1_280)];
    loop {
        let received = tokio::time::timeout_at(deadline, socket.recv_from(&mut buffer)).await;
        let (size, source) = match received {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => return Err(error),
            Err(_) => return Ok(None),
        };
        if source != request.destination {
            continue;
        }
        if let Some(reply) = packet::build_icmp_echo_reply(
            &request,
            probe_id,
            &buffer[..size],
            local,
            usize::from(mtu),
        ) {
            return Ok(Some(reply));
        }
    }
}

fn reject(responses: &mpsc::Sender<Vec<u8>>, packet: &[u8], mtu: u16) {
    if let Some(response) = packet::build_icmp_echo_unreachable_response(packet, usize::from(mtu)) {
        let _ = responses.try_send(response);
    }
}

#[cfg(test)]
#[path = "icmp/tests.rs"]
mod tests;
