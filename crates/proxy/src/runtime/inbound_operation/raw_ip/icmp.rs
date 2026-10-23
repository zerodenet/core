//! Routed ICMP echo relay for authenticated raw-IP ingress.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::{watch, Semaphore};
use zero_platform_tokio::{EgressInterface, IcmpSocket};
use zero_stack::packet;

use crate::runtime::route_runtime::InboundRouteRuntimeFactory;
mod probe;
use probe::probe;

pub(super) const MAX_ECHO_PROBES: usize = 64;
const ECHO_TIMEOUT: Duration = Duration::from_secs(3);
static NEXT_PROBE_ID: AtomicU16 = AtomicU16::new(1);

pub(crate) struct IcmpEchoRelay {
    responses: zero_stack::packet_output::PacketSender,
    route: InboundRouteRuntimeFactory,
    slots: Arc<Semaphore>,
    shutdown: watch::Receiver<bool>,
    tasks: Option<super::tasks::RetainedTasks<()>>,
    native: Option<crate::component::instance::HostIcmpProbeGrant>,
}

impl IcmpEchoRelay {
    pub(crate) fn accepts_direct_echo(packet: &[u8]) -> bool {
        packet::parse_icmp_echo_request(packet).is_some()
    }

    pub(crate) fn new(
        responses: zero_stack::packet_output::PacketSender,
        route: InboundRouteRuntimeFactory,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            responses,
            route,
            slots: Arc::new(Semaphore::new(MAX_ECHO_PROBES)),
            shutdown,
            tasks: None,
            native: None,
        }
    }

    pub(super) fn new_owned(
        responses: zero_stack::packet_output::PacketSender,
        route: InboundRouteRuntimeFactory,
        shutdown: watch::Receiver<bool>,
        tasks: super::tasks::RetainedTasks<()>,
        native: Option<crate::component::instance::HostIcmpProbeGrant>,
    ) -> Self {
        let mut relay = Self::new(responses, route, shutdown);
        relay.tasks = Some(tasks);
        relay.native = native;
        relay
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
        let owned = self.tasks.is_some();
        let native = self.native.clone();
        let probe = async move {
            let _permit = permit;
            let result = tokio::select! {
                result = async {
                    if owned { probe::owned(&packet, mtu, egress.as_ref(), native).await }
                    else { probe(&packet, mtu, egress.as_ref()).await }
                } => result,
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
        };
        if let Some(owner) = &self.tasks {
            let Some(mut tasks) = owner.try_lock() else {
                return true;
            };
            while let Some(joined) = tasks.try_join_next() {
                if let Err(error) = joined {
                    let cancelled = error.is_cancelled();
                    owner.remember(error);
                    if !cancelled {
                        return true;
                    }
                }
            }
            let _ = tasks.spawn(probe);
        } else {
            tokio::spawn(probe); // Ordinary TUN retains its existing execution path.
        }
        true
    }
}

fn reject(responses: &zero_stack::packet_output::PacketSender, packet: &[u8], mtu: u16) {
    if let Some(response) = packet::build_icmp_echo_unreachable_response(packet, usize::from(mtu)) {
        let _ = responses.try_send(response);
    }
}

#[cfg(test)]
#[path = "icmp/tests.rs"]
mod tests;
