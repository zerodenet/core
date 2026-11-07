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
use zero_platform_tokio::EgressInterface;
#[cfg(test)]
use zero_platform_tokio::IcmpSocket;
use zero_stack::packet;

use crate::runtime::route_runtime::InboundRouteRuntimeFactory;
mod probe;
#[cfg(test)]
use probe::probe;

pub(super) const MAX_ECHO_PROBES: usize = 64;
const ECHO_TIMEOUT: Duration = Duration::from_secs(3);
static NEXT_PROBE_ID: AtomicU16 = AtomicU16::new(1);

pub(crate) struct IcmpEchoRelay {
    responses: zero_stack::packet_output::PacketSender,
    route: InboundRouteRuntimeFactory,
    slots: Arc<Semaphore>,
    shutdown: watch::Receiver<bool>,
    tasks: Option<crate::runtime::raw_ip::tasks::scoped::ScopedSourceTasks<()>>,
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
        tasks: crate::runtime::raw_ip::tasks::scoped::ScopedSourceTasks<()>,
        native: Option<crate::component::instance::HostIcmpProbeGrant>,
    ) -> Self {
        let mut relay = Self::new(responses, route, shutdown);
        relay.tasks = Some(tasks);
        relay.native = native;
        relay
    }

    /// One actual Source-owned packet; all echo memory is prepaid before the
    /// already registered child can acquire its native ICMP original.
    pub(crate) fn dispatch_direct_owned(&self, mut packet: zero_traits::PacketBuffer, mtu: u16) {
        let Some(request) = packet::parse_icmp_echo_request(&packet) else {
            return;
        };
        let unicast = match request.destination {
            IpAddr::V4(ip) => !ip.is_unspecified() && !ip.is_multicast() && !ip.is_broadcast(),
            IpAddr::V6(ip) => !ip.is_unspecified() && !ip.is_multicast(),
        };
        if !unicast {
            reject(&self.responses, &packet, mtu);
            return;
        }
        let Ok(permit) = self.slots.clone().try_acquire_owned() else {
            reject(&self.responses, &packet, mtu);
            return;
        };
        let egress = self.route.icmp_egress(request.destination);
        let prepared = match probe::prepaid::prepare(&mut packet, mtu, &self.responses) {
            Ok(prepared) => prepared,
            Err(source) => {
                tracing::debug!(?source, "ICMP memory not submitted");
                reject(&self.responses, &packet, mtu);
                return;
            }
        };
        let Some(owner) = &self.tasks else {
            reject(&self.responses, &packet, mtu);
            return;
        };
        let Some(mut tasks) = owner.try_lock() else {
            return;
        };
        while let Some(joined) = tasks.try_join_next() {
            if let Err(source) = joined {
                let cancelled = source.is_cancelled();
                owner.remember(source);
                if !cancelled {
                    return;
                }
            }
        }
        let packet = Arc::new(packet);
        let responses = self.responses.clone();
        let mut shutdown = self.shutdown.clone();
        let native = self.native.clone();
        let _=tasks.spawn_with_witness(move |caller|async move {
            let _permit=permit;
            let result=tokio::select! {
                result=probe::owned(&packet,mtu,egress.as_ref(),native,Some(caller),Some(prepared))=>result,
                _=shutdown.changed()=>return,
            };
            match result {
                Ok(Some(reply))=>{let reply=probe::prepaid::retain_reply(reply,packet.clone());let _=responses.send_buffer(reply).await;},
                Ok(None)=>{},
                Err(source)=>{tracing::debug!(?source,"ICMP echo probe unavailable");reject(&responses,&packet,mtu);}
            }
        });
    }
}

fn reject(responses: &zero_stack::packet_output::PacketSender, packet: &[u8], mtu: u16) {
    if packet::parse_icmp_echo_request(packet).is_none() {
        return;
    }
    if let Some(spec) = packet::IcmpErrorSpec::for_packet(packet, usize::from(mtu), true) {
        if let Ok(mut output) = responses.allocate_transmit(spec.length()) {
            if spec.write_into(&mut output).is_ok() {
                let _ = responses.try_send_buffer(output);
            }
        }
    }
}

#[cfg(test)]
#[path = "icmp/tests.rs"]
mod tests;
