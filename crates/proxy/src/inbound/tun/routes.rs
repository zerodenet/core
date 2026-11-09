use std::io;
use std::net::IpAddr;
use std::time::Duration;

use ipnet::IpNet;
use tokio::sync::{mpsc, oneshot};
use zero_tun::{
    strict_route_socket_mark, FamilyEgressState, RouteChangeMonitor, RouteInterface,
    SystemRouteGuard,
};

use crate::runtime::Proxy;

#[cfg(feature = "raw-ip-runtime")]
pub(super) mod owned;

const ROUTE_EVENT_DEBOUNCE: Duration = Duration::from_millis(400);
const ROUTE_WATCHDOG_INTERVAL: Duration = Duration::from_secs(30);
const ROUTE_RETRY_DELAYS: [Duration; 4] = [
    Duration::from_millis(250),
    Duration::from_secs(1),
    Duration::from_secs(3),
    Duration::from_secs(10),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileTrigger {
    PlatformEvent,
    Retry,
    Watchdog,
    Manual,
}

impl ReconcileTrigger {
    fn invalidates_network(self, routes_changed: bool) -> bool {
        routes_changed || matches!(self, Self::Manual | Self::PlatformEvent)
    }

    fn should_debounce(self) -> bool {
        matches!(self, Self::PlatformEvent)
    }
}

pub(super) fn platform_egress_interface(
    route: &RouteInterface,
    socket_mark: Option<u32>,
) -> io::Result<zero_platform_tokio::EgressInterface> {
    let interface =
        zero_platform_tokio::EgressInterface::new(route.name().to_owned(), route.index())?;
    match socket_mark {
        Some(mark) => interface.with_socket_mark(mark),
        None => Ok(interface),
    }
}

pub(super) fn publish_family_egress(
    control: &zero_platform_tokio::EgressInterfaceControl,
    ipv6: bool,
    state: &FamilyEgressState,
    socket_mark: Option<u32>,
) -> io::Result<()> {
    let previous_generation = control.generation();
    match state {
        FamilyEgressState::Available(route) => {
            let interface = platform_egress_interface(route, socket_mark)?;
            control.replace_for(ipv6, Some(interface));
        }
        FamilyEgressState::Unavailable(reason) => {
            control.mark_unavailable_for(ipv6, reason.as_str());
        }
        FamilyEgressState::Unknown => control.replace_for(ipv6, None),
    }
    let generation = control.generation();
    if generation != previous_generation {
        match state {
            FamilyEgressState::Available(route) => tracing::info!(
                event_type = "tun_family_egress_available",
                address_family = if ipv6 { "ipv6" } else { "ipv4" },
                interface = route.name(),
                interface_index = route.index(),
                route_source = "native_default_route",
                generation,
                "TUN family egress became available"
            ),
            FamilyEgressState::Unavailable(reason) => tracing::warn!(
                event_type = "tun_family_egress_unavailable",
                address_family = if ipv6 { "ipv6" } else { "ipv4" },
                reason = reason.as_str(),
                route_source = if *reason == zero_tun::EgressUnavailableReason::NoDefaultRoute {
                    "cross_family_route_carrier"
                } else {
                    "native_default_route"
                },
                generation,
                "TUN family egress became unavailable"
            ),
            FamilyEgressState::Unknown => {}
        }
        tracing::info!(
            event_type = "tun_network_generation_changed",
            previous_generation,
            generation,
            cause = "family_egress_changed",
            address_family = if ipv6 { "ipv6" } else { "ipv4" },
            "TUN network generation changed"
        );
    }
    Ok(())
}

fn log_route_install_carrier_selected(
    guard: &SystemRouteGuard,
    generation: u64,
    route_source: &'static str,
) {
    let family_state = guard.family_egress();
    let cross_family = matches!(
        family_state,
        FamilyEgressState::Unavailable(zero_tun::EgressUnavailableReason::NoDefaultRoute)
    );
    tracing::info!(
        event_type = "tun_route_install_carrier_selected",
        address_family = if guard.is_ipv6() { "ipv6" } else { "ipv4" },
        carrier_family = if guard.is_ipv6() != cross_family {
            "ipv6"
        } else {
            "ipv4"
        },
        interface = guard.egress().name(),
        interface_index = guard.egress().index(),
        route_source,
        generation,
        native_egress_state = match family_state {
            FamilyEgressState::Available(_) => "available",
            FamilyEgressState::Unavailable(_) => "unavailable",
            FamilyEgressState::Unknown => "unknown",
        },
        native_egress_reason = family_state
            .unavailable_reason()
            .map(zero_tun::EgressUnavailableReason::as_str),
        "TUN route installation carrier selected"
    );
}

pub(super) struct RouteRuntimeSpec {
    pub id: u64,
    pub tun_name: String,
    pub recovery_key: String,
    pub primary_ipv6: bool,
    pub addresses: Vec<(IpAddr, IpAddr)>,
    pub include_cidrs: Vec<IpNet>,
    pub exclude_cidrs: Vec<IpNet>,
    pub dns_hijack: bool,
    pub strict_route: bool,
}

impl RouteRuntimeSpec {
    fn managed_families(&self) -> (bool, bool) {
        let managed_v4 = self.addresses.iter().any(|(address, _)| address.is_ipv4());
        let managed_v6 = self.addresses.iter().any(|(address, _)| address.is_ipv6());
        (managed_v4, managed_v6)
    }
}

fn next_retry(current: Option<usize>) -> usize {
    current
        .map(|index| index.saturating_add(1))
        .unwrap_or(0)
        .min(ROUTE_RETRY_DELAYS.len() - 1)
}

#[cfg(test)]
mod tests;
