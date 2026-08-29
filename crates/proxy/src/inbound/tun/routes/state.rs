use std::io;
use std::net::IpAddr;

use zero_tun::{strict_route_socket_mark, FamilyEgressState, SystemRouteGuard};

use super::{publish_family_egress, RouteRuntimeSpec};
use crate::runtime::Proxy;

pub(super) fn publish_state(
    proxy: &Proxy,
    spec: &RouteRuntimeSpec,
    guards: &[SystemRouteGuard],
    exclusions: Option<Vec<IpAddr>>,
    error: Option<String>,
) -> io::Result<()> {
    let mut info = proxy.tun_info.lock().unwrap();
    let Some(info) = info.as_mut().filter(|info| info.id == spec.id) else {
        return Ok(());
    };
    let egress_v4 = guards
        .iter()
        .find(|guard| !guard.is_ipv6())
        .map(SystemRouteGuard::family_egress);
    let egress_v6 = guards
        .iter()
        .find(|guard| guard.is_ipv6())
        .map(SystemRouteGuard::family_egress);
    let socket_mark = spec
        .strict_route
        .then(|| strict_route_socket_mark(&spec.recovery_key));
    for (ipv6, egress) in [(false, egress_v4), (true, egress_v6)] {
        match egress {
            Some(state) => {
                publish_family_egress(&proxy.egress_interface, ipv6, state, socket_mark)?
            }
            None => proxy.egress_interface.replace_for(ipv6, None),
        }
    }
    info.egress_interface_v4 = egress_v4
        .and_then(FamilyEgressState::available_interface)
        .map(|egress| egress.name().to_owned());
    info.egress_interface_v6 = egress_v6
        .and_then(FamilyEgressState::available_interface)
        .map(|egress| egress.name().to_owned());
    info.egress_interface = if spec.primary_ipv6 {
        info.egress_interface_v6
            .clone()
            .or_else(|| info.egress_interface_v4.clone())
    } else {
        info.egress_interface_v4
            .clone()
            .or_else(|| info.egress_interface_v6.clone())
    };
    if let Some(exclusions) = exclusions {
        info.route_exclusions = exclusions;
    }
    info.healthy = error.is_none() && guards.len() == spec.addresses.len();
    info.last_error = error;
    Ok(())
}

pub(super) fn publish_error(proxy: &Proxy, id: u64, error: String) {
    let mut info = proxy.tun_info.lock().unwrap();
    if let Some(info) = info.as_mut().filter(|info| info.id == id) {
        info.healthy = false;
        info.last_error = Some(error);
    }
}

pub(super) fn publish_unavailable(proxy: &Proxy, spec: &RouteRuntimeSpec, error: String) {
    let mut info = proxy.tun_info.lock().unwrap();
    let Some(info) = info.as_mut().filter(|info| info.id == spec.id) else {
        return;
    };
    let (managed_v4, managed_v6) = spec.managed_families();
    withdraw_managed_egress(&proxy.egress_interface, managed_v4, managed_v6, &error);

    if managed_v4 {
        info.egress_interface_v4 = None;
    }
    if managed_v6 {
        info.egress_interface_v6 = None;
    }
    info.egress_interface = if spec.primary_ipv6 {
        info.egress_interface_v6
            .clone()
            .or_else(|| info.egress_interface_v4.clone())
    } else {
        info.egress_interface_v4
            .clone()
            .or_else(|| info.egress_interface_v6.clone())
    };
    info.healthy = false;
    info.last_error = Some(error);
}

fn withdraw_managed_egress(
    control: &zero_platform_tokio::EgressInterfaceControl,
    managed_v4: bool,
    managed_v6: bool,
    error: &str,
) {
    if managed_v4 {
        control.mark_unavailable_for(false, error);
    }
    if managed_v6 {
        control.mark_unavailable_for(true, error);
    }
}

pub(super) fn route_names(guards: &[SystemRouteGuard]) -> (Option<String>, Option<String>) {
    let v4 = guards
        .iter()
        .find(|guard| !guard.is_ipv6())
        .and_then(|guard| guard.family_egress().available_interface())
        .map(|egress| egress.name().to_owned());
    let v6 = guards
        .iter()
        .find(|guard| guard.is_ipv6())
        .and_then(|guard| guard.family_egress().available_interface())
        .map(|egress| egress.name().to_owned());
    (v4, v6)
}

#[cfg(test)]
mod tests;
