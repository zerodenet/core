use std::io;
use std::net::IpAddr;

use zero_tun::SystemRouteGuard;

use super::RouteRuntimeSpec;
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
        .map(|guard| guard.egress().clone());
    let egress_v6 = guards
        .iter()
        .find(|guard| guard.is_ipv6())
        .map(|guard| guard.egress().clone());
    for (ipv6, egress) in [(false, egress_v4.as_ref()), (true, egress_v6.as_ref())] {
        let interface = egress
            .map(|egress| {
                zero_platform_tokio::EgressInterface::new(egress.name().to_owned(), egress.index())
            })
            .transpose()?;
        proxy.egress_interface.replace_for(ipv6, interface);
    }
    info.egress_interface_v4 = egress_v4.map(|egress| egress.name().to_owned());
    info.egress_interface_v6 = egress_v6.map(|egress| egress.name().to_owned());
    info.egress_interface = if spec.primary_ipv6 {
        info.egress_interface_v6.clone()
    } else {
        info.egress_interface_v4.clone()
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
    withdraw_managed_egress(&proxy.egress_interface, managed_v4, managed_v6);

    if managed_v4 {
        info.egress_interface_v4 = None;
    }
    if managed_v6 {
        info.egress_interface_v6 = None;
    }
    info.egress_interface = if spec.primary_ipv6 {
        info.egress_interface_v6.clone()
    } else {
        info.egress_interface_v4.clone()
    };
    info.healthy = false;
    info.last_error = Some(error);
}

fn withdraw_managed_egress(
    control: &zero_platform_tokio::EgressInterfaceControl,
    managed_v4: bool,
    managed_v6: bool,
) {
    if managed_v4 {
        control.replace_for(false, None);
    }
    if managed_v6 {
        control.replace_for(true, None);
    }
}

pub(super) fn route_names(guards: &[SystemRouteGuard]) -> (Option<String>, Option<String>) {
    let v4 = guards
        .iter()
        .find(|guard| !guard.is_ipv6())
        .map(|guard| guard.egress().name().to_owned());
    let v6 = guards
        .iter()
        .find(|guard| guard.is_ipv6())
        .map(|guard| guard.egress().name().to_owned());
    (v4, v6)
}

#[cfg(test)]
mod tests;
