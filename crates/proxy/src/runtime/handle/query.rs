use super::model::ProxyHandle;

impl zero_api::QueryService for ProxyHandle {
    fn query(
        &self,
        request: zero_api::QueryRequest,
    ) -> zero_api::ApiResult<zero_api::QueryResponse> {
        if let zero_api::QueryRequest::Capabilities(_) = &request {
            let response = self.inner.query(request)?;
            let zero_api::QueryResponse::Capabilities(mut capabilities) = response else {
                return Ok(response);
            };
            capabilities.protocols = self.proxy.protocols.protocol_capabilities();
            return Ok(zero_api::QueryResponse::Capabilities(capabilities));
        }
        if let zero_api::QueryRequest::TunStatus(_) = &request {
            let info = self.proxy.tun_info.lock().unwrap();
            tracing::debug!(running = info.is_some(), "querying TUN runtime state");
            let ipv4_egress = tun_family_egress_snapshot(&self.proxy.egress_interface, false);
            let ipv6_egress = tun_family_egress_snapshot(&self.proxy.egress_interface, true);
            let network_generation = self.proxy.egress_interface.generation();
            let address_family_policy = Some(
                self.proxy
                    .resolver
                    .address_family_policy()
                    .as_str()
                    .to_owned(),
            );
            let ipv6_to_ipv4_fallbacks = self.proxy.egress_interface.ipv6_to_ipv4_fallbacks();
            let snap = match info.as_ref() {
                Some(tun) => zero_api::TunStatusSnapshot {
                    running: true,
                    name: Some(tun.name.clone()),
                    addr: Some(tun.addr.clone()),
                    addresses: tun.addresses.clone(),
                    mtu: Some(tun.mtu),
                    tag: Some(tun.tag.clone()),
                    healthy: tun.healthy,
                    auto_route: tun.auto_route,
                    include_cidrs: tun.include_cidrs.iter().map(ToString::to_string).collect(),
                    exclude_cidrs: tun.exclude_cidrs.iter().map(ToString::to_string).collect(),
                    dual_stack: tun.dual_stack,
                    strict_route: tun.strict_route,
                    dns_hijack: tun.dns_hijack,
                    egress_interface: tun.egress_interface.clone(),
                    egress_interface_v4: tun.egress_interface_v4.clone(),
                    egress_interface_v6: tun.egress_interface_v6.clone(),
                    ipv4_egress,
                    ipv6_egress,
                    network_generation,
                    address_family_policy,
                    ipv6_to_ipv4_fallbacks,
                    last_error: tun.last_error.clone(),
                    managed_by_config: tun.managed_config.is_some(),
                },
                None => zero_api::TunStatusSnapshot {
                    last_error: self.proxy.tun_last_error.lock().unwrap().clone(),
                    ipv4_egress,
                    ipv6_egress,
                    network_generation,
                    address_family_policy,
                    ipv6_to_ipv4_fallbacks,
                    ..Default::default()
                },
            };
            return Ok(zero_api::QueryResponse::TunStatus(snap));
        }
        self.inner.query(request)
    }
}

fn tun_family_egress_snapshot(
    control: &zero_platform_tokio::EgressInterfaceControl,
    ipv6: bool,
) -> zero_api::TunFamilyEgressSnapshot {
    let snapshot = control.snapshot_for(ipv6);
    let (availability, interface, reason) =
        match (snapshot.interface(), snapshot.unavailable_reason()) {
            (Some(interface), _) => (
                zero_api::TunFamilyEgressAvailability::Available,
                Some(interface.name().to_owned()),
                None,
            ),
            (None, Some(reason)) => (
                zero_api::TunFamilyEgressAvailability::Unavailable,
                None,
                Some(reason.to_owned()),
            ),
            (None, None) => (zero_api::TunFamilyEgressAvailability::Unknown, None, None),
        };
    zero_api::TunFamilyEgressSnapshot {
        availability,
        interface,
        reason,
    }
}

#[cfg(test)]
mod tests;
