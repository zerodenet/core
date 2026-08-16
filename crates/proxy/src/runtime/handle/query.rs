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
                    dual_stack: tun.dual_stack,
                    strict_route: tun.strict_route,
                    dns_hijack: tun.dns_hijack,
                    egress_interface: tun.egress_interface.clone(),
                    egress_interface_v4: tun.egress_interface_v4.clone(),
                    egress_interface_v6: tun.egress_interface_v6.clone(),
                    last_error: tun.last_error.clone(),
                    managed_by_config: tun.managed_config.is_some(),
                },
                None => zero_api::TunStatusSnapshot {
                    last_error: self.proxy.tun_last_error.lock().unwrap().clone(),
                    ..Default::default()
                },
            };
            return Ok(zero_api::QueryResponse::TunStatus(snap));
        }
        self.inner.query(request)
    }
}

#[cfg(test)]
mod tests;
