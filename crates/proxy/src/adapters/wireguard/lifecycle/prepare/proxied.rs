//! The preserved TEMP outer-proxy peer constructor; no new provider/gate.
use super::*;
pub(super) struct ProxiedPreparation<'a> {
    pub outbound: &'a OutboundConfig,
    pub plan: &'a Arc<WireguardRawIpPlan>,
    pub context: &'a OutboundDevicePreparationContext,
    pub device_identity: [u8; 32],
    pub generation: u64,
    pub meters: &'a Option<(zero_engine::TrafficMeter, Vec<zero_engine::TrafficMeter>)>,
    pub outer_udp_proxy: Option<&'a str>,
}
impl WireguardAdapter {
    pub(super) async fn prepare_proxied_peers(
        &self,
        actual: ProxiedPreparation<'_>,
        staged: &mut crate::runtime::raw_ip::StagedRawIpDevices,
    ) -> Result<(), EngineError> {
        let ProxiedPreparation {
            outbound,
            plan,
            context,
            device_identity,
            generation,
            meters,
            outer_udp_proxy,
        } = actual;
        let upstream = &context.upstream;
        for peer_index in 0..plan.peer_count() {
            if staged.len() >= crate::runtime::raw_ip::MAX_RAW_IP_DEVICES {
                return Err(invalid("raw-IP device limit exceeded"));
            }
            if let Some(cell) =
                self.pool
                    .reusable_cell(&outbound.tag, peer_index, device_identity, generation)
            {
                staged.insert(
                    outbound.tag.clone(),
                    peer_index,
                    device_identity,
                    generation,
                    cell,
                    false,
                    outer_udp_proxy.is_some(),
                )?;
                continue;
            }
            let (host, port) = plan
                .peer_endpoint(peer_index)
                .ok_or_else(|| invalid("unknown WireGuard peer"))?;
            let address = match host.parse::<IpAddr>() {
                Ok(IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
                Ok(IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
                Err(_) => Address::Domain(host.to_owned()),
            };
            let endpoint = match upstream
                .resolve_node_address(&address, port, "WireGuard peer endpoint")
                .await
            {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    if matches!(address, Address::Domain(_)) {
                        self.pool.mark_endpoint_unresolved(
                            &outbound.tag,
                            peer_index,
                            device_identity,
                        );
                    }
                    return Err(error);
                }
            };
            self.pool
                .clear_endpoint_unresolved(&outbound.tag, peer_index, device_identity);
            let tunnel = plan.build_tunnel(peer_index)?;
            let meter = meters
                .as_ref()
                .map(|(endpoint, peers)| {
                    crate::runtime::raw_ip::RawIpTraffic::new(
                        endpoint.clone(),
                        peers.get(peer_index).cloned(),
                    )
                })
                .unwrap_or_default()
                .with_global(
                    context
                        .traffic
                        .traffic_meter(&zero_api::TrafficScope::Global),
                );
            let device = if let Some(proxy_tag) = outer_udp_proxy {
                    let operation = context.packet_paths.get(proxy_tag).ok_or_else(|| {
                        invalid(format!(
                            "WireGuard outer_udp_proxy `{proxy_tag}` has no persistent bidirectional UDP packet path"
                        ))
                    })?;
                    let operation = &operation.operation;
                    let path = operation
                        .build_carrier(context.packet_path_services.clone())
                        .await?;
                    SharedRawIpDevice::start_metered(
                        plan.local_addresses(),
                        plan.mtu(),
                        endpoint,
                        Arc::new(ProxiedRawIpWireCarrier::new(
                            path,
                            operation.clone(),
                            context.packet_path_services.clone(),
                        )),
                        tunnel,
                        context.stack_options.clone().with_traffic(meter),
                    )
                } else {
                    return Err(invalid("plain standalone WireGuard already requires its actual standard Host root"));
                }
                .map_err(|error| invalid(format!("raw-IP device: {error:?}")))?;
            let cell = Arc::new(OnceCell::new());
            cell.set(device.clone())
                .map_err(|_| invalid("duplicate WireGuard peer device"))?;
            staged.insert(
                outbound.tag.clone(),
                peer_index,
                device_identity,
                generation,
                cell,
                true,
                outer_udp_proxy.is_some(),
            )?;
            device.wait_ready().await?;
        }
        Ok(())
    }
}
