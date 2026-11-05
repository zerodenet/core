//! Transactional device preparation; protocol config stays inside this adapter.

mod directions;
mod linked;
mod pending;
mod pending_map;
mod publication;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
    )
)]
mod recipe;
mod selection;
pub(super) use pending::{PeerResources, PendingLinkedEndpoint};
use pending_map::PendingEndpoints;

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
};

use tokio::sync::OnceCell;
use zero_config::{InboundConfig, OutboundConfig, OutboundProtocolConfig};
use zero_core::Address;
use zero_engine::EngineError;

use super::{
    inbound, protocol_identity, udp::WireguardRawIpPlan, CachedProfile, LinkedEndpointMap,
    PendingEndpointMap, WireguardAdapter,
};
use crate::{
    adapters::devices::OutboundDevicePreparationContext,
    runtime::raw_ip::{
        ProxiedRawIpWireCarrier, RawIpDevicePool, RawIpOutboundPlan, SharedRawIpDevice,
    },
};

pub(crate) struct PreparedWireguardDevices {
    pool: Arc<RawIpDevicePool>,
    profiles: Arc<Mutex<HashMap<String, CachedProfile>>>,
    next_profiles: HashMap<String, CachedProfile>,
    staged: crate::runtime::raw_ip::StagedRawIpDevices,
    linked: Arc<Mutex<LinkedEndpointMap>>,
    next_linked: LinkedEndpointMap,
    linked_updates: Vec<inbound::LinkedEndpointUpdate>,
    pending: PendingEndpoints,
    traffic: Vec<zero_engine::PreparedEndpointTraffic>,
}

impl crate::protocol_registry::EndpointControlCapability for WireguardAdapter {
    fn supports_endpoint_control(&self, binding: &zero_config::EndpointBindingConfig) -> bool {
        binding.protocol == "wireguard"
    }

    fn live_direction_contraction(
        &self,
        binding: &zero_config::EndpointBindingConfig,
    ) -> zero_api::EndpointDirections {
        binding.supported_directions
    }
}

impl WireguardAdapter {
    pub(crate) async fn prepare_devices(
        &self,
        outbounds: &[&OutboundConfig],
        inbounds: &[InboundConfig],
        context: OutboundDevicePreparationContext,
    ) -> Result<PreparedWireguardDevices, EngineError> {
        PendingEndpoints::ensure_available(&self.pending_endpoints)?;
        let upstream = &context.upstream;
        let mut profiles = HashMap::new();
        let mut linked = HashMap::new();
        let mut linked_updates = Vec::new();
        let mut traffic = Vec::new();
        let generation = upstream.egress_generation();
        let selected = self.select_devices(outbounds, &context)?;
        let desired = selected
            .iter()
            .map(|(outbound, _, identity, plan)| {
                let count = self.prepared_peer_count(
                    outbound, inbounds, plan, *identity, generation, &context,
                )?;
                Ok((outbound.tag.clone(), (*identity, generation, count)))
            })
            .collect::<Result<HashMap<_, _>, EngineError>>()?;
        // No candidate device/task/socket may start before these actual
        // originals (including discarded candidates) have confirmed cleanup.
        self.pool.retire_changed(&desired).await?;
        let mut staged = self.pool.begin_stage();
        for (outbound, identity, device_identity, plan) in selected {
            let outer_udp_proxy = match &outbound.protocol {
                OutboundProtocolConfig::Wireguard {
                    outer_udp_proxy, ..
                } => outer_udp_proxy.as_deref(),
                _ => None,
            };
            let prepared_traffic = context
                .endpoint_bindings
                .iter()
                .find(|b| b.canonical && b.outbound_tags.contains(&outbound.tag))
                .and_then(|binding| {
                    context
                        .traffic
                        .prepare_endpoint_traffic(binding, &plan.peer_ids())
                });
            let meters = prepared_traffic.as_ref().map(|p| (p.endpoint(), p.peers()));
            if let Some(prepared) = prepared_traffic {
                traffic.push(prepared);
            }
            if let OutboundProtocolConfig::Wireguard {
                inbound_tag: Some(inbound_tag),
                ..
            } = &outbound.protocol
            {
                let inbound = inbounds
                    .iter()
                    .find(|inbound| inbound.tag == *inbound_tag)
                    .ok_or_else(|| invalid("linked WireGuard inbound missing"))?;
                let (link, update) = self
                    .prepare_linked_endpoint(
                        outbound,
                        inbound,
                        &plan,
                        &context,
                        &mut staged,
                        device_identity,
                        generation,
                        desired[&outbound.tag].2,
                        meters.as_ref(),
                    )
                    .await?;
                if let Some(update) = update {
                    linked_updates.push(update);
                }
                linked.insert(inbound_tag.clone(), link);
                profiles.insert(
                    outbound.tag.clone(),
                    CachedProfile {
                        packet: self.packet_operations(
                            &outbound.tag,
                            device_identity,
                            plan.clone(),
                        ),
                        identity,
                        device_identity,
                        plan,
                    },
                );
                continue;
            }
            // A standalone device has no retained listener role. Revocation
            // removes its stacks/carrier through normal pool publication.
            if context.disabled_outbounds.contains(&outbound.tag) {
                continue;
            }
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
                    let socket = upstream.bind_datagram_socket(endpoint).await?;
                    SharedRawIpDevice::start_metered(
                        plan.local_addresses(),
                        plan.mtu(),
                        endpoint,
                        Arc::new(crate::runtime::raw_ip::DirectRawIpWireCarrier::new(socket)),
                        tunnel,
                        context.stack_options.clone().with_traffic(meter),
                    )
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
            profiles.insert(
                outbound.tag.clone(),
                CachedProfile {
                    packet: self.packet_operations(&outbound.tag, device_identity, plan.clone()),
                    identity,
                    device_identity,
                    plan,
                },
            );
        }
        let pending = PendingEndpoints::install(
            self.pending_endpoints.clone(),
            linked
                .iter()
                .map(|(tag, link)| {
                    Ok((
                        tag.clone(),
                        Arc::new(PendingLinkedEndpoint::from_ready(link.clone())?),
                    ))
                })
                .collect::<Result<HashMap<_, _>, EngineError>>()?,
        )?;
        Ok(PreparedWireguardDevices {
            pool: self.pool.clone(),
            profiles: self.profiles.clone(),
            next_profiles: profiles,
            staged,
            linked: self.linked_endpoints.clone(),
            next_linked: linked,
            linked_updates,
            traffic,
            pending,
        })
    }
}

fn invalid(message: impl ToString) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.to_string(),
    ))
}

#[cfg(test)]
mod tests;
