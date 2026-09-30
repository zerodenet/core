//! Transactional device preparation; protocol config stays inside this adapter.

mod linked;

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
};

use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;
use zero_config::{InboundConfig, OutboundConfig, OutboundProtocolConfig};
use zero_core::Address;
use zero_engine::EngineError;

use super::{
    inbound, protocol_identity, udp::WireguardRawIpPlan, CachedProfile, LinkedEndpointMap,
    PendingEndpointMap, WireguardAdapter,
};
use crate::{
    protocol_registry::{
        OutboundDeviceLifecycleCapability, OutboundDevicePreparationContext,
        PreparedOutboundDeviceState,
    },
    runtime::raw_ip::{
        ProxiedRawIpWireCarrier, RawIpDevicePool, RawIpOutboundPlan, SharedRawIpDevice,
    },
};

struct PreparedWireguardDevices {
    pool: Arc<RawIpDevicePool>,
    profiles: Arc<Mutex<HashMap<String, CachedProfile>>>,
    next_profiles: HashMap<String, CachedProfile>,
    staged: crate::runtime::raw_ip::StagedRawIpDevices,
    linked: Arc<Mutex<LinkedEndpointMap>>,
    next_linked: LinkedEndpointMap,
    linked_updates: Vec<inbound::LinkedEndpointUpdate>,
    pending: PendingEndpoints,
}

struct PendingEndpoints(PendingEndpointMap);

impl crate::protocol_registry::EndpointControlCapability for WireguardAdapter {
    fn supports_endpoint_control(&self, binding: &zero_config::EndpointBindingConfig) -> bool {
        binding.protocol == "wireguard"
    }
}

impl Drop for PendingEndpoints {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(|error| error.into_inner()) = None;
    }
}

impl PreparedOutboundDeviceState for PreparedWireguardDevices {
    fn publish(self: Box<Self>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let Self {
            pool,
            profiles,
            next_profiles,
            staged,
            linked,
            next_linked,
            linked_updates,
            pending,
        } = *self;
        let mut active = profiles.lock().unwrap_or_else(|error| error.into_inner());
        for update in linked_updates {
            update.publish();
        }
        let stopped = pool.publish(staged);
        *active = next_profiles;
        *linked.lock().unwrap_or_else(|error| error.into_inner()) = next_linked;
        drop(pending);
        Box::pin(async move {
            for device in stopped {
                device.wait_stopped().await;
            }
        })
    }
}

#[async_trait::async_trait]
impl OutboundDeviceLifecycleCapability for WireguardAdapter {
    async fn prepare_outbound_devices(
        &self,
        outbounds: &[&OutboundConfig],
        inbounds: &[InboundConfig],
        context: OutboundDevicePreparationContext,
    ) -> Result<Box<dyn PreparedOutboundDeviceState>, EngineError> {
        let upstream = &context.upstream;
        let mut staged = self.pool.begin_stage();
        let mut profiles = HashMap::new();
        let mut linked = HashMap::new();
        let mut linked_updates = Vec::new();
        let generation = upstream.egress_generation();
        for outbound in outbounds {
            if !matches!(outbound.protocol, OutboundProtocolConfig::Wireguard { .. }) {
                continue;
            }
            let identity = protocol_identity(&outbound.protocol)
                .ok_or_else(|| invalid("cannot identify WireGuard outbound"))?;
            let outer_udp_proxy = match &outbound.protocol {
                OutboundProtocolConfig::Wireguard {
                    outer_udp_proxy, ..
                } => outer_udp_proxy.as_deref(),
                _ => None,
            };
            let device_identity = if let Some(proxy_tag) = outer_udp_proxy {
                let proxy_identity = context.packet_path_identities.get(proxy_tag).ok_or_else(|| {
                    invalid(format!(
                        "WireGuard outer_udp_proxy `{proxy_tag}` has no persistent bidirectional UDP packet path"
                    ))
                })?;
                let mut hash = Sha256::new();
                hash.update(identity);
                hash.update(proxy_identity);
                hash.finalize().into()
            } else {
                identity
            };
            let plan = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&outbound.tag)
                .filter(|cached| cached.identity == identity)
                .map(|cached| cached.plan.clone())
                .map(Ok)
                .unwrap_or_else(|| {
                    WireguardRawIpPlan::from_protocol(&outbound.protocol).map(Arc::new)
                })?;
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
                    )
                    .await?;
                if let Some(update) = update {
                    linked_updates.push(update);
                }
                linked.insert(inbound_tag.clone(), link);
                profiles.insert(
                    outbound.tag.clone(),
                    CachedProfile {
                        identity,
                        device_identity,
                        plan,
                    },
                );
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
                let device = if let Some(proxy_tag) = outer_udp_proxy {
                    let operation = context.packet_paths.get(proxy_tag).ok_or_else(|| {
                        invalid(format!(
                            "WireGuard outer_udp_proxy `{proxy_tag}` has no persistent bidirectional UDP packet path"
                        ))
                    })?;
                    let path = operation
                        .build_carrier(context.packet_path_services.clone())
                        .await?;
                    SharedRawIpDevice::start_with_carrier(
                        plan.local_addresses(),
                        plan.mtu(),
                        endpoint,
                        Arc::new(ProxiedRawIpWireCarrier::new(
                            path,
                            operation.clone(),
                            context.packet_path_services.clone(),
                        )),
                        tunnel,
                    )
                } else {
                    let socket = upstream.bind_datagram_socket(endpoint).await?;
                    SharedRawIpDevice::start(
                        plan.local_addresses(),
                        plan.mtu(),
                        endpoint,
                        socket,
                        tunnel,
                    )
                }
                .map_err(|error| invalid(format!("raw-IP device: {error:?}")))?;
                if let Err(error) = device.wait_ready().await {
                    device.close_now();
                    return Err(error);
                }
                let cell = Arc::new(OnceCell::new());
                cell.set(device)
                    .map_err(|_| invalid("duplicate WireGuard peer device"))?;
                staged.insert(
                    outbound.tag.clone(),
                    peer_index,
                    device_identity,
                    generation,
                    cell,
                    true,
                )?;
            }
            profiles.insert(
                outbound.tag.clone(),
                CachedProfile {
                    identity,
                    device_identity,
                    plan,
                },
            );
        }
        *self
            .pending_endpoints
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(linked.clone());
        Ok(Box::new(PreparedWireguardDevices {
            pool: self.pool.clone(),
            profiles: self.profiles.clone(),
            next_profiles: profiles,
            staged,
            linked: self.linked_endpoints.clone(),
            next_linked: linked,
            linked_updates,
            pending: PendingEndpoints(self.pending_endpoints.clone()),
        }))
    }

    fn shutdown_outbound_devices(&self) {
        self.pool.shutdown();
        self.profiles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        self.linked_endpoints
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }

    fn outbound_device_health(
        &self,
        outbounds: &[&OutboundConfig],
    ) -> Vec<zero_api::OutboundDeviceHealthSnapshot> {
        let mut snapshots = Vec::new();
        for outbound in outbounds {
            let OutboundProtocolConfig::Wireguard { peers, .. } = &outbound.protocol else {
                continue;
            };
            let Some(identity) = protocol_identity(&outbound.protocol) else {
                continue;
            };
            let identity = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&outbound.tag)
                .filter(|cached| cached.identity == identity)
                .map_or(identity, |cached| cached.device_identity);
            for peer_index in 0..peers.len() {
                snapshots.push(
                    self.pool
                        .health_snapshot(&outbound.tag, peer_index, identity)
                        .unwrap_or_else(|| zero_api::OutboundDeviceHealthSnapshot {
                            tag: outbound.tag.clone(),
                            peer_index,
                            ..Default::default()
                        }),
                );
            }
        }
        snapshots
    }
}

fn invalid(message: impl ToString) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.to_string(),
    ))
}
