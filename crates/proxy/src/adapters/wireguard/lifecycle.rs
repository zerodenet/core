//! Transactional device preparation; protocol config stays inside this adapter.

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
};

use tokio::sync::OnceCell;
use zero_config::{OutboundConfig, OutboundProtocolConfig};
use zero_core::Address;
use zero_engine::EngineError;

use super::{protocol_identity, udp::WireguardRawIpPlan, CachedProfile, WireguardAdapter};
use crate::{
    protocol_registry::{
        OutboundDeviceLifecycleCapability, PreparedOutboundDeviceState, UpstreamConnectServices,
    },
    runtime::raw_ip::{RawIpDevicePool, RawIpOutboundPlan, SharedRawIpDevice},
};

struct PreparedWireguardDevices {
    pool: Arc<RawIpDevicePool>,
    profiles: Arc<Mutex<HashMap<String, CachedProfile>>>,
    next_profiles: HashMap<String, CachedProfile>,
    staged: crate::runtime::raw_ip::StagedRawIpDevices,
}

impl PreparedOutboundDeviceState for PreparedWireguardDevices {
    fn publish(self: Box<Self>) {
        let Self {
            pool,
            profiles,
            next_profiles,
            staged,
        } = *self;
        let mut active = profiles.lock().unwrap_or_else(|error| error.into_inner());
        pool.publish(staged);
        *active = next_profiles;
    }
}

#[async_trait::async_trait]
impl OutboundDeviceLifecycleCapability for WireguardAdapter {
    async fn prepare_outbound_devices(
        &self,
        outbounds: &[&OutboundConfig],
        upstream: UpstreamConnectServices,
    ) -> Result<Box<dyn PreparedOutboundDeviceState>, EngineError> {
        let mut staged = self.pool.begin_stage();
        let mut profiles = HashMap::new();
        let generation = upstream.egress_generation();
        for outbound in outbounds {
            if !matches!(outbound.protocol, OutboundProtocolConfig::Wireguard { .. }) {
                continue;
            }
            let identity = protocol_identity(&outbound.protocol)
                .ok_or_else(|| invalid("cannot identify WireGuard outbound"))?;
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
            for peer_index in 0..plan.peer_count() {
                if staged.len() >= crate::runtime::raw_ip::MAX_RAW_IP_DEVICES {
                    return Err(invalid("raw-IP device limit exceeded"));
                }
                if let Some(cell) =
                    self.pool
                        .reusable_cell(&outbound.tag, peer_index, identity, generation)
                {
                    staged.insert(
                        outbound.tag.clone(),
                        peer_index,
                        identity,
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
                            self.pool
                                .mark_endpoint_unresolved(&outbound.tag, peer_index, identity);
                        }
                        return Err(error);
                    }
                };
                self.pool
                    .clear_endpoint_unresolved(&outbound.tag, peer_index, identity);
                let socket = upstream.bind_datagram_socket(endpoint).await?;
                let tunnel = plan.build_tunnel(peer_index)?;
                let device = SharedRawIpDevice::start(
                    plan.local_addresses(),
                    plan.mtu(),
                    endpoint,
                    socket,
                    tunnel,
                )
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
                    identity,
                    generation,
                    cell,
                    true,
                )?;
            }
            profiles.insert(outbound.tag.clone(), CachedProfile { identity, plan });
        }
        Ok(Box::new(PreparedWireguardDevices {
            pool: self.pool.clone(),
            profiles: self.profiles.clone(),
            next_profiles: profiles,
            staged,
        }))
    }

    fn shutdown_outbound_devices(&self) {
        self.pool.shutdown();
        self.profiles
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
