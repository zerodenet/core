//! Shared WireGuard endpoint preparation. The listener owns the only wire
//! socket and protocol session; outbound devices own only client IP stacks.

use std::{net::IpAddr, sync::Arc};

use tokio::sync::{mpsc, watch, OnceCell};
use zero_config::{InboundConfig, OutboundConfig, OutboundProtocolConfig};
use zero_core::Address;
use zero_engine::EngineError;

use super::super::inbound_protocol_identity;
use super::{inbound, invalid, WireguardAdapter, WireguardRawIpPlan};
use crate::{
    protocol_registry::{OutboundDevicePreparationContext, UpstreamConnectServices},
    runtime::raw_ip::{
        ProxiedRawIpWireCarrier, RawIpWireCarrier, SharedRawIpDevice, StagedRawIpDevices,
    },
};

impl WireguardAdapter {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn prepare_linked_endpoint(
        &self,
        outbound: &OutboundConfig,
        inbound: &InboundConfig,
        plan: &Arc<WireguardRawIpPlan>,
        context: &OutboundDevicePreparationContext,
        staged: &mut StagedRawIpDevices,
        identity: [u8; 32],
        generation: u64,
    ) -> Result<
        (
            Arc<inbound::LinkedEndpoint>,
            Option<inbound::LinkedEndpointUpdate>,
        ),
        EngineError,
    > {
        let upstream = &context.upstream;
        let client_peer_count = if context.disabled_outbounds.contains(&outbound.tag) {
            0
        } else {
            plan.peer_count()
        };
        let inbound_identity = inbound_protocol_identity(&inbound.protocol)
            .ok_or_else(|| invalid("cannot identify WireGuard inbound"))?;
        let outer_udp_proxy = match &outbound.protocol {
            OutboundProtocolConfig::Wireguard {
                outer_udp_proxy, ..
            } => outer_udp_proxy.as_deref(),
            _ => None,
        };
        let existing = self
            .linked_endpoints
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&inbound.tag)
            .filter(|link| link.listen == inbound.listen)
            .cloned();
        let reusable = existing.as_ref().and_then(|link| {
            let state = link.state.lock().unwrap_or_else(|error| error.into_inner());
            (state.identity == identity
                && state.generation == generation
                && state.devices.len() == client_peer_count)
                .then(|| state.devices.clone())
        });
        if let (Some(link), Some(devices)) = (&existing, reusable) {
            let cells = devices
                .iter()
                .enumerate()
                .map(|(peer_index, device)| {
                    self.pool
                        .reusable_cell(&outbound.tag, peer_index, identity, generation)
                        .filter(|cell| {
                            cell.get()
                                .is_some_and(|current| Arc::ptr_eq(current, device))
                        })
                })
                .collect::<Option<Vec<_>>>();
            if let Some(cells) = cells {
                for (peer_index, cell) in cells.into_iter().enumerate() {
                    staged.insert(
                        outbound.tag.clone(),
                        peer_index,
                        identity,
                        generation,
                        cell,
                        false,
                    )?;
                }
                let endpoints = resolve_endpoints(plan, upstream).await?;
                let carriers = prepare_carriers(outer_udp_proxy, context, &endpoints).await?;
                return Ok((
                    link.clone(),
                    Some(inbound::LinkedEndpointUpdate {
                        link: link.clone(),
                        identity,
                        generation,
                        device: inbound::prepare_device(inbound)?,
                        inbound_identity,
                        devices,
                        endpoints,
                        carriers,
                    }),
                ));
            }
        }

        let (sender_watch, packets) = if let Some(link) = &existing {
            (link.sender.clone(), None)
        } else {
            let (sender, packets) = mpsc::channel(256);
            let (sender_watch, _) = watch::channel(sender);
            (sender_watch, Some(packets))
        };
        let sender_rx = sender_watch.subscribe();
        let endpoints = resolve_endpoints(plan, upstream).await?;
        let mut devices = Vec::with_capacity(plan.peer_count());
        for peer_index in 0..client_peer_count {
            let device = SharedRawIpDevice::start_on_endpoint(
                plan.local_addresses(),
                plan.mtu(),
                peer_index,
                sender_rx.clone(),
            )
            .map_err(|error| invalid(format!("raw-IP endpoint stack: {error:?}")))?;
            let cell = Arc::new(OnceCell::new());
            cell.set(device.clone())
                .map_err(|_| invalid("duplicate WireGuard peer device"))?;
            staged.insert(
                outbound.tag.clone(),
                peer_index,
                identity,
                generation,
                cell,
                true,
            )?;
            devices.push(device);
        }
        let carriers = prepare_carriers(outer_udp_proxy, context, &endpoints).await?;
        let device = inbound::prepare_device(inbound)?;
        if let Some(link) = existing {
            return Ok((
                link.clone(),
                Some(inbound::LinkedEndpointUpdate {
                    link,
                    identity,
                    generation,
                    device,
                    inbound_identity,
                    devices,
                    endpoints,
                    carriers,
                }),
            ));
        }
        let live = self
            .inbound_devices
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&inbound.tag)
            .and_then(std::sync::Weak::upgrade)
            .filter(|live| live.matches_identity(inbound_identity))
            .unwrap_or_else(|| Arc::new(inbound::LiveInboundDevice::new(device, inbound_identity)));
        Ok((
            Arc::new(inbound::LinkedEndpoint::new(
                identity,
                generation,
                inbound.listen.clone(),
                live,
                devices,
                endpoints,
                carriers,
                sender_watch,
                packets.expect("new link owns packet receiver"),
            )),
            None,
        ))
    }
}

async fn prepare_carriers(
    proxy_tag: Option<&str>,
    context: &OutboundDevicePreparationContext,
    endpoints: &[Option<std::net::SocketAddr>],
) -> Result<Vec<Option<Arc<dyn RawIpWireCarrier>>>, EngineError> {
    let Some(proxy_tag) = proxy_tag else {
        return Ok(vec![None; endpoints.len()]);
    };
    if endpoints.iter().any(Option::is_none) {
        return Err(invalid(
            "outer_udp_proxy requires a configured address for every peer",
        ));
    }
    let operation = context.packet_paths.get(proxy_tag).ok_or_else(|| {
        invalid(format!(
        "WireGuard outer_udp_proxy `{proxy_tag}` has no persistent bidirectional UDP packet path"
    ))
    })?;
    let mut carriers = Vec::with_capacity(endpoints.len());
    for _ in endpoints {
        let path = operation
            .build_carrier(context.packet_path_services.clone())
            .await?;
        carriers.push(Some(Arc::new(ProxiedRawIpWireCarrier::new(
            path,
            operation.clone(),
            context.packet_path_services.clone(),
        )) as Arc<dyn RawIpWireCarrier>));
    }
    Ok(carriers)
}

async fn resolve_endpoints(
    plan: &WireguardRawIpPlan,
    upstream: &UpstreamConnectServices,
) -> Result<Vec<Option<std::net::SocketAddr>>, EngineError> {
    let mut endpoints = Vec::with_capacity(plan.peer_count());
    for peer_index in 0..plan.peer_count() {
        let Some((host, port)) = plan.peer_endpoint(peer_index) else {
            endpoints.push(None);
            continue;
        };
        let address = match host.parse::<IpAddr>() {
            Ok(IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
            Ok(IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
            Err(_) => Address::Domain(host.to_owned()),
        };
        endpoints.push(Some(
            upstream
                .resolve_node_address(&address, port, "WireGuard peer endpoint")
                .await?,
        ));
    }
    Ok(endpoints)
}
