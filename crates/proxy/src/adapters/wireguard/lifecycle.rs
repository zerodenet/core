//! Transactional device preparation; protocol config stays inside this adapter.

mod borrowers;
mod components;
mod directions;
mod linked;
mod pending;
pub(super) mod pending_map;
mod prepare;
mod publication;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
    )
)]
mod recipe;
pub(crate) use recipe::alias::LinkedOutboundFactory;
mod selection;
mod standalone;
pub(super) use pending::{PeerResources, PendingLinkedEndpoint};
use pending_map::PendingEndpoints;
pub(super) use standalone::StandaloneStackOptions;

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
    pending: Option<PendingEndpoints>,
    traffic: Vec<zero_engine::PreparedEndpointTraffic>,
    published: bool,
    stopped: Vec<Arc<SharedRawIpDevice>>,
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
    #[cfg(test)]
    pub(crate) async fn prepare_devices(
        &self,
        outbounds: &[&OutboundConfig],
        inbounds: &[InboundConfig],
        context: OutboundDevicePreparationContext,
    ) -> Result<PreparedWireguardDevices, EngineError> {
        self.prepare_devices_inner(outbounds, inbounds, context, None)
            .await
    }
    pub(crate) async fn prepare_devices_for_candidate(
        &self,
        config: &zero_config::RuntimeConfig,
        outbounds: &[&OutboundConfig],
        inbounds: &[InboundConfig],
        context: OutboundDevicePreparationContext,
    ) -> Result<PreparedWireguardDevices, EngineError> {
        let cold = PendingEndpoints::staged(&self.pending_endpoints, config);
        if outbounds.iter().any(|outbound| match &outbound.protocol {
            OutboundProtocolConfig::Wireguard {
                inbound_tag: Some(tag),
                outer_udp_proxy: None,
                ..
            } => !cold
                .as_ref()
                .is_some_and(|candidate| candidate.contains_key(tag)),
            OutboundProtocolConfig::Wireguard {
                inbound_tag: None,
                outer_udp_proxy: None,
                ..
            } => !cold.as_ref().is_some_and(|candidate| {
                candidate
                    .standalone
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .contains_key(&outbound.tag)
            }),
            _ => false,
        }) {
            return Err(invalid(
                "plain linked WireGuard requires its actual cold component candidate",
            ));
        }
        self.prepare_devices_inner(outbounds, inbounds, context, cold)
            .await
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
