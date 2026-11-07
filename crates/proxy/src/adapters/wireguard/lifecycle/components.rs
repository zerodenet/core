//! Production linked graph recipes and invocations in the actual candidate map.
use super::*;
mod install;
mod memory;
mod resolve;
use super::{
    pending_map::PendingEntries,
    recipe::{LinkedEndpointRecipe, LinkedRecipeSettings},
};
use std::collections::BTreeSet;
use zero_core::component::{ComponentId, NetworkGeneration};

impl WireguardAdapter {
    pub(crate) async fn close_discarded_devices(&self) -> Result<(), EngineError> {
        self.pool.close_discarded().await
    }
    /// After the host drove exact discarded slots through their actual joins.
    /// Cold or borrowed wrappers can be released without revoking current work.
    pub(crate) fn release_component_resources(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<(), EngineError> {
        let current = self
            .pending_endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        match current {
            Some(candidate) if candidate.candidate_matches(config) => {
                PendingEndpoints::release_closed(&self.pending_endpoints, &candidate)
            }
            Some(_) => Err(invalid(
                "device cleanup belongs to another actual candidate",
            )),
            None => Ok(()),
        }
    }
    pub(crate) fn prepare_component_resources(
        &self,
        config: &Arc<zero_config::RuntimeConfig>,
        resources: &mut crate::component::ResourcePreparation<'_>,
        generation: Option<u64>,
        network: Option<Arc<crate::component::background::BackgroundNetwork>>,
    ) -> Result<BTreeSet<ComponentId>, EngineError> {
        let admission = zero_engine::EndpointAdmission::from_config(config);
        let outbounds = config
            .outbounds
            .iter()
            .filter(|outbound| admission.device_enabled(&outbound.tag))
            .filter(|outbound| {
                matches!(
                    outbound.protocol,
                    OutboundProtocolConfig::Wireguard {
                        inbound_tag: Some(_),
                        outer_udp_proxy: None,
                        ..
                    }
                )
            })
            .collect::<Vec<_>>();
        let previous = self
            .pending_endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(previous) = previous {
            if !previous.candidate_matches(config) {
                PendingEndpoints::release_closed(&self.pending_endpoints, &previous)?;
            }
        }
        let has_standalone = config.outbounds.iter().any(|outbound| {
            matches!(
                outbound.protocol,
                OutboundProtocolConfig::Wireguard {
                    inbound_tag: None,
                    outer_udp_proxy: None,
                    ..
                }
            )
        });
        if outbounds.is_empty() && !has_standalone {
            return Ok(BTreeSet::new());
        }
        let generation = generation
            .ok_or_else(|| invalid("plain linked device graph requires its actual host network"))?;
        let options = crate::runtime::raw_ip::RawIpStackOptions::from_config(
            &config.runtime.network.udp_receive_queue,
        )
        .with_packet_wire_memory(
            zero_stack::packet_output::memory::PacketWireMemory::PaddedEnvelope {
                multiple: 16,
                overhead: 32,
            },
        )
        .with_packet_native_read_limits(
            config
                .runtime
                .effective_packet_native_read_total_max_frames(),
            config
                .runtime
                .effective_packet_native_read_total_max_bytes(),
        )
        .with_packet_transmit_limits(
            config.runtime.effective_packet_transmit_total_max_frames(),
            config.runtime.effective_packet_transmit_total_max_bytes(),
        )
        .with_packet_return_limits(
            config.runtime.effective_packet_return_max_frames(),
            config.runtime.effective_packet_return_max_bytes(),
        )
        .with_packet_return_totals(
            self.packet_return_totals
                .get_or_init(Default::default)
                .clone(),
            config.runtime.effective_packet_return_total_max_frames(),
            config.runtime.effective_packet_return_total_max_bytes(),
        );
        let (standalone, mut deferred) = self.prepare_standalone_resources(
            config,
            resources,
            NetworkGeneration(generation),
            network
                .as_ref()
                .ok_or_else(|| invalid("plain WireGuard requires its actual host network"))?,
            &options,
        )?;
        let mut declared_peers = standalone
            .values()
            .try_fold(0_usize, |used, candidate| {
                used.checked_add(candidate.root.peers.len())
            })
            .filter(|used| *used <= crate::runtime::raw_ip::MAX_RAW_IP_DEVICES)
            .ok_or_else(|| invalid("actual cold WireGuard peer union exceeds the device bound"))?;
        let mut recipes = Vec::new();
        let mut entries = HashMap::new();
        for outbound in outbounds {
            let OutboundProtocolConfig::Wireguard {
                inbound_tag: Some(tag),
                ..
            } = &outbound.protocol
            else {
                unreachable!()
            };
            let inbound = config
                .inbounds
                .iter()
                .find(|inbound| &inbound.tag == tag)
                .ok_or_else(|| invalid("linked WireGuard inbound missing"))?;
            let plan = Arc::new(WireguardRawIpPlan::from_protocol(&outbound.protocol)?);
            let needs_node_dns = (0..plan.peer_count()).any(|peer| {
                plan.peer_endpoint(peer)
                    .is_some_and(|(host, _)| host.parse::<IpAddr>().is_err())
            });
            let dependencies = if needs_node_dns {
                let network = network.as_ref().ok_or_else(|| {
                    invalid("linked Domain peer requires its actual Node DNS owner")
                })?;
                let dns = crate::component::native_dns::prepare_node(resources, network.clone())
                    .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
                vec![(dns, resources.reusable_resource_instance(dns))]
            } else {
                Vec::new()
            };
            let identity = options.identity(
                protocol_identity(&outbound.protocol)
                    .ok_or_else(|| invalid("cannot identify linked WireGuard outbound"))?,
            );
            let peers = self.retained_peer_count(
                outbound,
                &config.inbounds,
                &plan,
                identity,
                generation,
                admission.outbound_denial(&outbound.tag).is_some(),
            )?;
            declared_peers = declared_peers
                .checked_add(peers)
                .filter(|used| *used <= crate::runtime::raw_ip::MAX_RAW_IP_DEVICES)
                .ok_or_else(|| {
                    invalid("actual cold WireGuard peer union exceeds the device bound")
                })?;
            let recipe = LinkedEndpointRecipe::prepare_with_resources(
                self,
                outbound,
                inbound,
                plan,
                LinkedRecipeSettings {
                    identity,
                    network_generation: NetworkGeneration(generation),
                    stacks: vec![options.clone(); peers],
                    dependencies,
                },
                resources,
            )?;
            if entries
                .insert(inbound.tag.clone(), recipe.pending())
                .is_some()
            {
                return Err(invalid("linked inbound has duplicate actual graph owners"));
            }
            recipes.push(recipe);
        }
        let candidate = match PendingEndpoints::staged(&self.pending_endpoints, config) {
            Some(candidate) => candidate,
            None => PendingEndpoints::stage(
                &self.pending_endpoints,
                PendingEntries::cold(config, entries),
            )?,
        };
        let previous = {
            let actual = candidate
                .standalone
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            actual
                .iter()
                .filter(|(tag, old)| {
                    standalone
                        .get(*tag)
                        .is_none_or(|new| !Arc::ptr_eq(&old.root, &new.root))
                })
                .map(|(_, candidate)| candidate.clone())
                .collect::<Vec<_>>()
        };
        for previous in &previous {
            previous.revoke();
            if previous.needs_cleanup() {
                return Err(invalid(
                    "previous standalone candidate retains its actual originals",
                ));
            }
        }
        // The actual host's cold pass is serial. No original/waker callback
        // runs while the candidate metadata lock is held.
        let retired = {
            let mut actual = candidate
                .standalone
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            std::mem::replace(&mut *actual, standalone)
        };
        drop(retired);
        for recipe in recipes {
            deferred.insert(recipe.into_retained_component(resources, &candidate)?);
        }
        Ok(deferred)
    }
}

#[cfg(test)]
mod tests;
