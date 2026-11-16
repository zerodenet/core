//! Production linked graph recipes and invocations in the actual candidate map.
use super::*;
mod cold;
mod install;
mod linked;
mod materials;
mod memory;
mod resolve;
use super::{
    pending_map::PendingEntries,
    recipe::{LinkedEndpointRecipe, LinkedRecipeSettings},
};
pub(in crate::adapters::wireguard) use cold::ColdGraphs;
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
        registry: &crate::protocol_registry::ProtocolRegistry,
        generation: Option<u64>,
        network: Option<Arc<crate::component::background::BackgroundNetwork>>,
    ) -> Result<BTreeSet<ComponentId>, EngineError> {
        self.prepare_component_resources_with_materials(
            config,
            resources,
            registry,
            generation,
            network,
            Default::default(),
        )
    }
    pub(crate) fn prepare_component_resources_with_materials(
        &self,
        config: &Arc<zero_config::RuntimeConfig>,
        resources: &mut crate::component::ResourcePreparation<'_>,
        registry: &crate::protocol_registry::ProtocolRegistry,
        generation: Option<u64>,
        network: Option<Arc<crate::component::background::BackgroundNetwork>>,
        inherited: super::recipe::outer::OuterMaterials,
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
                        ..
                    }
                )
            })
            .filter(|outbound| resources.named_component(&outbound.tag).is_some())
            .filter(|outbound| {
                super::source_selection::outer_members_selected(outbound, config, |member| {
                    resources.named_component(&member.tag).is_some()
                })
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
                    ..
                }
            ) && resources.named_component(&outbound.tag).is_some()
        });
        if outbounds.is_empty() && !has_standalone {
            return Ok(BTreeSet::new());
        }
        let generation = generation
            .ok_or_else(|| invalid("plain linked device graph requires its actual host network"))?;
        let options = crate::runtime::raw_ip::RawIpStackOptions::from_config_with_packet_root(
            &config.runtime.network.udp_receive_queue,
            self.packet_return_totals
                .get()
                .ok_or_else(|| invalid("WireGuard assembly has no original Kernel packet Root"))?
                .clone(),
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
        .with_packet_driver_metadata_limits(
            config
                .runtime
                .effective_packet_driver_metadata_total_max_allocations(),
            config
                .runtime
                .effective_packet_driver_metadata_total_max_bytes(),
        )
        .with_packet_return_rows(&config.runtime)
        .with_packet_return_limits(
            config.runtime.effective_packet_return_max_frames(),
            config.runtime.effective_packet_return_max_bytes(),
        )
        .with_packet_return_totals(
            self.packet_return_totals
                .get()
                .ok_or_else(|| invalid("WireGuard assembly has no original Kernel packet Root"))?
                .clone(),
            config.runtime.effective_packet_return_total_max_frames(),
            config.runtime.effective_packet_return_total_max_bytes(),
        );
        let mut outer_materials = self.retained_outer_materials(config)?;
        outer_materials.extend(inherited);
        // Dependency order applies to cold decoding too: an outer consumer
        // may decode only the actual already prepared WG donor graph. This
        // scoped table holds those same owners, and is cleared on every exit.
        let _cold = self.begin_cold_graphs(config)?;
        let ordered = super::source_selection::ordered_devices(config, resources)?;
        let mut standalone = HashMap::new();
        let mut deferred = BTreeSet::new();
        let mut entries = HashMap::new();
        let mut invocations = Vec::new();
        let mut declared_peers = 0usize;
        for outbound in ordered {
            if matches!(
                outbound.protocol,
                OutboundProtocolConfig::Wireguard {
                    inbound_tag: None,
                    ..
                }
            ) {
                let (roots, ids) = self.prepare_standalone_resources(
                    config,
                    resources,
                    NetworkGeneration(generation),
                    network.as_ref().ok_or_else(|| {
                        invalid("plain WireGuard requires its actual host network")
                    })?,
                    &options,
                    registry,
                    &mut outer_materials,
                    Some(outbound.tag()),
                )?;
                for (tag, candidate) in roots {
                    declared_peers = declared_peers
                        .checked_add(candidate.root.peers.len())
                        .ok_or_else(|| invalid("WireGuard peer bound overflow"))?;
                    self.remember_cold_graph(
                        &tag,
                        super::recipe::carrier::Graph::Standalone(candidate.root.clone()),
                    )?;
                    standalone.insert(tag, candidate);
                }
                deferred.extend(ids);
            } else {
                let recipe = self.prepare_linked_recipe(
                    outbound,
                    config,
                    resources,
                    registry,
                    generation,
                    &network,
                    &options,
                    &mut outer_materials,
                )?;
                let tag = match &outbound.protocol {
                    OutboundProtocolConfig::Wireguard {
                        inbound_tag: Some(tag),
                        ..
                    } => tag.clone(),
                    _ => unreachable!(),
                };
                let pending = recipe.pending();
                declared_peers = declared_peers
                    .checked_add(pending.client_peer_count())
                    .ok_or_else(|| invalid("WireGuard peer bound overflow"))?;
                let ready = pending.ready().is_some();
                let (id, invocation) = recipe.into_component(resources)?;
                let graph = pending
                    .actual_graph()
                    .ok_or_else(|| invalid("prepared WG graph disappeared"))?;
                self.remember_cold_graph(
                    outbound.tag(),
                    super::recipe::carrier::Graph::Linked(graph),
                )?;
                let retained = if ready {
                    super::recipe::retained::RetainedInvocation::Installed {
                        binding: invocation.binding(),
                    }
                } else {
                    super::recipe::retained::RetainedInvocation::Prepared(Box::new(invocation))
                };
                if entries.insert(tag.clone(), pending).is_some() {
                    return Err(invalid("linked inbound has duplicate actual graph owners"));
                }
                invocations.push((tag, retained));
                deferred.insert(id);
            }
            if declared_peers > crate::runtime::raw_ip::MAX_RAW_IP_DEVICES {
                return Err(invalid(
                    "actual cold WireGuard peer union exceeds the device bound",
                ));
            }
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
        for (tag, invocation) in invocations {
            candidate
                .invocations
                .try_lock()
                .map_err(std::io::Error::other)?
                .entry(tag)
                .or_insert(invocation);
        }
        Ok(deferred)
    }
}

#[cfg(test)]
mod tests;
