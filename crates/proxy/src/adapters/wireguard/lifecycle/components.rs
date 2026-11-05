//! Production linked graph recipes and invocations in the actual candidate map.
use super::*;
use super::{
    pending_map::PendingEntries,
    recipe::{LinkedEndpointRecipe, LinkedRecipeSettings},
};
use std::collections::BTreeSet;
use zero_core::component::{ComponentId, NetworkGeneration};
use zero_platform_tokio::contract::{
    context::RequestIdentity,
    network::{CarrierContext, UpstreamBinding},
    CancellationToken,
};

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
        if outbounds.is_empty() {
            return Ok(BTreeSet::new());
        }
        let generation = generation
            .ok_or_else(|| invalid("plain linked device graph requires its actual host network"))?;
        let options = crate::runtime::raw_ip::RawIpStackOptions::from_config(
            &config.runtime.network.udp_receive_queue,
        );
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
        recipes
            .into_iter()
            .map(|recipe| recipe.into_retained_component(resources, &candidate))
            .collect()
    }
    pub(crate) async fn resolve_component_resources(
        &self,
        config: &zero_config::RuntimeConfig,
        services: &crate::protocol_registry::TcpRuntimeServices,
        deadline: tokio::time::Instant,
        cancel: CancellationToken,
    ) -> Result<(), EngineError> {
        let Some(candidate) = PendingEndpoints::staged(&self.pending_endpoints, config) else {
            return Ok(());
        };
        if !std::ptr::eq(services.snapshot().config().as_ref(), config) {
            return Err(invalid(
                "device resolution source differs from the actual candidate snapshot",
            ));
        }
        let upstream = services.upstream();
        let mut invocations = candidate.invocations.lock().await;
        for (tag, invocation) in invocations.iter_mut() {
            let outbound = config.outbounds.iter().find(|outbound|matches!(&outbound.protocol, OutboundProtocolConfig::Wireguard {inbound_tag:Some(inbound),outer_udp_proxy:None,..} if inbound == tag)).ok_or_else(||invalid("actual linked invocation has no candidate outbound"))?;
            let pending = candidate
                .get(tag)
                .ok_or_else(|| invalid("actual linked peer originals are missing"))?;
            let graph = pending
                .actual_graph()
                .ok_or_else(|| invalid("actual linked factory owner was released"))?;
            let plan = WireguardRawIpPlan::from_protocol(&outbound.protocol)?;
            let prepared = config
                .endpoint_bindings()
                .iter()
                .find(|binding| binding.canonical && binding.outbound_tags.contains(&outbound.tag))
                .and_then(|binding| {
                    services
                        .engine()
                        .prepare_endpoint_traffic(binding, &plan.peer_ids())
                });
            let traffic = (0..graph.peer_count())
                .map(|peer| {
                    prepared
                        .as_ref()
                        .map(|prepared| {
                            crate::runtime::raw_ip::RawIpTraffic::new(
                                prepared.endpoint().clone(),
                                prepared.peers().get(peer).cloned(),
                            )
                        })
                        .unwrap_or_default()
                        .with_global(
                            services
                                .engine()
                                .traffic_meter(&zero_api::TrafficScope::Global),
                        )
                })
                .collect();
            invocation
                .resolve(
                    &upstream,
                    crate::runtime::udp_dispatch::packet_owner::PacketCarrierDemand {
                        identity: RequestIdentity::Service(Arc::new(
                            zero_core::request::ServiceRequestFacts {
                                origin: zero_core::request::ServiceOrigin::Runtime,
                                purpose: zero_core::request::ServicePurpose::BackgroundControl,
                            },
                        )),
                        context: CarrierContext {
                            session: None,
                            deadline: Some(deadline),
                            cancel: cancel.clone(),
                        },
                        binding: UpstreamBinding {
                            prefix: Vec::new(),
                            dial: outbound.dial.to_policy(),
                            network_generation: NetworkGeneration(upstream.egress_generation()),
                        },
                    },
                )
                .await?;
            graph.apply_traffic(traffic)?;
            if let Some(prepared) = prepared {
                candidate
                    .traffic
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entry(tag.clone())
                    .or_insert(prepared);
            }
        }
        Ok(())
    }
    pub(super) async fn install_component_link(
        &self,
        candidate: &Arc<PendingEntries>,
        tag: &str,
        outbound: &OutboundConfig,
        staged: &mut crate::runtime::raw_ip::StagedRawIpDevices,
        identity: [u8; 32],
        generation: u64,
    ) -> Result<Arc<inbound::LinkedEndpoint>, EngineError> {
        let pending = candidate
            .get(tag)
            .ok_or_else(|| invalid("actual deferred linked peer originals are missing"))?;
        let link = candidate
            .invocations
            .lock()
            .await
            .get_mut(tag)
            .ok_or_else(|| invalid("actual linked invocation was not retained"))?
            .install(pending)?;
        let devices = link
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .devices
            .clone();
        for (peer, device) in devices.into_iter().enumerate() {
            let previous = self
                .pool
                .reusable_cell(&outbound.tag, peer, identity, generation)
                .filter(|cell| {
                    cell.get()
                        .is_some_and(|original| Arc::ptr_eq(original, &device))
                });
            let fresh = previous.is_none();
            let cell = match previous {
                Some(cell) => cell,
                None => {
                    let cell = Arc::new(OnceCell::new());
                    cell.set(device)
                        .map_err(|_| invalid("duplicate canonical peer device"))?;
                    cell
                }
            };
            staged.insert(
                outbound.tag.clone(),
                peer,
                identity,
                generation,
                cell,
                fresh,
                false,
            )?;
        }
        Ok(link)
    }
}

#[cfg(test)]
mod tests;
