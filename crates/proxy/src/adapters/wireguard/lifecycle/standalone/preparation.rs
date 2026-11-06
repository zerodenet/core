//! The same cold namespace and candidate own exactly the root selected by Host.
use super::*;
#[derive(Clone)]
pub(in crate::adapters::wireguard::lifecycle) struct Candidate {
    pub(in crate::adapters::wireguard::lifecycle) root: Arc<StandaloneStackOptions>,
    owned: bool,
}
impl Candidate {
    pub(in crate::adapters::wireguard::lifecycle) fn revoke(&self) {
        if self.owned {
            self.root.revoke();
        }
    }
    pub(in crate::adapters::wireguard::lifecycle) fn needs_cleanup(&self) -> bool {
        self.owned && self.root.needs_cleanup()
    }
}
impl WireguardAdapter {
    pub(in crate::adapters::wireguard::lifecycle) fn prepare_standalone_resources(
        &self,
        config: &Arc<zero_config::RuntimeConfig>,
        resources: &mut crate::component::ResourcePreparation<'_>,
        generation: NetworkGeneration,
        network: &Arc<crate::component::background::BackgroundNetwork>,
        options: &RawIpStackOptions,
    ) -> Result<
        (
            HashMap<String, Candidate>,
            std::collections::BTreeSet<ComponentId>,
        ),
        EngineError,
    > {
        let mut roots = HashMap::new();
        let mut deferred = std::collections::BTreeSet::new();
        let admission = zero_engine::EndpointAdmission::from_config(config);
        for outbound in &config.outbounds {
            if !matches!(
                outbound.protocol,
                OutboundProtocolConfig::Wireguard {
                    inbound_tag: None,
                    outer_udp_proxy: None,
                    ..
                }
            ) {
                continue;
            }
            let plan = Arc::new(WireguardRawIpPlan::from_protocol(&outbound.protocol)?);
            let dependencies = if (0..plan.peer_count()).any(|peer| {
                plan.peer_endpoint(peer)
                    .is_some_and(|(host, _)| host.parse::<IpAddr>().is_err())
            }) {
                vec![
                    crate::component::native_dns::prepare_node(resources, network.clone())
                        .map_err(std::io::Error::other)?,
                ]
            } else {
                Vec::new()
            };
            let actual = dependencies
                .iter()
                .map(|id| resources.reusable_resource_instance(*id))
                .collect::<Option<Vec<_>>>();
            let fresh = Arc::new(StandaloneStackOptions::new(
                outbound,
                plan.clone(),
                options.clone(),
                generation,
                dependencies.clone(),
                plan.peer_count(),
            )?);
            let staged = pending_map::PendingEndpoints::staged(&self.pending_endpoints, config)
                .and_then(|candidate| {
                    candidate
                        .standalone
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .get(&outbound.tag)
                        .cloned()
                });
            let current = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&outbound.tag)
                .and_then(|profile| profile.graph.as_ref())
                .and_then(std::sync::Weak::upgrade)
                .map(|root| Candidate { root, owned: false });
            let reused = staged.or(current).filter(|candidate| {
                candidate.root.spec == fresh.spec
                    && actual
                        .as_ref()
                        .is_some_and(|actual| candidate.root.reusable_in(resources, actual))
            });
            let (candidate, fresh) = match reused {
                Some(current) => (current, false),
                None if !admission.device_enabled(&outbound.tag) => (
                    Candidate {
                        root: Arc::new(StandaloneStackOptions::new(
                            outbound,
                            plan,
                            options.clone(),
                            generation,
                            dependencies,
                            0,
                        )?),
                        owned: true,
                    },
                    true,
                ),
                None => (
                    Candidate {
                        root: fresh,
                        owned: true,
                    },
                    true,
                ),
            };
            let mut recipe = crate::component::ComponentRecipe::from_validated(
                Arc::new(component::Factory),
                candidate.root.clone(),
            );
            for index in 0..candidate.root.peers.len() {
                recipe = recipe
                    .with_native_datagram_binding(
                        index * 2,
                        network.egress.clone(),
                        outbound.dial.to_policy(),
                        generation,
                    )
                    .map_err(std::io::Error::other)?;
            }
            let id = resources
                .prepare_component_recipe(NAMESPACE, recipe)
                .map_err(std::io::Error::other)?;
            if candidate.root.resource.get_or_init(|| id) != &id {
                return Err(invalid(
                    "same standalone root was assigned a different actual resource",
                ));
            }
            if fresh {
                resources
                    .require_fresh_resource(id)
                    .map_err(std::io::Error::other)?;
            }
            deferred.insert(id);
            roots.insert(outbound.tag.clone(), candidate);
        }
        Ok((roots, deferred))
    }
    pub(in crate::adapters::wireguard::lifecycle) fn standalone_outbound_options(
        &self,
        outbound: &OutboundConfig,
        resources: &mut crate::component::ResourcePreparation<'_>,
    ) -> Result<super::super::recipe::alias::LinkedOutboundOptions, ChannelError> {
        let candidate = pending_map::PendingEndpoints::staged(
            &self.pending_endpoints,
            resources
                .candidate()
                .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?,
        )
        .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let root = candidate
            .standalone
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&outbound.tag)
            .map(|candidate| candidate.root.clone())
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let dependency = *root
            .resource
            .get()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        if !resources
            .resources()
            .any(|entry| entry.component() == dependency && entry.recipe().spec() == &root.spec)
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        super::super::recipe::alias::LinkedOutboundOptions::standalone(root)
    }
}
