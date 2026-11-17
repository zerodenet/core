//! Prepare the actual linked graph before a consumer decodes its alias.
use super::*;
impl WireguardAdapter {
    #[expect(
        clippy::too_many_arguments,
        reason = "original generation, caller, native and budget inputs remain explicitly bound"
    )]
    pub(super) fn prepare_linked_recipe(
        &self,
        outbound: &OutboundConfig,
        config: &Arc<zero_config::RuntimeConfig>,
        resources: &mut crate::component::ResourcePreparation<'_>,
        registry: &crate::protocol_registry::ProtocolRegistry,
        generation: u64,
        network: &Option<Arc<crate::component::background::BackgroundNetwork>>,
        options: &crate::runtime::raw_ip::RawIpStackOptions,
        outer_materials: &mut super::super::recipe::outer::OuterMaterials,
    ) -> Result<LinkedEndpointRecipe, EngineError> {
        let admission = zero_engine::EndpointAdmission::from_config(config);
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
            let network = network
                .as_ref()
                .ok_or_else(|| invalid("linked Domain peer requires its actual Node DNS owner"))?;
            let dns = crate::component::native_dns::prepare_node(resources, network.clone())
                .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
            vec![(dns, resources.reusable_resource_instance(dns))]
        } else {
            Vec::new()
        };
        // The actual owning cold pass computes the finite outer data and
        // dependencies; no None default can hide a configured outer path.
        // The selection above used the existing whole-owning factory
        // grammar before any named dependency decode. A selected outer
        // carries this exact accepted material; errors cannot fall back.
        let outer = super::recipe::outer::OuterPathRecipe::prepare(
            outbound,
            resources,
            registry,
            config,
            network.as_ref(),
            outer_materials,
        )?;
        if matches!(
            &outbound.protocol,
            OutboundProtocolConfig::Wireguard {
                outer_udp_proxy: Some(_),
                ..
            }
        ) && outer.is_none()
        {
            return Err(invalid(
                "selected WireGuard Source lost its declared outer path",
            ));
        }
        let mut dependencies = dependencies;
        if let Some(outer) = &outer {
            for (id, instance) in outer.dependencies(resources) {
                if !dependencies.iter().any(|(actual, _)| actual == &id) {
                    dependencies.push((id, instance));
                }
            }
        }
        let protocol = protocol_identity(&outbound.protocol)
            .ok_or_else(|| invalid("cannot identify linked WireGuard outbound"))?;
        let identity = options.identity(match &outer {
            Some(outer) => outer.device_identity(config, protocol)?,
            None => protocol,
        });
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
                outer,
                dependencies,
            },
            resources,
        )?;

        Ok(recipe)
    }
}
