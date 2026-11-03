use zero_engine::EngineError;

use super::ProtocolInventory;

impl ProtocolInventory {
    pub(crate) fn prepare_inbound_bind(
        &self,
        inbound: zero_config::InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<Option<crate::protocol_registry::PreparedInboundBind>, EngineError> {
        if let Some(prepared) = self.devices.prepare_inbound(inbound.clone(), source_dir)? {
            return Ok(Some(prepared));
        }
        if self
            .registry
            .inbounds
            .has_factory_for(inbound.protocol.protocol_name(), &inbound)
        {
            let mut unavailable = || {
                Err(zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::InvalidConfig,
                    zero_platform_tokio::contract::Impact::Component,
                ))
            };
            let mut preparation = crate::component::ResourcePreparation::new(&mut unavailable)
                .with_source_dir(source_dir);
            let recipe = self
                .registry
                .inbounds
                .prepare_with_resources(
                    inbound.protocol.protocol_name(),
                    &inbound,
                    &mut preparation,
                )
                .map_err(|error| EngineError::Io(std::io::Error::other(error)))?;
            return crate::protocol_registry::PreparedInboundBind::from_recipe(
                recipe.component_recipe(),
            )
            .map(Some);
        }
        Err(EngineError::CompiledFeatureDisabled {
            kind: "inbound",
            tag: inbound.tag.clone(),
            protocol: inbound.protocol.protocol_name(),
            feature: inbound.protocol.protocol_name(),
        })
    }

    pub(crate) fn inbound_listener_requires_restart(
        &self,
        inbound: &zero_config::InboundConfig,
    ) -> Result<bool, EngineError> {
        Ok(self.devices.inbound_requires_restart(inbound))
    }
    pub(crate) fn update_inbound_listener(
        &self,
        inbound: zero_config::InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<bool, EngineError> {
        self.devices.update_inbound(inbound, source_dir)
    }
}
