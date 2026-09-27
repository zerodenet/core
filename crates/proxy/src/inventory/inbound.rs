use zero_engine::EngineError;

use super::ProtocolInventory;
use crate::protocol_registry::{BoundInbound, InboundListenerCapability};
use crate::runtime::inbound_operation::PreparedInboundListenerOperation;

impl ProtocolInventory {
    pub(crate) fn inbound_listener_requires_restart(
        &self,
        inbound: &zero_config::InboundConfig,
    ) -> Result<bool, EngineError> {
        let adapter = self.registry.find_inbound(&inbound.protocol)?;
        Ok(InboundListenerCapability::inbound_listener_requires_restart(adapter.as_ref(), inbound))
    }

    pub(crate) fn update_inbound_listener(
        &self,
        inbound: zero_config::InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<bool, EngineError> {
        let adapter = self.registry.find_inbound(&inbound.protocol)?;
        InboundListenerCapability::update_inbound_listener(adapter.as_ref(), inbound, source_dir)
    }

    pub(crate) async fn bind_inbound(
        &self,
        inbound: &zero_config::InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<BoundInbound, EngineError> {
        self.registry.bind_inbound(inbound, source_dir).await
    }

    /// Resolve the registered adapter and prepare its listener operation.
    pub(crate) fn prepare_inbound_listener(
        &self,
        inbound: zero_config::InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedInboundListenerOperation>, EngineError> {
        let adapter = self.registry.find_inbound(&inbound.protocol)?;
        InboundListenerCapability::prepare_inbound_listener(adapter.as_ref(), inbound, source_dir)
    }

    pub(crate) fn prepare_rollback_inbound_listener(
        &self,
        inbound: zero_config::InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedInboundListenerOperation>, EngineError> {
        let adapter = self.registry.find_inbound(&inbound.protocol)?;
        InboundListenerCapability::prepare_rollback_inbound_listener(
            adapter.as_ref(),
            inbound,
            source_dir,
        )
    }
}
