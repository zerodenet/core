//! Pure byte layout from the same cold retained graph, before listener bind.
use super::*;

impl PeerResources {
    pub(in crate::adapters::wireguard) fn source_supplemental_memory(
        &self,
    ) -> Result<zero_platform_tokio::contract::component::PacketSupplementalMemory, EngineError>
    {
        let graph = self
            .graph
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade();
        let Some(graph) = graph else {
            return if self.owned {
                Err(super::super::invalid(
                    "actual Source memory graph owner was released",
                ))
            } else {
                Ok(Default::default())
            };
        };
        graph.source_supplemental_memory()
    }
}
impl PendingLinkedEndpoint {
    pub(in crate::adapters::wireguard) fn source_supplemental_memory(
        &self,
    ) -> Result<zero_platform_tokio::contract::component::PacketSupplementalMemory, EngineError>
    {
        self.resources.source_supplemental_memory()
    }
}

impl PeerResources {
    pub(in crate::adapters::wireguard) fn source_outer_tasks(&self) -> Result<usize, EngineError> {
        let graph = self
            .graph
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade();
        match graph {
            Some(graph) => graph.source_outer_tasks(),
            None if !self.owned => Ok(0),
            None => Err(super::super::invalid(
                "actual Source task graph owner was released",
            )),
        }
    }
}
impl PendingLinkedEndpoint {
    pub(in crate::adapters::wireguard) fn source_outer_tasks(&self) -> Result<usize, EngineError> {
        self.resources.source_outer_tasks()
    }
}

impl PeerResources {
    pub(in crate::adapters::wireguard) fn source_outer_network(
        &self,
    ) -> Result<Option<std::sync::Weak<crate::component::background::BackgroundNetwork>>, EngineError>
    {
        match self
            .graph
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade()
        {
            Some(graph) => Ok(graph.source_outer_network()),
            None if !self.owned => Ok(None),
            None => Err(super::super::invalid(
                "actual Source environment graph owner was released",
            )),
        }
    }
}
impl PendingLinkedEndpoint {
    pub(in crate::adapters::wireguard) fn source_outer_network(
        &self,
    ) -> Result<Option<std::sync::Weak<crate::component::background::BackgroundNetwork>>, EngineError>
    {
        self.resources.source_outer_network()
    }
}
