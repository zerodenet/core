use crate::component::{
    catalog::ComponentPlan,
    instance::{retained, SharedError},
};
use std::sync::Arc;
use zero_config::RuntimeConfig;
impl super::ProtocolInventory {
    /// Inline across the whole runtime and passive nested operations. This
    /// elects a join consumer; it never spawns another owner/task registry.
    pub(crate) async fn drive_component_supervision<T>(
        &self,
        work: impl std::future::Future<Output = T>,
    ) -> T {
        self.registry.components.drive_supervised(work).await
    }
    pub(crate) async fn component_failure(&self) -> zero_engine::EngineError {
        zero_engine::EngineError::Io(std::io::Error::other(
            self.registry.components.next_failure().await,
        ))
    }
    pub(crate) async fn scan_components(
        &self,
        config: &Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), zero_engine::EngineError> {
        self.registry
            .components
            .scan_candidate(config, deadline)
            .await
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))
    }
    pub(crate) async fn maintain_service_components<T, F, Fut>(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
        work: F,
    ) -> Result<T, zero_platform_tokio::contract::ChannelError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        self.registry
            .components
            .maintain_service(config, deadline, work)
            .await
    }
    /// Read only: historical snapshots cannot prepare or publish a replacement.
    pub(crate) fn current_components(
        &self,
        config: &Arc<RuntimeConfig>,
    ) -> Option<Arc<ComponentPlan>> {
        self.registry.components.current(config)
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn allocate_peer_association(
        &self,
    ) -> Result<zero_core::component::AssociationId, zero_platform_tokio::contract::ChannelError>
    {
        self.registry.components.allocate_association()
    }
    pub(crate) fn allocate_resource_identity(
        &self,
    ) -> Result<zero_core::component::ComponentInstanceId, zero_engine::EngineError> {
        let component = self
            .registry
            .components
            .allocate_id()
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))?;
        Ok(zero_core::component::ComponentInstanceId {
            component,
            generation: zero_core::component::ResourceGeneration(1),
        })
    }

    pub(crate) fn ingress_identity(
        &self,
        tag: String,
    ) -> Result<crate::component::request::IngressIdentity, SharedError> {
        use zero_core::component::{InboundId, ResourceGeneration, RoleId, RoleInstanceId};
        let component = self.registry.components.allocate_id().map_err(Arc::new)?;
        Ok(crate::component::request::IngressIdentity {
            tag,
            role: RoleInstanceId {
                component,
                role: RoleId::Inbound(InboundId(component.0)),
                generation: ResourceGeneration(1),
            },
        })
    }
    pub(crate) fn stop_component_admission(&self) {
        self.registry.components.stop_business();
    }
    pub(crate) async fn start_components(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), zero_engine::EngineError> {
        self.registry
            .components
            .start(config, deadline)
            .await
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))
    }
    pub(crate) fn commit_components<T>(
        &self,
        config: &Arc<RuntimeConfig>,
        commit: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        self.registry.components.commit(config, commit)
    }
    pub(crate) async fn shutdown_components(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), zero_engine::EngineError> {
        self.registry
            .components
            .shutdown(deadline)
            .await
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error)))
    }
    pub(crate) fn prepare_components(
        &self,
        config: Arc<RuntimeConfig>,
    ) -> Result<Arc<ComponentPlan>, SharedError> {
        self.registry.components.prepare(config)
    }
    pub(crate) fn validate_components(
        &self,
        config: Arc<RuntimeConfig>,
    ) -> Result<(), zero_engine::EngineError> {
        self.prepare_components(config)
            .map(|_| ())
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(retained(error))))
    }
}
